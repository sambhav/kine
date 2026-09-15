package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type results struct {
	Profile      string            `json:"profile"`
	Commit       string            `json:"commit"`
	Go           string            `json:"go"`
	CPUs         int               `json:"cpus"`
	Environment  map[string]string `json:"environment"`
	DatabasePool string            `json:"database_pool"`
	Keys         int               `json:"keys"`
	Versions     int               `json:"versions"`
	PayloadBytes int               `json:"payload_bytes"`
	OpsPerTrial  int               `json:"ops_per_trial"`
	Trials       int               `json:"trials"`
	Seconds      float64           `json:"seconds"`
	Checks       []string          `json:"checks"`
	Warnings     []string          `json:"warnings"`
	Rows         []sample          `json:"rows"`
}

type sample struct {
	Mode                 string  `json:"mode"`
	Workload             string  `json:"workload"`
	Clients              int     `json:"clients"`
	Trial                int     `json:"trial"`
	Ops                  int     `json:"ops"`
	Writes               int     `json:"writes"`
	OpsPerSecond         float64 `json:"ops_per_second"`
	Seconds              float64 `json:"seconds"`
	WatchCompleteSeconds float64 `json:"watch_complete_seconds"`
	P50                  float64 `json:"p50_ms"`
	P95                  float64 `json:"p95_ms"`
	P99                  float64 `json:"p99_ms"`
	WatchP95             float64 `json:"watch_p95_ms"`
	WatchEvents          int     `json:"watch_events"`
}

func cas(ctx context.Context, c *clientv3.Client, key string, rev int64, value string) (*clientv3.TxnResponse, error) {
	return c.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).Then(clientv3.OpPut(key, value)).Else(clientv3.OpGet(key)).Commit()
}

func quantile(values []float64, p float64) float64 {
	sort.Float64s(values)
	if len(values) == 0 {
		return 0
	}
	i := int(float64(len(values)) * p)
	if i >= len(values) {
		i = len(values) - 1
	}
	return values[i]
}

func measure(c *clientv3.Client, workload string, concurrency, ops int) (sample, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const prefix = "/registry/bench/"
	initial, err := c.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return sample{}, err
	}
	if len(initial.Kvs) < concurrency {
		return sample{}, fmt.Errorf("only %d initial keys", len(initial.Kvs))
	}
	if workload == "put-probe" {
		initial.Kvs = initial.Kvs[:concurrency]
	}
	keyCount := len(initial.Kvs)
	keys, revs, creates := make([]string, keyCount), make([]int64, keyCount), make([]int64, keyCount)
	last, expected := map[string][]byte{}, make([][]byte, keyCount)
	for i := 0; i < keyCount; i++ {
		kv := initial.Kvs[i]
		keys[i] = string(kv.Key)
		revs[i] = kv.ModRevision
		creates[i] = kv.CreateRevision
		last[keys[i]] = kv.Value
		expected[i] = kv.Value
	}
	keyIndex := func(id int) int {
		worker := id % concurrency
		owned := (keyCount - worker + concurrency - 1) / concurrency
		return worker + ((id/concurrency)%owned)*concurrency
	}
	// Warm connections without modifying the identical database fixture.
	for round := 0; round < 3; round++ {
		ready := make(chan error, concurrency)
		start := make(chan struct{})
		for i := 0; i < concurrency; i++ {
			go func(i int) {
				<-start
				_, err := c.Get(ctx, keys[i])
				ready <- err
			}(i)
		}
		close(start)
		for i := 0; i < concurrency; i++ {
			if err := <-ready; err != nil {
				return sample{}, err
			}
		}
	}
	isWrite := func(i int) bool { return workload != "mixed" || (i/concurrency)%2 == 1 }
	writes := 0
	for i := 0; i < ops; i++ {
		if isWrite(i) {
			writes++
		}
	}
	watch := c.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(initial.Header.Revision+1), clientv3.WithPrevKV(), clientv3.WithCreatedNotify())
	select {
	case ready := <-watch:
		if !ready.Created {
			return sample{}, fmt.Errorf("watch not ready: %v", ready.Err())
		}
	case <-ctx.Done():
		return sample{}, ctx.Err()
	}
	starts := make([]atomic.Int64, ops)
	latencies, watchLatencies := make([]float64, ops), make([]float64, 0, writes)
	watchDone := make(chan error, 1)
	go func() {
		seen := make([]bool, ops)
		if writes == 0 {
			watchDone <- nil
			return
		}
		for response := range watch {
			if response.Err() != nil {
				watchDone <- response.Err()
				return
			}
			for _, event := range response.Events {
				if len(event.Kv.Value) != 512 || event.IsCreate() || event.Type != clientv3.EventTypePut {
					watchDone <- fmt.Errorf("unexpected watch event")
					return
				}
				id := int(binary.LittleEndian.Uint64(event.Kv.Value[:8])) - 1
				if id < 0 || id >= ops || seen[id] || !isWrite(id) || starts[id].Load() == 0 {
					watchDone <- fmt.Errorf("unexpected/duplicate event id %d", id)
					return
				}
				key := string(event.Kv.Key)
				if event.PrevKv == nil || !bytes.Equal(event.PrevKv.Value, last[key]) || event.Kv.CreateRevision != creates[keyIndex(id)] || key != keys[keyIndex(id)] {
					watchDone <- fmt.Errorf("watch old value/create revision mismatch")
					return
				}
				last[key] = append([]byte(nil), event.Kv.Value...)
				seen[id] = true
				watchLatencies = append(watchLatencies, float64(time.Now().UnixNano()-starts[id].Load())/1e6)
				if len(watchLatencies) == writes {
					watchDone <- nil
					return
				}
			}
		}
		watchDone <- fmt.Errorf("watch closed before all writes arrived")
	}()
	var wg sync.WaitGroup
	errors := make(chan error, concurrency)
	start := time.Now()
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < ops; i += concurrency {
				k := keyIndex(i)
				value := bytes.Repeat([]byte{'b'}, 512)
				binary.LittleEndian.PutUint64(value[:8], uint64(i+1))
				t := time.Now()
				starts[i].Store(t.UnixNano())
				if !isWrite(i) {
					response, e := c.Get(ctx, keys[k])
					if e != nil {
						errors <- e
						return
					}
					if len(response.Kvs) != 1 || !bytes.Equal(response.Kvs[0].Value, expected[k]) || response.Kvs[0].ModRevision != revs[k] {
						errors <- fmt.Errorf("mixed read mismatch")
						return
					}
				} else {
					var rev int64
					if workload == "put" || workload == "put-probe" {
						response, e := c.Put(ctx, keys[k], string(value))
						if e != nil {
							errors <- e
							return
						}
						rev = response.Header.Revision
					} else {
						response, e := cas(ctx, c, keys[k], revs[k], string(value))
						if e != nil {
							errors <- e
							return
						}
						if !response.Succeeded {
							errors <- fmt.Errorf("unexpected failed CAS")
							return
						}
						rev = response.Header.Revision
					}
					if rev <= revs[k] {
						current, readErr := c.Get(ctx, keys[k])
						if readErr != nil {
							errors <- readErr
							return
						}
						if len(current.Kvs) != 1 {
							errors <- fmt.Errorf("missing key after acknowledgement")
							return
						}
						errors <- fmt.Errorf("acknowledged %s did not advance: key=%s previous=%d response=%d stored=%d requested_value_stored=%t", workload, keys[k], revs[k], rev, current.Kvs[0].ModRevision, bytes.Equal(current.Kvs[0].Value, value))
						return
					}
					revs[k] = rev
					expected[k] = value
				}
				latencies[i] = float64(time.Since(t)) / 1e6
			}
		}(worker)
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(errors)
	for e := range errors {
		if e != nil {
			return sample{}, e
		}
	}
	select {
	case e := <-watchDone:
		if e != nil {
			return sample{}, e
		}
	case <-time.After(15 * time.Second):
		return sample{}, fmt.Errorf("watch did not deliver %d writes", writes)
	}
	watchComplete := time.Since(start).Seconds()
	final, e := c.Get(ctx, prefix, clientv3.WithPrefix())
	if e != nil {
		return sample{}, e
	}
	for i, kv := range final.Kvs {
		if i >= keyCount {
			break
		}
		if string(kv.Key) != keys[i] || !bytes.Equal(kv.Value, expected[i]) || kv.ModRevision != revs[i] || kv.CreateRevision != creates[i] {
			return sample{}, fmt.Errorf("final API state mismatch")
		}
	}
	if len(final.Kvs) < keyCount {
		return sample{}, fmt.Errorf("final keys missing")
	}
	return sample{Workload: workload, Clients: concurrency, Ops: ops, Writes: writes, OpsPerSecond: float64(ops) / elapsed.Seconds(), Seconds: elapsed.Seconds(),
		WatchCompleteSeconds: watchComplete, P50: quantile(latencies, .5), P95: quantile(latencies, .95), P99: quantile(latencies, .99), WatchP95: quantile(watchLatencies, .95), WatchEvents: len(watchLatencies)}, nil
}
