package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func readRequest(ctx context.Context, c *clientv3.Client, work string, keys int, revision int64) error {
	opts := []clientv3.OpOption{clientv3.WithPrefix()}
	if work == "list-page" {
		opts = append(opts, clientv3.WithLimit(500))
	} else {
		opts = append(opts, clientv3.WithCountOnly())
	}
	if work == "count-history" {
		opts = append(opts, clientv3.WithRev(revision))
	}
	r, err := c.Get(ctx, "/registry/bench/", opts...)
	if err != nil {
		return err
	}
	if r.Count != int64(keys) {
		return fmt.Errorf("%s count %d != %d", work, r.Count, keys)
	}
	if work == "list-page" {
		want := min(500, keys)
		if len(r.Kvs) != want || r.More != (keys > 500) {
			return fmt.Errorf("list pagination mismatch")
		}
		for i, kv := range r.Kvs {
			if string(kv.Key) != fmt.Sprintf("/registry/bench/%06d", i+1) || len(kv.Value) != 512 {
				return fmt.Errorf("list key/value mismatch")
			}
		}
	}
	return nil
}

func (e *experiment) readExperiments() error {
	variants := []variant{{Name: "seven", Mode: "atomic", Indexes: 7}, {Name: "four", Mode: "atomic", Indexes: 4},
		{Name: "seven-count", Mode: "atomic-count", Indexes: 7}, {Name: "four-count", Mode: "atomic-count", Indexes: 4}}
	samples := 3
	if e.profile == "full" {
		samples = 5
	}
	for trial := 0; trial < e.trials; trial++ {
		for j := range variants {
			v := variants[(j+trial)%len(variants)]
			phases := []string{"fresh", "vacuumed"}
			if trial%2 == 1 {
				phases = []string{"vacuumed", "fresh"}
			}
			for _, phase := range phases {
				if err := e.trial(v, phase, trial, func(p *process, db *sql.DB) (map[string]any, error) {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
					defer cancel()
					if err := countChecks(p.client); err != nil {
						return nil, err
					}
					var head int64
					if err := db.QueryRow("SELECT max(id) FROM kine WHERE name >= '/registry/bench/' AND name < '/registry/bench0'").Scan(&head); err != nil {
						return nil, err
					}
					historical := head - int64(2*e.keys)
					reads := map[string]any{}
					for _, work := range []string{"list-page", "count", "count-history"} {
						durations := []float64{}
						for i := 0; i < samples+2; i++ {
							start := time.Now()
							if err := readRequest(ctx, p.client, work, e.keys, historical); err != nil {
								return nil, err
							}
							if i >= 2 {
								durations = append(durations, float64(time.Since(start))/1e6)
							}
						}
						reads[work] = map[string]any{"samples_ms": durations, "p50_ms": quantile(append([]float64(nil), durations...), .5), "p95_ms": quantile(append([]float64(nil), durations...), .95)}
					}
					row := map[string]any{"kind": "read", "reads": reads, "count_checks": true}
					if phase == "vacuumed" && !strings.Contains(v.Mode, "count") {
						before, err := stats(db)
						if err != nil {
							return nil, err
						}
						target := head - int64(e.keys)
						start := time.Now()
						if _, err = p.client.Compact(ctx, target); err != nil {
							return nil, err
						}
						elapsed := time.Since(start)
						var compact int64
						if err = db.QueryRow("SELECT max(prev_revision) FROM kine WHERE name='compact_rev_key'").Scan(&compact); err != nil {
							return nil, err
						}
						if compact < target {
							return nil, fmt.Errorf("compaction returned before target: %d < %d", compact, target)
						}
						_, err = p.client.Get(ctx, "/registry/bench/", clientv3.WithPrefix(), clientv3.WithCountOnly(), clientv3.WithRev(historical))
						if !errors.Is(err, rpctypes.ErrCompacted) {
							return nil, fmt.Errorf("historical count after compact: %v", err)
						}
						if err = readRequest(ctx, p.client, "list-page", e.keys, 0); err != nil {
							return nil, err
						}
						after, err := stats(db)
						if err != nil {
							return nil, err
						}
						var wal int64
						if err = db.QueryRow("SELECT pg_wal_lsn_diff($1::pg_lsn,$2::pg_lsn)::bigint", after["wal_lsn"], before["wal_lsn"]).Scan(&wal); err != nil {
							return nil, err
						}
						row["compaction"] = map[string]any{"ms": float64(elapsed) / 1e6, "before": before, "after": after, "wal_bytes": wal, "target": target, "reached": compact}
					}
					return row, nil
				}); err != nil {
					return err
				}
			}
		}
	}
	e.completed = true
	return e.save()
}

func (e *experiment) churnExperiments() error {
	variants := []variant{{Name: "default-table", Mode: "atomic", Indexes: 7}, {Name: "eager-vacuum", Mode: "atomic", Indexes: 7, Eager: true},
		{Name: "count-rewrite", Mode: "atomic-count", Indexes: 7}, {Name: "four-indexes", Mode: "atomic", Indexes: 4}}
	window := 3 * time.Second
	batch := 300
	if e.profile == "full" {
		window = 25 * time.Second
		batch = 1000
	}
	for trial := 0; trial < e.trials; trial++ {
		for j := range variants {
			v := variants[(j+trial)%len(variants)]
			if err := e.trial(v, "live", trial, func(p *process, db *sql.DB) (map[string]any, error) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				type observed struct {
					data map[string][]float64
					err  error
				}
				done := make(chan observed, 1)
				go func() {
					data := map[string][]float64{"list-page": {}, "count": {}}
					for ctx.Err() == nil {
						for _, work := range []string{"list-page", "count"} {
							start := time.Now()
							err := readRequest(ctx, p.client, work, e.keys, 0)
							if err != nil {
								if ctx.Err() != nil {
									done <- observed{data: data}
									return
								}
								done <- observed{data: data, err: err}
								return
							}
							data[work] = append(data[work], float64(time.Since(start))/1e6)
						}
					}
					done <- observed{data: data}
				}()
				start := time.Now()
				batches := []sample{}
				writes := 0
				for time.Since(start) < window {
					r, err := measure(p.client, "cas", 8, batch)
					if err != nil {
						return nil, err
					}
					batches = append(batches, r)
					writes += r.Writes
				}
				elapsed := time.Since(start).Seconds()
				cancel()
				read := <-done
				if read.err != nil {
					return nil, read.err
				}
				reads := map[string]any{}
				for work, values := range read.data {
					if len(values) == 0 {
						return nil, fmt.Errorf("no %s observations", work)
					}
					reads[work] = map[string]any{"requests": len(values), "samples_ms": values, "p50_ms": quantile(append([]float64(nil), values...), .5), "p95_ms": quantile(append([]float64(nil), values...), .95), "p99_ms": quantile(append([]float64(nil), values...), .99)}
				}
				// Give completed backend statistics a chance to reach the collector.
				// The measured interval has ended; this is outside timing.
				if _, err := db.Exec("SELECT pg_stat_clear_snapshot()"); err != nil {
					return nil, err
				}
				return map[string]any{"kind": "churn", "seconds": elapsed, "writes": writes, "writes_per_second": float64(writes) / elapsed, "reads": reads, "batches": batches}, nil
			}); err != nil {
				return err
			}
		}
	}
	e.completed = true
	return e.save()
}
