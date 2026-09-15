package server

import (
	"context"
	"errors"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type putStep struct {
	method   string
	revision int64
	kv       *KeyValue
	updated  bool
	err      error
	compare  int64
}

type putBackend struct {
	Backend
	t     *testing.T
	steps []putStep
}

func (b *putBackend) next(method string) putStep {
	b.t.Helper()
	if len(b.steps) == 0 || b.steps[0].method != method {
		b.t.Fatalf("unexpected %s call; remaining steps: %+v", method, b.steps)
	}
	s := b.steps[0]
	b.steps = b.steps[1:]
	return s
}

func (b *putBackend) Create(_ context.Context, key string, value []byte, lease int64) (int64, error) {
	if key != "key" || string(value) != "requested" || lease != 17 {
		b.t.Fatalf("unexpected create arguments: %s %q %d", key, value, lease)
	}
	s := b.next("create")
	return s.revision, s.err
}

func (b *putBackend) Get(_ context.Context, key string, revision int64, keysOnly bool) (int64, *KeyValue, error) {
	if key != "key" || revision != 0 || keysOnly {
		b.t.Fatalf("Put must read the current value: key=%s revision=%d keysOnly=%t", key, revision, keysOnly)
	}
	s := b.next("get")
	return s.revision, s.kv, s.err
}

func (b *putBackend) Update(_ context.Context, key string, value []byte, revision, lease int64) (int64, *KeyValue, bool, error) {
	s := b.next("update")
	if key != "key" || string(value) != "requested" || revision != s.compare || lease != 17 {
		b.t.Fatalf("unexpected update arguments: %s %q %d %d", key, value, revision, lease)
	}
	return s.revision, s.kv, s.updated, s.err
}

func TestPutRetriesConflicts(t *testing.T) {
	before := &KeyValue{Key: "key", Value: []byte("before"), CreateRevision: 1, ModRevision: 10}
	raced := &KeyValue{Key: "key", Value: []byte("concurrent"), CreateRevision: 1, ModRevision: 11}
	failure := errors.New("storage failed")
	for _, tt := range []struct {
		name     string
		steps    []putStep
		previous *KeyValue
		err      error
	}{
		{name: "create", steps: []putStep{{method: "create", revision: 12}}},
		{name: "existing key reads current revision", previous: before, steps: []putStep{
			{method: "create", revision: 9, err: ErrKeyExists},
			{method: "get", kv: before},
			{method: "update", compare: 10, revision: 12, updated: true},
		}},
		{name: "failed update is retried", previous: raced, steps: []putStep{
			{method: "create", err: ErrKeyExists},
			{method: "get", kv: before},
			{method: "update", compare: 10, revision: 11, kv: raced},
			{method: "create", err: ErrKeyExists},
			{method: "get", kv: raced},
			{method: "update", compare: 11, revision: 12, updated: true},
		}},
		{name: "deleted before read is recreated", steps: []putStep{
			{method: "create", err: ErrKeyExists},
			{method: "get"},
			{method: "create", revision: 12},
		}},
		{name: "deleted before update is recreated", steps: []putStep{
			{method: "create", err: ErrKeyExists},
			{method: "get", kv: before},
			{method: "update", compare: 10, revision: 11},
			{method: "create", revision: 12},
		}},
		{name: "create error", err: failure, steps: []putStep{{method: "create", err: failure}}},
		{name: "read error", err: failure, steps: []putStep{
			{method: "create", err: ErrKeyExists}, {method: "get", err: failure},
		}},
		{name: "update error", err: failure, steps: []putStep{
			{method: "create", err: ErrKeyExists}, {method: "get", kv: before},
			{method: "update", compare: 10, err: failure},
		}},
	} {
		for _, prevKV := range []bool{false, true} {
			t.Run(tt.name+map[bool]string{false: "/without-prev", true: "/with-prev"}[prevKV], func(t *testing.T) {
				b := &putBackend{t: t, steps: append([]putStep(nil), tt.steps...)}
				s := &LimitedServer{backend: b}
				r, err := s.Put(context.Background(), &etcdserverpb.PutRequest{Key: []byte("key"), Value: []byte("requested"), Lease: 17, PrevKv: prevKV})
				if !errors.Is(err, tt.err) || len(b.steps) != 0 {
					t.Fatalf("err=%v remaining steps=%+v", err, b.steps)
				}
				if err != nil {
					return
				}
				if r.Header.Revision != 12 {
					t.Fatalf("acknowledged an unsuccessful write at revision %d", r.Header.Revision)
				}
				if !prevKV || tt.previous == nil {
					if r.PrevKv != nil {
						t.Fatalf("unexpected previous KV: %+v", r.PrevKv)
					}
				} else if r.PrevKv == nil || string(r.PrevKv.Value) != string(tt.previous.Value) || r.PrevKv.ModRevision != tt.previous.ModRevision {
					t.Fatalf("wrong previous KV: %+v", r.PrevKv)
				}
			})
		}
	}
}

func TestPutCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := &LimitedServer{backend: &putBackend{t: t}}
	if _, err := s.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("key"), Value: []byte("requested"), Lease: 17}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}
