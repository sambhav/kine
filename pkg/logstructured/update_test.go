package logstructured

import (
	"context"
	"errors"
	"testing"

	"github.com/k3s-io/kine/pkg/server"
)

type updateLog struct {
	Log
	supported      bool
	atomicKV       *server.KeyValue
	atomicErr      error
	readErr        error
	lists, appends int
}

func (l *updateLog) TryUpdate(context.Context, string, []byte, int64, int64) (*server.KeyValue, bool, error) {
	return l.atomicKV, l.supported, l.atomicErr
}
func (l *updateLog) List(context.Context, string, string, int64, int64, bool, bool) (server.EventBatch, error) {
	l.lists++
	return server.EventBatch{CurrentRev: 9, Events: []*server.Event{{KV: &server.KeyValue{Key: "key", Value: []byte("old"), CreateRevision: 2, ModRevision: 7}}}}, l.readErr
}
func (l *updateLog) Append(context.Context, *server.Event) (int64, error) {
	l.appends++
	return 10, nil
}
func (l *updateLog) CurrentRevision(context.Context) (int64, error) { return 9, nil }

func TestUpdateFastPathAndFallback(t *testing.T) {
	readErr := errors.New("read failed")
	for _, tt := range []struct {
		name           string
		log            updateLog
		updated        bool
		lists, appends int
		err            error
	}{
		{name: "unsupported uses original path", updated: true, lists: 1, appends: 1},
		{name: "atomic success skips read and append", log: updateLog{supported: true, atomicKV: &server.KeyValue{Key: "key", Value: []byte("new"), CreateRevision: 2, ModRevision: 10}}, updated: true},
		{name: "failed comparison reads current value", log: updateLog{supported: true}, lists: 1},
		{name: "insert conflict reads current value", log: updateLog{supported: true, atomicErr: server.ErrKeyExists}, lists: 1},
		{name: "fallback read error propagates", log: updateLog{supported: true, readErr: readErr}, lists: 1, err: readErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := tt.log
			rev, kv, updated, err := New(&l).Update(context.Background(), "key", []byte("new"), 7, 0)
			if !errors.Is(err, tt.err) || updated != tt.updated || l.lists != tt.lists || l.appends != tt.appends {
				t.Fatalf("rev=%d kv=%v updated=%v err=%v lists=%d appends=%d", rev, kv, updated, err, l.lists, l.appends)
			}
			if err == nil {
				if kv == nil || kv.CreateRevision != 2 {
					t.Fatalf("lost create revision: %+v", kv)
				}
				if updated && (rev != 10 || string(kv.Value) != "new") {
					t.Fatalf("invalid successful update: rev=%d kv=%+v", rev, kv)
				}
			}
		})
	}
}
