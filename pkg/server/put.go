package server

import (
	"bytes"
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (l *LimitedServer) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	if r.IgnoreValue {
		return nil, unsupported("ignoreValue")
	}
	if r.IgnoreLease {
		return nil, unsupported("ignoreLease")
	}

	// redirect apiserver get to the substitute compact revision key
	// response is fixed up in toKV()
	if bytes.Equal(r.Key, compactRevKey) {
		r.Key = compactRevAPI
	}

	key := string(r.Key)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var kv *KeyValue
		rev, err := l.backend.Create(ctx, key, r.Value, r.Lease)
		if err == ErrKeyExists {
			// A failed Create can return a cached revision. Read the current
			// value so an unconditional Put does not compare against old state.
			_, kv, err = l.backend.Get(ctx, key, 0, false)
			if err != nil {
				return nil, err
			}
			if kv == nil {
				// The key was deleted after Create; try creating it again.
				continue
			}
			var updated bool
			rev, _, updated, err = l.backend.Update(ctx, key, r.Value, kv.ModRevision, r.Lease)
			if err == nil && !updated {
				// Another writer won the comparison. Put must retry instead of
				// acknowledging a value that was never stored.
				continue
			}
		}
		if err != nil {
			return nil, err
		}
		if !r.PrevKv {
			kv = nil
		}

		return &etcdserverpb.PutResponse{
			Header: &etcdserverpb.ResponseHeader{Revision: rev},
			PrevKv: toKV(kv),
		}, nil
	}
}
