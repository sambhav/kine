package generic

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/k3s-io/kine/pkg/server"
)

func (d *Generic) TryUpdate(ctx context.Context, key string, value []byte, revision, lease int64) (*server.KeyValue, bool, error) {
	if d.UpdateSQL == nil {
		return nil, false, nil
	}
	kv := &server.KeyValue{Key: key, Value: value, Lease: lease}
	var err error
	for i := 0; i < 20; i++ {
		err = d.queryRow(ctx, d.UpdateSQL, key, revision, lease, value).Scan(&kv.ModRevision, &kv.CreateRevision)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, true, nil
		}
		if err == nil {
			return kv, true, nil
		}
		// Preserve Insert's retry for primary-key collisions with gap-fill rows.
		if d.InsertRetry == nil || !d.InsertRetry(err) {
			break
		}
		metrics.InsertErrorsTotal.WithLabelValues("true").Inc()
		select {
		case <-ctx.Done():
			return nil, true, ctx.Err()
		case <-time.After(time.Duration(i+1) * 100 * time.Millisecond):
		}
	}
	metrics.InsertErrorsTotal.WithLabelValues("false").Inc()
	if d.TranslateErr != nil {
		err = d.TranslateErr(err)
	}
	return nil, true, err
}
