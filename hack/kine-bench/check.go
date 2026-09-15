package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func correctness(c *clientv3.Client, dsn string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	const key = "/checks/value"
	create, err := cas(ctx, c, key, 0, "original")
	if err != nil {
		return err
	}
	if !create.Succeeded {
		return errors.New("create failed")
	}
	r1 := create.Header.Revision
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()
	w := c.Watch(wctx, key, clientv3.WithRev(r1+1), clientv3.WithPrevKV(), clientv3.WithCreatedNotify())
	select {
	case response := <-w:
		if !response.Created {
			return errors.New("watch registration failed")
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	update, err := cas(ctx, c, key, r1, "changed")
	if err != nil {
		return err
	}
	if !update.Succeeded {
		return errors.New("CAS failed")
	}
	select {
	case response := <-w:
		if response.Err() != nil {
			return response.Err()
		}
		if len(response.Events) != 1 {
			return fmt.Errorf("expected one watch event, got %d", len(response.Events))
		}
		e := response.Events[0]
		if e.PrevKv == nil || string(e.PrevKv.Value) != "original" || string(e.Kv.Value) != "changed" || e.Kv.CreateRevision != r1 || e.Kv.ModRevision != update.Header.Revision {
			return errors.New("watch metadata mismatch")
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	wcancel()
	old, err := c.Get(ctx, key, clientv3.WithRev(r1))
	if err != nil {
		return err
	}
	if len(old.Kvs) != 1 || string(old.Kvs[0].Value) != "original" {
		return errors.New("historical value mismatch")
	}
	stale, err := cas(ctx, c, key, r1, "stale")
	if err != nil {
		return err
	}
	if stale.Succeeded || len(stale.Responses) != 1 || len(stale.Responses[0].GetResponseRange().Kvs) != 1 || string(stale.Responses[0].GetResponseRange().Kvs[0].Value) != "changed" {
		return errors.New("stale CAS response mismatch")
	}
	missing, err := cas(ctx, c, "/checks/missing", r1, "missing")
	if err != nil {
		return err
	}
	if missing.Succeeded || len(missing.Responses[0].GetResponseRange().Kvs) != 0 {
		return errors.New("missing CAS mismatch")
	}
	if _, err = c.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", update.Header.Revision)).Then(clientv3.OpDelete(key)).Else(clientv3.OpGet(key)).Commit(); err != nil {
		return err
	}
	deleted, err := cas(ctx, c, key, update.Header.Revision, "deleted")
	if err != nil {
		return err
	}
	if deleted.Succeeded {
		return errors.New("updated a deleted key")
	}
	recreate, err := cas(ctx, c, key, 0, "recreated")
	if err != nil {
		return err
	}
	if !recreate.Succeeded {
		return errors.New("recreate failed")
	}
	recreated, err := c.Get(ctx, key)
	if err != nil {
		return err
	}
	if len(recreated.Kvs) != 1 || recreated.Kvs[0].CreateRevision != recreate.Header.Revision {
		return errors.New("recreate revision mismatch")
	}
	lease, err := c.Grant(ctx, 60)
	if err != nil {
		return err
	}
	leased, err := c.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", recreate.Header.Revision)).Then(clientv3.OpPut(key, "leased", clientv3.WithLease(lease.ID))).Else(clientv3.OpGet(key)).Commit()
	if err != nil {
		return err
	}
	if !leased.Succeeded {
		return errors.New("leased CAS failed")
	}
	value, err := c.Get(ctx, key)
	if err != nil {
		return err
	}
	if value.Kvs[0].Lease != int64(lease.ID) {
		return errors.New("lease not preserved")
	}
	start := make(chan struct{})
	outcomes := make(chan *clientv3.TxnResponse, 8)
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r, e := cas(ctx, c, key, leased.Header.Revision, "winner")
			if e != nil {
				errs <- e
				return
			}
			outcomes <- r
		}()
	}
	close(start)
	wg.Wait()
	close(outcomes)
	close(errs)
	for e := range errs {
		return e
	}
	winners := 0
	for r := range outcomes {
		if r.Succeeded {
			winners++
		}
	}
	if winners != 1 {
		return fmt.Errorf("CAS race had %d winners", winners)
	}
	// Force a serial primary-key collision. Both paths must retry as they do
	// when the poller has inserted a gap-fill record for a reserved sequence ID.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	value, err = c.Get(ctx, key)
	if err != nil {
		return err
	}
	if err = mustExec(db, "SELECT setval('kine_id_seq',(SELECT max(id)-1 FROM kine))"); err != nil {
		return err
	}
	retried, err := cas(ctx, c, key, value.Kvs[0].ModRevision, "retried")
	if err != nil {
		return err
	}
	if !retried.Succeeded {
		return errors.New("primary-key collision not retried")
	}
	// Keep advancing so compaction has both older and live revisions to handle.
	rev := retried.Header.Revision
	for i := 0; i < 16; i++ {
		r, e := cas(ctx, c, key, rev, fmt.Sprint(i))
		if e != nil {
			return e
		}
		if !r.Succeeded {
			return errors.New("pre-compaction update failed")
		}
		rev = r.Header.Revision
	}
	if _, err = c.Compact(ctx, rev-2); err != nil {
		return err
	}
	_, err = c.Get(ctx, key, clientv3.WithRev(r1))
	if !errors.Is(err, rpctypes.ErrCompacted) {
		return fmt.Errorf("expected compacted read error, got %v", err)
	}
	after, err := cas(ctx, c, key, rev, "after-compaction")
	if err != nil {
		return err
	}
	if !after.Succeeded {
		return errors.New("update after compaction failed")
	}
	return nil
}
