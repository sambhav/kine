package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type variant struct {
	Name    string `json:"name"`
	Mode    string `json:"mode"`
	Pool    int    `json:"max_open"`
	Indexes int    `json:"indexes"`
	Eager   bool   `json:"eager_vacuum"`
}

type experiment struct {
	completed                   bool
	admin                       *sql.DB
	base, seed, suite, profile  string
	keys, versions, trials, ops int
	start                       time.Time
	rows                        []map[string]any
	environment                 map[string]string
}

func (e *experiment) record(row map[string]any) error {
	e.rows = append(e.rows, row)
	b, err := json.Marshal(row)
	if err != nil {
		return err
	}
	fmt.Println("EXPERIMENT_SAMPLE", string(b))
	return e.save()
}
func (e *experiment) save() error {
	b, err := json.MarshalIndent(map[string]any{"completed": e.completed, "suite": e.suite, "profile": e.profile, "commit": os.Getenv("GITHUB_SHA"), "go": runtime.Version(), "cpus": runtime.NumCPU(), "environment": e.environment, "keys": e.keys, "versions": e.versions, "trials": e.trials, "seconds": time.Since(e.start).Seconds(), "rows": e.rows}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile("experiments-results.json", b, 0644)
}

func stats(db *sql.DB) (map[string]any, error) {
	var raw []byte
	err := db.QueryRow(`SELECT json_build_object('index_bytes',pg_indexes_size('kine'),'heap_bytes',pg_table_size('kine'),
 'rows',(SELECT count(*) FROM kine),'visible_pages',c.relallvisible,'pages',c.relpages,
 'autovacuum_count',s.autovacuum_count,'autoanalyze_count',s.autoanalyze_count,'dead_tuples',s.n_dead_tup,
 'inserted',s.n_tup_ins,'table_options',c.reloptions,'wal_lsn',pg_current_wal_insert_lsn()::text,
 'checkpoints_timed',b.checkpoints_timed,'checkpoints_requested',b.checkpoints_req)
 FROM pg_class c JOIN pg_stat_user_tables s ON c.oid=s.relid CROSS JOIN pg_stat_bgwriter b WHERE c.oid='kine'::regclass`).Scan(&raw)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = json.Unmarshal(raw, &result)
	return result, err
}

func (e *experiment) trial(v variant, phase string, trial int, run func(*process, *sql.DB) (map[string]any, error)) error {
	name := fmt.Sprintf("%s_trial", e.seed)
	if err := mustExec(e.admin, "CREATE DATABASE "+name+" TEMPLATE "+e.seed); err != nil {
		return err
	}
	defer mustExec(e.admin, "DROP DATABASE "+name+" WITH (FORCE)")
	dsn := databaseURL(e.base, name)
	label := fmt.Sprintf("%s-%s-%d-%s", e.suite, v.Name, trial, phase)
	p, err := startServer(dsn, v.Mode, label, 0, "--datastore-max-open-connections", strconv.Itoa(v.Pool), "--compact-timeout", "2m")
	if err != nil {
		return err
	}
	defer p.stop()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if v.Indexes == 4 {
		for _, index := range []string{"kine_name_index", "kine_name_id_index", "kine_prev_revision_index"} {
			if err = mustExec(db, "DROP INDEX "+index); err != nil {
				return err
			}
		}
	}
	if v.Eager {
		if err = mustExec(db, `ALTER TABLE kine SET (autovacuum_vacuum_insert_threshold=100,autovacuum_vacuum_insert_scale_factor=0.02,autovacuum_vacuum_threshold=100,autovacuum_vacuum_scale_factor=0.02,autovacuum_analyze_threshold=100,autovacuum_analyze_scale_factor=0.02)`); err != nil {
			return err
		}
	}
	if phase == "vacuumed" {
		if err = mustExec(db, "VACUUM (ANALYZE) kine"); err != nil {
			return err
		}
	}
	var indexCount int
	if err = db.QueryRow("SELECT count(*) FROM pg_index WHERE indrelid='kine'::regclass").Scan(&indexCount); err != nil {
		return err
	}
	if indexCount != v.Indexes {
		return fmt.Errorf("wanted %d indexes, got %d", v.Indexes, indexCount)
	}
	if err = mustExec(db, "CHECKPOINT"); err != nil {
		return err
	}
	before, err := stats(db)
	if err != nil {
		return err
	}
	row, err := run(p, db)
	if err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	after, err := stats(db)
	if err != nil {
		return err
	}
	var wal int64
	if err = db.QueryRow("SELECT pg_wal_lsn_diff($1::pg_lsn,$2::pg_lsn)::bigint", after["wal_lsn"], before["wal_lsn"]).Scan(&wal); err != nil {
		return err
	}
	row["variant"], row["phase"], row["trial"], row["before"], row["after"], row["wal_bytes"] = v, phase, trial, before, after, wal
	return e.record(row)
}

func experiments(admin *sql.DB, base, profile, suite string, environment map[string]string) error {
	e := &experiment{admin: admin, base: base, profile: profile, suite: suite, environment: environment, start: time.Now(), keys: 512, versions: 5, trials: 1, ops: 300, rows: []map[string]any{}}
	if profile == "full" {
		e.keys, e.versions, e.trials, e.ops = 1024, 10, 3, 5000
	}
	if suite == "reads" {
		e.keys, e.versions = 512, 10
		if profile == "full" {
			e.keys, e.versions = 20000, 15
		}
	}
	if suite == "churn" {
		e.keys, e.versions = 512, 5
		if profile == "full" {
			e.keys, e.versions = 5000, 5
		}
		// A common accelerated launcher cadence makes short comparisons useful.
		// This is NOT the PostgreSQL default cadence; only table thresholds vary.
		if err := mustExec(admin, "ALTER SYSTEM SET autovacuum_naptime='1s'"); err != nil {
			return err
		}
		if err := mustExec(admin, "SELECT pg_reload_conf()"); err != nil {
			return err
		}
		e.environment["autovacuum_naptime_experiment"] = "1s for every variant"
	}
	e.seed = fmt.Sprintf("kine_exp_%d", time.Now().UnixNano())
	defer mustExec(admin, "DROP DATABASE IF EXISTS "+e.seed+" WITH (FORCE)")
	if err := fixture(admin, base, e.seed, e.keys, e.versions, suite == "reads"); err != nil {
		return err
	}
	if err := e.save(); err != nil {
		return err
	}
	switch suite {
	case "writes":
		return e.writeExperiments()
	case "reads":
		return e.readExperiments()
	case "churn":
		return e.churnExperiments()
	default:
		return fmt.Errorf("unknown suite %s", suite)
	}
}

func (e *experiment) writeExperiments() error {
	variants := []variant{{Name: "baseline", Mode: "baseline", Indexes: 7}, {Name: "atomic", Mode: "atomic", Indexes: 7},
		{Name: "pool8", Mode: "atomic", Pool: 8, Indexes: 7}, {Name: "pool16", Mode: "atomic", Pool: 16, Indexes: 7}, {Name: "pool32", Mode: "atomic", Pool: 32, Indexes: 7},
		{Name: "four-indexes", Mode: "atomic", Indexes: 4}, {Name: "baseline-four-indexes", Mode: "baseline", Indexes: 4}}
	for _, v := range variants {
		if err := e.trial(v, "checks", 0, func(p *process, db *sql.DB) (map[string]any, error) {
			if err := correctness(p.client, databaseURL(e.base, e.seed+"_trial")); err != nil {
				return nil, err
			}
			if err := countChecks(p.client, db); err != nil {
				return nil, err
			}
			return map[string]any{"kind": "correctness", "passed": true}, nil
		}); err != nil {
			return err
		}
	}
	for _, work := range []string{"cas", "put", "mixed"} {
		for trial := 0; trial < e.trials; trial++ {
			for j := range variants {
				v := variants[(j+trial)%len(variants)]
				if err := e.trial(v, work, trial, func(p *process, db *sql.DB) (map[string]any, error) {
					r, err := measure(p.client, work, 32, e.ops)
					return map[string]any{"kind": "write", "sample": r}, err
				}); err != nil {
					return err
				}
			}
		}
	}
	e.completed = true
	return e.save()
}

func countChecks(c *clientv3.Client, db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const key = "/count-check/key"
	r, err := c.Put(ctx, key, "one")
	if err != nil {
		return err
	}
	rev := r.Header.Revision
	if _, err = c.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).Then(clientv3.OpDelete(key)).Else(clientv3.OpGet(key)).Commit(); err != nil {
		return err
	}
	for _, q := range []struct{ rev, want int64 }{{0, 0}, {rev, 1}} {
		opts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithCountOnly()}
		if q.rev > 0 {
			opts = append(opts, clientv3.WithRev(q.rev))
		}
		got, err := c.Get(ctx, "/count-check/", opts...)
		if err != nil {
			return err
		}
		if got.Count != q.want {
			return fmt.Errorf("count deletion/history: %d != %d", got.Count, q.want)
		}
	}
	if _, err = c.Put(ctx, key, "two"); err != nil {
		return err
	}
	for _, revision := range []int64{0, rev} {
		opts := []clientv3.OpOption{clientv3.WithFromKey(), clientv3.WithRev(revision), clientv3.WithKeysOnly()}
		// The baseline full-keyspace list returns an empty result. Compare
		// all-key counts against the original grouped revision-log query.
		var expected int64
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM kine WHERE deleted=0 AND id IN
          (SELECT max(id) FROM kine WHERE ($1::bigint=0 OR id<=$1) GROUP BY name)`, revision).Scan(&expected); err != nil {
			return err
		}
		count, err := c.Get(ctx, "\x00", append(opts, clientv3.WithCountOnly())...)
		if err != nil {
			return err
		}
		if count.Count != expected {
			return fmt.Errorf("all-key count mismatch %d/%d", count.Count, expected)
		}
	}
	return nil
}
