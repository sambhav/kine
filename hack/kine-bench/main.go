// kine-bench runs the real Kine application in a separate process and drives its
// etcd gRPC API. PostgreSQL is used directly only for isolated fixture setup.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/k3s-io/kine/pkg/app"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type process struct {
	client *clientv3.Client
	cmd    *exec.Cmd
	done   chan error
	log    *os.File
}

func startServer(dsn, mode, label string, compactRetain int) (*process, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	address := listener.Addr().String()
	listener.Close()
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	f, err := os.Create(filepath.Join("kine-bench-logs", label+".log"))
	if err != nil {
		return nil, err
	}
	p := &process{log: f, done: make(chan error, 1)}
	p.cmd = exec.Command(exe, "serve", "--endpoint", dsn, "--listen-address", "http://"+address,
		"--metrics-bind-address", "0", "--compact-min-retain", strconv.Itoa(compactRetain))
	p.cmd.Env = append(os.Environ(), "KINE_BENCH_ATOMIC_UPDATE="+map[string]string{"baseline": "0", "atomic": "1"}[mode])
	p.cmd.Stdout, p.cmd.Stderr = f, f
	if err = p.cmd.Start(); err != nil {
		f.Close()
		return nil, err
	}
	go func() { p.done <- p.cmd.Wait() }()
	p.client, err = clientv3.New(clientv3.Config{Endpoints: []string{"http://" + address}, DialTimeout: 10 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithBlock()}, Logger: zap.NewNop()})
	if err != nil {
		p.stop()
		return nil, fmt.Errorf("start %s: %w", label, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err = p.client.Get(ctx, "/registry/health"); err != nil {
		p.stop()
		return nil, err
	}
	return p, nil
}

func (p *process) stop() {
	if p.client != nil {
		p.client.Close()
	}
	if p.cmd.Process != nil {
		p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			p.cmd.Process.Kill()
			<-p.done
		}
	}
	p.log.Close()
}

func mustExec(db *sql.DB, statement string, args ...any) error {
	_, err := db.ExecContext(context.Background(), statement, args...)
	return err
}

func databaseURL(base, name string) string {
	u, err := url.Parse(base)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

func fixture(admin *sql.DB, base, name string, keys, versions int) error {
	if err := mustExec(admin, "CREATE DATABASE "+name); err != nil {
		return err
	}
	dsn := databaseURL(base, name)
	p, err := startServer(dsn, "baseline", "seed", 1000)
	if err != nil {
		return err
	}
	p.stop()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	var head int64
	if err = db.QueryRow("SELECT max(id) FROM kine").Scan(&head); err != nil {
		return err
	}
	query := fmt.Sprintf(`INSERT INTO kine(id,name,created,deleted,create_revision,prev_revision,lease,value,old_value)
	 SELECT %[1]d+(v-1)*%[2]d+k, '/registry/bench/'||lpad(k::text,6,'0'),
	 CASE WHEN v=1 THEN 1 ELSE 0 END,0,CASE WHEN v=1 THEN 0 ELSE %[1]d+k END,
	 CASE WHEN v=1 THEN 0 ELSE %[1]d+(v-2)*%[2]d+k END,0,
	 repeat('x',512)::bytea,CASE WHEN v=1 THEN NULL ELSE repeat('x',512)::bytea END
	 FROM generate_series(1,%[3]d) v CROSS JOIN generate_series(1,%[2]d) k ORDER BY v,k`, head, keys, versions)
	for _, q := range []string{query, "SELECT setval('kine_id_seq',(SELECT max(id) FROM kine))", "VACUUM (ANALYZE) kine", "CHECKPOINT"} {
		if err = mustExec(db, q); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "serve" {
		err := app.New().Run(append([]string{"kine"}, os.Args[2:]...))
		if err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	start := time.Now()
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		return errors.New("DATABASE_URL must name a dedicated local PostgreSQL benchmark server")
	}
	if err := os.MkdirAll("kine-bench-logs", 0755); err != nil {
		return err
	}
	admin, err := sql.Open("pgx", databaseURL(base, "postgres"))
	if err != nil {
		return err
	}
	defer admin.Close()
	environment := map[string]string{}
	for _, setting := range []string{"server_version", "fsync", "synchronous_commit", "full_page_writes", "shared_buffers", "autovacuum", "checkpoint_timeout", "max_wal_size"} {
		var value string
		if err = admin.QueryRow("SELECT current_setting($1)", setting).Scan(&value); err != nil {
			return err
		}
		environment[setting] = value
	}
	for _, s := range []string{"fsync", "synchronous_commit", "full_page_writes"} {
		if environment[s] != "on" {
			return fmt.Errorf("%s must be on", s)
		}
	}
	profile := os.Getenv("BENCH_PROFILE")
	if profile == "" {
		profile = "quick"
	}
	keys, versions, ops, trials, clients := 128, 3, 300, 3, []int{1, 8}
	if profile == "full" {
		keys, versions, ops, clients = 1024, 10, 5000, []int{1, 8, 32}
	}
	if value := os.Getenv("KINE_BENCH_OPS"); value != "" {
		ops, err = strconv.Atoi(value)
		if err != nil || ops < 1 {
			return errors.New("invalid KINE_BENCH_OPS")
		}
	}
	seed := fmt.Sprintf("kine_e2e_%d_seed", time.Now().UnixNano())
	defer mustExec(admin, "DROP DATABASE IF EXISTS "+seed+" WITH (FORCE)")
	if err = fixture(admin, base, seed, keys, versions); err != nil {
		return err
	}
	r := results{Profile: profile, Commit: os.Getenv("GITHUB_SHA"), Go: runtime.Version(), CPUs: runtime.NumCPU(),
		Environment: environment, Keys: keys, Versions: versions, PayloadBytes: 512, OpsPerTrial: ops, Trials: trials,
		DatabasePool: "Kine defaults: max idle 20, max open unlimited", Rows: []sample{}}
	save := func() error {
		r.Seconds = time.Since(start).Seconds()
		b, e := json.MarshalIndent(r, "", "  ")
		if e != nil {
			return e
		}
		return os.WriteFile("kine-results.json", b, 0644)
	}
	if err = save(); err != nil {
		return err
	}
	for _, mode := range []string{"baseline", "atomic"} {
		name := strings.TrimSuffix(seed, "_seed") + "_check"
		err = func() error {
			if e := mustExec(admin, "CREATE DATABASE "+name+" TEMPLATE "+seed); e != nil {
				return e
			}
			defer mustExec(admin, "DROP DATABASE "+name+" WITH (FORCE)")
			p, e := startServer(databaseURL(base, name), mode, "check-"+mode, 0)
			if e != nil {
				return e
			}
			defer p.stop()
			return correctness(p.client, databaseURL(base, name))
		}()
		if err != nil {
			return fmt.Errorf("%s correctness: %w", mode, err)
		}
		r.Checks = append(r.Checks, mode+": CAS/stale/missing/delete/recreate/lease/watch/race/compaction/primary-key retry passed")
		fmt.Println("CORRECTNESS", r.Checks[len(r.Checks)-1])
		save()
	}
	for _, workload := range []string{"cas", "put", "mixed"} {
		for _, concurrency := range clients {
			for trial := 0; trial < trials; trial++ {
				modes := []string{"baseline", "atomic"}
				if trial%2 == 1 {
					modes = []string{"atomic", "baseline"}
				}
				for _, mode := range modes {
					label := fmt.Sprintf("%s-c%d-t%d-%s", workload, concurrency, trial, mode)
					name := strings.TrimSuffix(seed, "_seed") + "_trial"
					var row sample
					err = func() error {
						if e := mustExec(admin, "CREATE DATABASE "+name+" TEMPLATE "+seed); e != nil {
							return e
						}
						defer mustExec(admin, "DROP DATABASE "+name+" WITH (FORCE)")
						p, e := startServer(databaseURL(base, name), mode, label, 1000)
						if e != nil {
							return e
						}
						defer p.stop()
						if e = mustExec(admin, "CHECKPOINT"); e != nil {
							return e
						}
						row, e = measure(p.client, workload, concurrency, ops)
						return e
					}()
					if err != nil {
						return fmt.Errorf("%s: %w", label, err)
					}
					row.Mode, row.Trial = mode, trial
					r.Rows = append(r.Rows, row)
					b, _ := json.Marshal(row)
					fmt.Println("KINE_SAMPLE", string(b))
					if err = save(); err != nil {
						return err
					}
				}
			}
		}
	}
	return save()
}
