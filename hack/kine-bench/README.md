# Actual Kine PostgreSQL benchmarks

This runs the real Kine CLI/application in separate child processes and measures
etcd v3 gRPC calls over TCP loopback. The baseline uses the unchanged update path;
the candidate opts into conditional INSERT ... SELECT with
`KINE_BENCH_ATOMIC_UPDATE=1`. Both use the same compiled binary and all seven
indexes. The experiment is off by default.

Push a commit containing `[kine]` to `perf/postgres-actions` for quick screening,
or `[kine:full]` for larger comparisons. The Actions job caches the compiled binary
by source/dependency hashes and caches Go dependencies/build state. The first
build is slower; unchanged binaries are restored on later runs.

| Profile | Seed keys × revisions | Concurrent RPCs | Operations per trial | Trials |
| --- | ---: | --- | ---: | ---: |
| quick | 128 × 3 | 1, 8 | 300 | 3 |
| full | 1,024 × 10 | 1, 8, 32 | 5,000 | 3 |

Each configuration measures CAS transactions, ordinary Put on existing keys, and
a 50% GET / 50% CAS mix. Workers cycle through disjoint portions of the full key set and share one etcd client
connection, as concurrent callers of an apiserver storage client would. Payloads
are 512 bytes. One active prefix watch consumes every measured write and checks
old values, create revisions, and duplicates. Watch latency is measured from
the start of the client request to receipt of its event. Request throughput
includes the gRPC round trip; draining the watch is reported separately.

The fixture starts with Kine's actual schema. Initial history is generated in
SQL outside timing, vacuumed, and checkpointed. PostgreSQL clones the closed seed
database locally for every trial, so both modes start from identical state.
Each trial runs a fresh Kine process and checkpoints before measured requests.
Fsync, synchronous commit, and full-page writes must remain on. Autovacuum and
Kine's default connection pool and TTL/watch poller remain enabled; periodic
Kine compaction is off by default, as in the regular CLI.

Before timing, both modes must pass API checks for stale/missing comparisons,
delete/recreate, leases, historical reads, old-value watches, eight-way CAS
conflicts, primary-key collision retry, compaction, and writes after compaction.
Checks and fixtures use uniquely named disposable databases on the supplied
server. Use a dedicated PostgreSQL instance, with permission to create databases.

```sh
go build -o /tmp/kine-bench ./hack/kine-bench
DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable' \
  BENCH_PROFILE=quick /tmp/kine-bench
python3 hack/kine-bench/summary.py
```

This measures Kine itself, not Kubernetes apiserver/controller overhead. Network
RTT, TLS, shared database load, sustained checkpoints, and multiple Kine replicas
can change production results. This branch is an experiment, not a production
release recommendation.

Concurrent standalone Put is excluded from timed comparisons: the unchanged
baseline acknowledged an update without advancing the revision in the first
concurrent run. A separate eight-worker hot-key probe records the response
revision and whether the requested value was actually stored. CAS transactions
(the Kubernetes update form) and mixed GET/CAS remain the concurrent workloads.
