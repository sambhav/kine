# Actual Kine PostgreSQL benchmarks

Measured conclusions and all run records: [RESULTS.md](RESULTS.md).

This runs the real Kine CLI/application in separate child processes and measures
etcd v3 gRPC calls over TCP loopback. The baseline uses the original read/append update path;
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

Both modes include the same ordinary Put correctness fix: read the current value
and retry failed conditional updates or concurrent deletion. Earlier runs of
the original Put handler could acknowledge values it had not stored, so their
concurrent Put throughput was excluded. Concurrent Put is now measured only
after the disjoint-key regression probe and same-key conflict checks pass.
The optimization comparison isolates the atomic SQL update; it does not compare
the fixed Put handler's speed against the buggy original handler.

## Remaining experiments

Push `[kine:experiments]` for quick screening or `[kine:experiments:full]` for
three-trial comparisons. `Kine PostgreSQL experiments` runs three independent
Actions jobs so comparisons within each suite share a runner:

- **writes:** 32 concurrent RPCs, CAS / Put / mixed, original and atomic updates,
  SQL pool limits 8/16/32/unlimited, and four/seven indexes. Full runs also
  include baseline-plus-pool-8 and atomic-plus-pool-8-plus-four-indexes. All configurations
  pass API correctness checks. Every measured write is watched and verified;
  WAL and database statistics are recorded around each trial. Three concurrent
  GET waves warm connections before timing; no writes occur during warmup.
- **reads:** count, historical count and a 500-key paginated list, with the
  experimental count rewrite off/on and four/seven indexes. Full fixtures have
  20,000 keys × 15 versions. Fresh and vacuumed snapshots share the same seed;
  table autovacuum is disabled only for this controlled visibility experiment.
  Compaction comparisons verify both the stored compact revision and API state.
- **churn:** live CAS writes and concurrent list/count requests. Table-default
  maintenance is compared with eager vacuum/analyze thresholds, the count
  rewrite, and four indexes. All variants keep autovacuum enabled and use a
  common accelerated 1s launcher cadence. Full trials last at least 25 seconds;
  this does not establish behavior at the PostgreSQL default 60s cadence.
  Throughput includes repeated batch setup and verification, so compare it only
  within this suite. Raw read latencies and per-batch watch checks are retained.

Run locally on a dedicated PostgreSQL server using `KINE_BENCH_SUITE=writes`
(or `reads` / `churn`) and `BENCH_PROFILE=quick` or `full`. The churn suite changes
`autovacuum_naptime` on that disposable server. `experiments_summary.py` generates
summary tables from `experiments-results.json`. The count rewrite is enabled only
in explicitly selected variants through `KINE_BENCH_DISTINCT_COUNT=1`.

The four-index fixture removes only `kine_name_index`, `kine_name_id_index`, and
`kine_prev_revision_index` after Kine startup. Required uniqueness, primary key,
list and id/deleted indexes remain. No production schema migration is applied.

The initial screening exposed a baseline full-keyspace list/count inconsistency
(`Get("\x00", WithFromKey(), WithKeysOnly())` returned no keys while count returned hundreds).
All-key count checks therefore use the original grouped SQL revision-log query
as their oracle. Bounded current, historical, deleted and recreated counts are
still checked through the API. The failed screening is retained in `results/`.

Use `[kine:experiments:confirm]` to repeat the focused pool-16 comparison.
It includes baseline/atomic with default pooling, baseline with pool 16, atomic
with pools 8/16, and atomic with pool 16 plus four indexes.
