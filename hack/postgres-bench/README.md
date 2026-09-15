# PostgreSQL optimization experiments

Baseline: Kine `35f4319798cf3543e49f86c775a1f8c5c23b1e80`.

This harness compares the baseline SQL with narrow PostgreSQL `DISTINCT ON`
counts, removing three overlapping indexes, and a one-statement successful CAS
update prototype. All candidates are isolated in this harness; this branch does
not change production driver behavior.

## Fast GitHub Actions loop

Push to `perf/postgres-actions` in the fork to run the quick profile. Add
`[bench:full]` to the push's final commit message for the larger profile.
Use `[bench:load]` for a focused repeat with 8/32 clients, 20,000 updates per
variant/trial, and an explicit checkpoint before each trial (outside timing).
Checkpoint statistics and maximum latency are recorded to expose stalls. The
workflow also supports manual dispatch with a `quick`/`full`/`load` input once GitHub
registers it on the default branch. Re-run the benchmark job to compare a warm
cache without another code change.

| Profile | Revision rows | Updates per variant | Read samples |
| --- | ---: | ---: | ---: |
| quick | 30,001 | 300 × 3 trials | 3 |
| full | 300,001 | 5,000 × 5 trials | 9 |

The workflow uses PostgreSQL already installed on Ubuntu 24.04; there is no image
pull or Go build in this SQL screening loop. It asserts that fsync, synchronous
commit, and full-page writes are on. Job summaries report throughput, latency,
WAL bytes, fixture setup time, and correctness checks. Artifacts contain every
trial, query plans, and runner details. Queue/runner provisioning time is outside
the benchmark and cannot be guaranteed to fit a one-minute budget.

Node dependencies are cached by lockfile. Quick runs regenerate the 30,001-row fixture: measured generation and restore
both took about 0.6 s, so skipping cache download is faster. For full runs, the
immutable initial fixture is a compressed `pg_dump` archive cached by PostgreSQL major, profile, and the fixture
source hash. A cache miss generates it; a hit restores it and refreshes planner
statistics. Never cache a running PGDATA directory. Measured writes always start
from reset fixtures; cache entries never contain a prior trial's mutated state.
Logical restore rebuilds indexes, so compare candidates within each run rather
than treating absolute timings from generated/restored layouts as interchangeable.
The JSON records generation/restoration and snapshot time separately. GitHub
cache scope and eviction can cause misses; every run works without the cache.

## Native PostgreSQL using Docker

Prerequisites: Docker daemon, Node.js 22+, and npm.

```sh
./hack/postgres-bench/run-docker.sh
```

This starts an isolated PostgreSQL 18.3 container, binds a random localhost port,
enables fsync, synchronous commit and full-page writes, records the image ID,
runs the comparisons, and removes the container on exit. It does not touch an
existing database. 

For an existing **dedicated benchmark database**:

```sh
cd hack/postgres-bench
npm ci --ignore-scripts
DATABASE_URL=postgresql://... npm run bench
```

The harness creates `kine_bench` schema and refuses to proceed if it exists. It
drops only that schema on successful completion. After a failed run, explicitly
remove that benchmark schema before rerunning. Never target a production server.

## Embedded SQL comparison

```sh
cd hack/postgres-bench
npm ci --ignore-scripts
npm run bench
```

Without DATABASE_URL this uses PGlite 0.5.8 (PostgreSQL 18.3 compiled to WASM) in
memory. This is useful for SQL correctness, query-plan inspection, and relative
single-session costs. It has fsync off and cannot measure native disk durability,
multi-session concurrency, network round trips, Kine gRPC performance, or watch
behavior. It must not be reported as production PostgreSQL or Kine throughput.

## Method

- 20,000 keys, 15 versions/key, 300,001 rows including the compact revision key.
- 512-byte values and prior values; delete/recreate at versions 8/9 for 1 in 19
  keys and final tombstones for 1 in 17 keys.
- Count correctness compares current/historical/all/range queries and both
  deletion inclusion modes.
- Read timings: six warmups, nine EXPLAIN ANALYZE samples, alternating baseline
  and candidate order. Record server execution times, full plans, buffers, and
  elapsed client times. PostgreSQL prepared plans use the normal auto policy.
- Compare freshly written and vacuumed data. Seven indexes versus four retains
  the primary key, name/previous-revision unique index, list index, and
  id/deleted covering index.
- Write timings: 1,000 initial live keys, 2,000 successful updates, five trials
  per variant, alternating execution order and rebuilding identical fixtures.
  Each update autocommits. Record elapsed throughput, per-operation percentiles,
  WAL insert-position deltas, and compare a checksum of every resulting row.
- Full native runs also measure 8 and 32 clients, each owning disjoint keys, with
  three trials per variant and client count. These use the pg driver's named
  prepared statements; single-client measurements use SQL PREPARE/EXECUTE.
  Compare candidates within the same client count/protocol.
- Four lock-barrier CAS races (both modes and both index sets) require exactly one
  winner among eight contenders. Each concurrent trial validates every stored
  revision link, old value, create revision, and total number of updates.
- The atomic prototype retains the uniqueness constraint and verifies failed
  revisions and absent/deleted keys. Kine's retry/error mapping, compaction races,
  watches, and revision-gap behavior need end-to-end tests before adoption.
- Autovacuum is disabled only on the disposable fixture table so background
  vacuum cannot contaminate the explicit fresh-versus-vacuumed comparison.
- Parallel query execution is disabled identically; work_mem is 64 MB. Native
  Docker shared_buffers is 256 MB. PGlite settings are recorded in the JSON.
- Timing ratios from WASM are not predictions of native speedups.

Configuration: `BENCH_KEYS`, `BENCH_VERSIONS` (use 15 for the standard fixture),
`BENCH_WRITE_OPS`, `BENCH_REPETITIONS`, `BENCH_TRIALS`, and `BENCH_OUTPUT`.
`BENCH_FIXTURE_CACHE` enables dump caching on native PostgreSQL (requires matching
`pg_dump`/`pg_restore` clients); `BENCH_PROFILE` labels the results; `full` also enables native concurrent tests.
Set `BENCH_PHASE=fresh-four` for the supplemental read-only experiment on
freshly written data with four indexes, before vacuum changes visibility.

