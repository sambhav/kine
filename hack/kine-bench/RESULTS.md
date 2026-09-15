# Actual Kine PostgreSQL experiment results — 2026-09-15

All planned experiments have been run through real Kine processes and etcd gRPC: atomic updates, SQL pool limits, index removal, count rewrites, controlled vacuum visibility, live read/write churn, and compaction. Failed screening results are retained alongside successful runs.

## Decisions

1. **Prioritize a bounded SQL pool and the atomic update path.** At 32 concurrent RPCs on these 4-vCPU runners, pool 16 plus atomic updates repeatedly delivered about 2.5–2.6× CAS throughput, 1.8–1.9× Put throughput, and 2.0–2.2× mixed throughput versus the original update path with an unlimited pool. Pool 16 was better than pool 8 in both larger comparisons. This is a measured tuning point, not a universal connection limit.
2. **Keep all seven indexes as the general default.** Removing three reduced index bytes by 33.4% and WAL per measured write by roughly 17–19%. Its throughput benefit was inconsistent, and some count/list measurements regressed. Compaction was approximately unchanged.
3. **Keep the count rewrite opt-in.** With seven indexes, fresh-data current counts were 35% slower, historical counts 40% slower, and paginated lists 14% slower. It helped on vacuumed data and in the tested live-churn workload, which is insufficient for a global default.
4. **Maintain vacuum coverage, but do not adopt the tested eager thresholds globally.** Explicit vacuum greatly improved controlled read snapshots. During live churn, lowering thresholds produced only modest median read-tail reductions while increasing autovacuums from 5 to 23 per trial; trial ranges overlap.
5. **Retain the independent Put correctness fix.** All configurations include current-state reads and retries after failed unconditional updates. The comparisons isolate performance changes from that bug fix.

## Scope and validation

- PostgreSQL 16.15, Ubuntu 24.04, 4 vCPUs, GOMAXPROCS=4; fsync, synchronous commit, and full-page writes remained on.
- 512-byte payloads. One Kine process per fresh database clone. The write tests have one active prefix watch and 32 concurrent RPC workers sharing an etcd connection.
- Full write fixtures: 1,024 keys × 10 revisions; 5,000 operations per trial, three trials per configuration/workload. Three concurrent GET waves warm connections outside timing. CHECKPOINT is outside timing; checkpoint counters and WAL are recorded.
- Pool limits 8, 16, 32, and unlimited were tested at fixed client concurrency. Default max-idle remains 20; database/sql clamps idle connections when the open limit is lower.
- Every measured write is checked through watch events, previous values, create revisions, and final API values/revisions. Configuration checks cover CAS conflicts, stale/missing revisions, delete/recreate, leases, historical reads, compaction, sequence collisions, and concurrent Put.
- These runs do not measure multiple Kine instances, production network/TLS latency, crash recovery, or Kubernetes apiserver overhead. Shared-runner timing variation is visible in the raw trials. All reported comparisons are within the same job.

The two larger write comparisons contain **675,000 measured API operations**. The larger churn suite adds **1,014,000 verified writes** and **14,584 concurrent list/count requests**. Across those runs, **1,576,320 write events were verified**. These totals exclude warmup, correctness checks, controlled read samples, and the earlier atomic-only benchmark.

## Confirmed pool and atomic gains

[Focused confirmation](https://github.com/sambhav/kine/actions/runs/34928141536) — AMD EPYC 7763 64-Core Processor; job 118s, harness 79.9s. Values below are medians of three trials.

| Variant | CAS ops/s | CAS p95 ms | Put ops/s | Put p95 ms | Mixed ops/s | Mixed p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 3,161 | 19.81 | 2,498 | 24.21 | 4,435 | 15.79 |
| baseline-pool16 | 5,484 | 9.66 | 3,531 | 14.03 | 7,069 | 8.19 |
| atomic | 3,880 | 18.60 | 2,720 | 23.79 | 4,834 | 14.36 |
| pool8 | 7,306 | 8.95 | 3,898 | 14.56 | 8,496 | 7.18 |
| pool16 | 8,007 | 6.71 | 4,561 | 11.34 | 8,721 | 6.52 |
| pool16-four-indexes | 8,368 | 6.38 | 4,553 | 11.30 | 9,154 | 6.12 |

`baseline` uses the original read/append update and unlimited SQL connections. `atomic` changes only the update SQL. `pool8` and `pool16` include atomic updates and retain seven indexes. `baseline-pool16` changes only the connection limit. `pool16-four-indexes` combines all three changes. Mixed is 50% GET / 50% CAS.

| Change versus baseline | CAS throughput | Put throughput | Mixed throughput |
| --- | ---: | ---: | ---: |
| baseline-pool16 | +73.5% | +41.4% | +59.4% |
| atomic | +22.7% | +8.9% | +9.0% |
| pool16 | +153.3% | +82.6% | +96.7% |
| pool16-four-indexes | +164.7% | +82.3% | +106.4% |

With atomic updates and pool 16, p95 request latency fell **66% for CAS, 53% for Put, and 59% for mixed** versus baseline in this confirmation. Watch p95 fell from 55.8→13.0ms, 53.6→16.8ms, and 74.2→12.6ms, respectively. Pool 16 alone improved throughput 74%, 41%, and 59%; atomic updates added another 46%, 29%, and 23% relative to that tuned baseline.

Use `--datastore-max-open-connections=16` as the measured starting point on a comparable small deployment, then validate its workload. The experiment does not change the production pool default. Atomic updates remain behind `KINE_BENCH_ATOMIC_UPDATE=1` on this branch.

## Full initial pool/index comparison

[Full matrix](https://github.com/sambhav/kine/actions/runs/34927803554) — write job: AMD EPYC 9V74 80-Core Processor, 191s. The focused confirmation above was run on another CPU model.

| Variant | CAS ops/s | CAS p95 ms | Put ops/s | Put p95 ms | Mixed ops/s | Mixed p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 3,356 | 20.87 | 2,677 | 24.18 | 4,485 | 16.74 |
| atomic | 3,956 | 22.08 | 3,043 | 21.92 | 4,712 | 16.33 |
| pool8 | 7,014 | 10.59 | 4,136 | 14.14 | 8,857 | 7.31 |
| pool16 | 8,700 | 6.19 | 4,956 | 10.45 | 9,646 | 5.99 |
| pool32 | 4,171 | 18.92 | 3,191 | 19.98 | 5,127 | 14.03 |
| four-indexes | 3,854 | 22.16 | 2,664 | 21.15 | 4,849 | 15.76 |
| baseline-four-indexes | 3,404 | 19.39 | 2,203 | 26.99 | 4,492 | 18.32 |
| baseline-pool8 | 5,006 | 12.30 | 3,448 | 15.89 | 7,396 | 9.27 |
| pool8-four-indexes | 7,698 | 8.85 | 4,286 | 13.34 | 9,203 | 6.97 |

A substantial slow trial occurred for CAS with pool 8 (1,738 ops/s versus 7,014 and 7,377); pool 16 ranged from 5,888 to 9,142. The medians are not confidence bounds. Put and mixed trials were steadier. This is why the winning configuration was repeated.

## Index space, WAL, and compaction

The four-index variant removes `kine_name_index`, `kine_name_id_index`, and `kine_prev_revision_index`. It retains the primary key, name/previous-revision uniqueness constraint, list index, and id/deleted index. Drops affect only disposable benchmark databases after startup.

On the 300,000-seeded-revision read fixture, index size fell **73.09→48.65 MiB**, a **33.4% reduction**.

| Workload | Pool 16 / seven indexes WAL bytes/write | Pool 16 / four indexes | Reduction |
| --- | ---: | ---: | ---: |
| cas | 2,119 | 1,750 | 17.4% |
| put | 2,119 | 1,750 | 17.4% |
| mixed | 2,242 | 1,815 | 19.0% |

These WAL deltas include background database activity and first-write page images after the pre-trial checkpoint. They are not a guaranteed steady-state WAL saving. Four indexes added only 4.5% CAS throughput, essentially no Put gain, and 5.0% mixed throughput over atomic+pool16 in the confirmation.

| Indexes | Compaction ms | Rows removed | WAL MiB |
| --- | ---: | ---: | ---: |
| seven | 1,348.1 | 260,000 | 289.28 |
| four | 1,294.5 | 260,000 | 287.24 |

Compaction used 1,000-revision batches and zero minimum retained revisions. The stored target revision, rejection of compacted historical counts, and preservation of current list data were checked. The small timing difference overlaps trial ranges; no strong compaction speedup is established. Removed rows do not imply immediate filesystem space reclamation.

## Actual list/count latency: fresh versus vacuumed

Read job: AMD EPYC 7763 64-Core Processor, 225s. Fixture: 20,000 keys × 15 revisions plus Kine system rows. Each latency is the median of three trial medians, with five measured calls after two warmups.

| Variant | State | Current count ms | Historical count ms | 500-key list ms |
| --- | --- | ---: | ---: | ---: |
| seven | fresh | 237.21 | 212.91 | 483.08 |
| seven | vacuumed | 70.20 | 69.40 | 77.63 |
| four | fresh | 255.86 | 227.79 | 484.02 |
| four | vacuumed | 71.56 | 71.52 | 81.50 |
| seven-count | fresh | 320.09 | 298.84 | 551.59 |
| seven-count | vacuumed | 45.65 | 46.08 | 53.83 |
| four-count | fresh | 257.36 | 300.00 | 553.91 |
| four-count | vacuumed | 44.80 | 45.52 | 53.98 |

`-count` enables the experimental DISTINCT ON count rewrite. Historical reads target two complete key generations before the seed head. All-key current/historical counts are checked against the original grouped revision-log SQL; bounded counts also check deletion and recreation.

Table autovacuum is disabled only for these controlled snapshots: fresh uses ANALYZE; vacuumed adds VACUUM ANALYZE. This isolates visibility effects and is **not** a live production configuration. With the existing seven-index queries, vacuum reduced current-count latency 70% and paginated-list latency 84% (483→78ms). Kine’s paginated list also executes a count; its API gain is much smaller than the earlier SQL-only list-plan gain.

## Live writes with concurrent list/count requests

Churn job: AMD EPYC 9V74 80-Core Processor, 347s. Each configuration has three windows of at least 25 seconds, with 5,000 keys × five initial revisions, eight CAS writers, and one reader alternating 500-key lists and counts.

| Variant | Validated writes/s | List p95 ms | Count p95 ms | Autovacuums/trial |
| --- | ---: | ---: | ---: | ---: |
| default-table | 3,292 | 45.40 | 30.70 | 5 |
| eager-vacuum | 3,252 | 42.60 | 28.26 | 23 |
| count-rewrite | 3,485 | 37.19 | 21.86 | 5 |
| four-indexes | 3,673 | 52.56 | 34.17 | 5 |

All variants have autovacuum **on** and use the same accelerated **1s launcher cadence**. `default-table` refers to default table thresholds, not PostgreSQL’s default global launcher cadence. `eager-vacuum` lowers vacuum/insert/analyze thresholds to 100 + 2%. Periodic Kine compaction is off in this suite.

Validated writes/s includes each batch’s warmup, initial/final reads, and watch draining. It is comparable within this suite, not to isolated RPC throughput. Write and read percentiles come from three window medians; individual raw read latencies and write batches are retained.

- Eager vacuum: about 1% lower median write rate, 6% lower list p95 and 8% lower count p95, but 23 versus five autovacuums. The small median gains and overlapping trial ranges do not justify making these aggressive thresholds a general default.
- Count rewrite: about 6% higher write rate, 18% lower list p95 and 29% lower count p95 here. Its fresh-data regressions above remain a blocker for enabling it globally.
- Four indexes: about 12% higher median write rate here, but 16% higher list p95 and 11% higher count p95. Throughput varied between trials; index removal is principally a space/WAL option with a read-latency trade-off.

## Runs, raw measurements, and screening failures

| Run | Status | Job durations | Record |
| --- | --- | --- | --- |
| [Initial screening](https://github.com/sambhav/kine/actions/runs/34927426850) | Churn passed; write/read checks failed before timing | See raw job metadata | `experiments-quick1-*.json` |
| [Revised screening](https://github.com/sambhav/kine/actions/runs/34927587000) | All passed | Writes 45s; reads 51s; churn 43s | `experiments-quick2-*.json` |
| [Full matrix](https://github.com/sambhav/kine/actions/runs/34927803554) | All passed | Writes 191s; reads 225s; churn 347s | `experiments-full-*.json` |
| [Pool-16 confirmation](https://github.com/sambhav/kine/actions/runs/34928141536) | Passed | 118s | `experiments-confirm-writes.json` |
| [Earlier atomic-only API run](https://github.com/sambhav/kine/actions/runs/34924580591) | Passed | 133s | `atomic-api-full.json` |

The first screening exposed a baseline full-keyspace **keys-only list/count inconsistency**: the list returned no keys while count returned hundreds. The all-key count oracle was changed to the original grouped SQL query. This pre-existing issue is recorded, not silently treated as a successful check or fixed by these experiments. Bounded API listing and count checks pass.

Quick runs contain one short trial and are screening evidence only. The larger run adds concurrent connection warmup and combination variants; compare configurations within each run. The earlier atomic-only run used sequential warmup and is retained as separate historical evidence. Build/binary caches are enabled; each suite compares fresh local PostgreSQL template clones. Caching these cheap synthetic database seeds did not establish a speed benefit in the earlier SQL tests.

All raw JSON records are committed in [results/](results/). They include per-trial measurements, machine/commit metadata, vacuum/visibility counters, WAL, compaction, watch checks, and raw churn read latencies. [results/index.json](results/index.json) lists SHA-256 hashes and run links. Earlier native SQL-only experiments remain in [../postgres-bench/RESULTS.md](../postgres-bench/RESULTS.md); they are not added to the API gains.

## Reproduce

Push `[kine:experiments]` for quick screening, `[kine:experiments:full]` for the full matrix, or `[kine:experiments:confirm]` for the focused pool-16 comparison. See [README.md](README.md) for local invocation and exact fixture/maintenance limits. The fork retains opt-in experimental SQL; no upstream PR, schema migration, or production default change is implied by this record.
