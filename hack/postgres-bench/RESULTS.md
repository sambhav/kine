# Native PostgreSQL benchmark results — 2026-09-15

Baseline SQL: Kine `35f4319798cf3543e49f86c775a1f8c5c23b1e80`. Candidates are isolated SQL prototypes; production driver code is unchanged.

All runs used native PostgreSQL 16.15 on GitHub's Ubuntu 24.04 runners (4 vCPUs, approximately 16 GiB RAM). `fsync`, `synchronous_commit`, and `full_page_writes` remained on. These are SQL-level measurements, not end-to-end Kine gRPC throughput or watch-delivery latency.

## Iteration time

| Run | Job duration | Harness duration | State |
| --- | ---: | ---: | --- |
| [Initial quick](https://github.com/sambhav/kine/actions/runs/34918305170/attempts/1) | 21 s | 4.95 s | Dependencies and fixture cache misses |
| [Cached quick rerun](https://github.com/sambhav/kine/actions/runs/34918305170/attempts/2) | 23 s | 5.60 s | Both caches restored successfully |
| [Revised quick](https://github.com/sambhav/kine/actions/runs/34918790097) | 18 s | 4.75 s | Cached dependencies; regenerated fixture; three trials |
| [Large single-client comparison](https://github.com/sambhav/kine/actions/runs/34918377948) | 113 s | 96.86 s | 300,001 revision rows; 100,000 measured updates |
| [Expanded full comparison](https://github.com/sambhav/kine/actions/runs/34918564222) | 191 s | 176.43 s | Cached full fixture; adds 8/32 clients; noisy timings |
| [Focused concurrent repeat](https://github.com/sambhav/kine/actions/runs/34918971561) | 70 s | 57.64 s | 480,000 updates; checkpoints before trials |

Durations cover the job on an allocated runner. Queue and provisioning time vary. Each run publishes a summary plus JSON trials, plans, and machine details.
[Archived measurements](measurements-2026-09-15.json) retain the logged trials
and runner metadata in git; full EXPLAIN plans are in the 14-day Actions artifacts.

## Successful update fast path

The baseline performs GET followed by INSERT. The atomic prototype uses one INSERT ... SELECT that checks the expected revision and preserves the old value and create revision. Both retain the unique(name, prev_revision) constraint.

Five-trial medians from the larger single-client run (AMD EPYC 7763):

| Variant | Updates/s | p50 ms | p95 ms | p99 ms | WAL bytes/update |
| --- | ---: | ---: | ---: | ---: | ---: |
| 4 indexes / atomic | 3,480 | 0.271 | 0.341 | 0.417 | 1633 |
| 4 indexes / baseline | 2,105 | 0.449 | 0.556 | 0.679 | 1633 |
| 7 indexes / atomic | 3,296 | 0.293 | 0.366 | 0.444 | 1933 |
| 7 indexes / baseline | 2,085 | 0.451 | 0.560 | 0.663 | 1933 |

Atomic writes with seven indexes improved throughput 58%; the second larger run also improved 52%, although its absolute timings and tails varied substantially.

The focused concurrent run used 20,000 updates per trial, three trials per variant and client count, and disjoint keys per worker. CHECKPOINT ran before every trial, outside measured time; durability remained on. Checkpoint counters did not advance during any of the 24 timed trials. This isolates checkpoint overlap but is not a steady-state checkpoint-inclusive production benchmark. The earlier stall cause is not proven: runner hardware and load also changed.

| Clients | Indexes | Mode | Median updates/s | Trial range updates/s | p50 ms | p95 ms | p99 ms |
| ---: | ---: | --- | ---: | --- | ---: | ---: | ---: |
| 8 | 7 | baseline | 6,452 | 6,189–6,453 | 1.199 | 1.733 | 2.942 |
| 8 | 7 | atomic | 12,393 | 12,377–12,611 | 0.606 | 0.878 | 1.529 |
| 8 | 4 | baseline | 6,436 | 5,837–6,497 | 1.205 | 1.768 | 2.668 |
| 8 | 4 | atomic | 11,573 | 11,506–12,653 | 0.641 | 1.029 | 1.717 |
| 32 | 7 | baseline | 6,898 | 6,255–6,917 | 3.992 | 7.822 | 8.376 |
| 32 | 7 | atomic | 14,748 | 14,685–14,779 | 1.719 | 3.443 | 3.928 |
| 32 | 4 | baseline | 6,828 | 6,339–6,930 | 4.142 | 8.100 | 8.784 |
| 32 | 4 | atomic | 14,679 | 13,976–14,919 | 1.742 | 3.507 | 4.013 |

With seven indexes, the atomic prototype delivered 1.92× throughput at 8 clients and 2.14× at 32 clients. p95 latency fell 49% and 56%, respectively. More clients increased latency much faster than throughput in this runner, so concurrency should be tuned instead of maximized.

The expanded full run showed second-long stalls and inconsistent variant rankings. Its raw measurements are retained; do not treat its apparent 3–4× gains or index-removal regressions as stable estimates. The focused repeat was substantially steadier.

## Indexes and counts

Removing `kine_name_index`, `kine_name_id_index`, and `kine_prev_revision_index` reduced index storage from 86,384,640 to 57,024,512 bytes (34.0%) on the generated 300,001-row fixture. WAL per update fell about 15.5%. It did not deliver a consistent throughput gain, including in the steadier concurrent run. The existing seven indexes are the stronger initial choice while evaluating the atomic path.

The DISTINCT ON count candidate regressed on freshly written data. In the first large native run, current-count execution rose from 272.396 to 359.298 ms (+31.9%); after explicit vacuum it fell from 92.469 to 66.763 ms (−27.8%). The second full run reproduced the fresh-data regression (302.380 to 376.450 ms). Do not enable this count rewrite globally.

The existing list query also depended strongly on visibility: 342.666 ms before vacuum versus 2.796 ms after vacuum in that large fixture. Autovacuum is disabled only on the disposable benchmark table to isolate these states; this is not a production setting recommendation.

## Cache decision

Both dependency and logical-fixture caches were created and restored successfully. Quick fixture generation took 0.59–0.64 s; dump restoration took 0.64 s plus cache download, so quick runs regenerate locally.

For the large synthetic fixture, generation took 6.28 s plus 1.74 s to create the dump. Restoration took 9.38 s on a different runner, before download overhead. These are not controlled hardware comparisons, but they do not demonstrate a cache benefit. Full runs therefore also regenerate by default. Cached dumps remain optional for experimenting with more expensive datasets.

Enable the full fixture cache by including both `[bench:full]` and `[bench:cache]` in the final push commit message. Cache keys include PostgreSQL major, profile, and fixture source hash. The cached archive is an immutable logical seed; every measured write trial resets its state. Live PGDATA is never cached. Restored indexes can have a different physical layout, so compare variants within one run.

## Correctness and limits

Each full run passed 40 count equivalence cases, 20 complete write-trial checksums, and seven mutation checks. Both concurrent runs passed four lock-barrier races (eight contenders, exactly one winner; both modes and index sets). Every revision link, old value, create revision, and update count was validated in all concurrent trials.

The measured gain covers successful CAS updates. Failed-CAS response mapping, retries, compaction races, range/watch behavior, and end-to-end Kine integration remain required before adopting the atomic prototype in the driver. The harness does not change those production paths.

## Repeat

Push normally to `perf/postgres-actions` for quick screening. Include `[bench:full]` for the full read/write comparison or `[bench:load]` for the focused 8/32-client repeat. You can rerun an existing job without changing code:

```sh
gh run rerun 34918790097 --repo sambhav/kine
gh run rerun 34918971561 --repo sambhav/kine
```

`workflow_dispatch` supports the same profiles and an optional cache input once the workflow is registered on the default branch. See [README.md](README.md) for local PostgreSQL and Docker commands.

