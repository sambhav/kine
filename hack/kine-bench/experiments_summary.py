import json
import os
import platform
import statistics
from pathlib import Path

r = json.loads(Path('experiments-results.json').read_text())
r.setdefault('machine', {'cpu': next((s.split(':', 1)[1].strip() for s in Path('/proc/cpuinfo').read_text().splitlines() if s.startswith('model name')), platform.processor()), 'cpus': os.cpu_count(), 'runner_image': os.environ.get('ImageVersion'), 'kernel': platform.release()})
Path('experiments-results.json').write_text(json.dumps(r, indent=2))
print('EXPERIMENT_META ' + json.dumps({k: v for k, v in r.items() if k != 'rows'}))
rows = r['rows']
lines = [f"# Kine experiments: {r['suite']} / {r['profile']}", '', f"Commit: {r['commit']}. Harness: {r['seconds']:.1f}s. Completed: {r.get('completed', False)}.", '', f"Fixture: {r['keys']:,} keys × {r['versions']} versions. Three trials in full; one in quick. CPU: {r['machine']['cpu']}.", '']
med = statistics.median
summary = []
if r['suite'] == 'writes':
    lines += ['| Variant | Workload (32 RPCs) | Ops/s | p95 ms | Watch p95 ms | WAL bytes/write |', '| --- | --- | ---: | ---: | ---: | ---: |']
    groups = {}
    for row in rows:
        if row['kind'] == 'write':
            groups.setdefault((row['variant']['name'], row['phase']), []).append(row)
    for (variant, work), group in groups.items():
        d = {'variant': variant, 'workload': work, 'ops_per_second': med(x['sample']['ops_per_second'] for x in group), 'p95_ms': med(x['sample']['p95_ms'] for x in group), 'watch_p95_ms': med(x['sample']['watch_p95_ms'] for x in group), 'wal_per_write': med(x['wal_bytes']/x['sample']['writes'] for x in group)}
        summary.append(d)
        lines.append(f"| {variant} | {work} | {d['ops_per_second']:.0f} | {d['p95_ms']:.2f} | {d['watch_p95_ms']:.2f} | {d['wal_per_write']:.0f} |")
    lines += ['', 'All timings use 32 concurrent RPC workers. Pool limits cap SQL connections, with the same default max-idle setting. Every measured write is checked through a watch and final API state. WAL includes background database activity during the trial.']
elif r['suite'] == 'reads':
    groups = {}
    for row in rows:
        for work, data in row['reads'].items():
            groups.setdefault((row['variant']['name'], row['phase'], work), []).append(data['p50_ms'])
    lines += ['| Variant | State | API operation | Median ms |', '| --- | --- | --- | ---: |']
    for (variant, phase, work), values in groups.items():
        d = {'variant': variant, 'phase': phase, 'workload': work, 'p50_ms': med(values)}
        summary.append(d)
        lines.append(f"| {variant} | {phase} | {work} | {d['p50_ms']:.2f} |")
    lines += ['', '| Variant | Compaction ms | Removed rows | WAL bytes |', '| --- | ---: | ---: | ---: |']
    for variant in sorted({x['variant']['name'] for x in rows if 'compaction' in x}):
        group = [x['compaction'] for x in rows if x['variant']['name'] == variant and 'compaction' in x]
        d = {'variant': variant, 'workload': 'compaction', 'ms': med(x['ms'] for x in group), 'rows_removed': med(x['before']['rows']-x['after']['rows'] for x in group), 'wal_bytes': med(x['wal_bytes'] for x in group)}
        summary.append(d)
        lines.append(f"| {variant} | {d['ms']:.1f} | {d['rows_removed']:.0f} | {d['wal_bytes']:.0f} |")
    lines += ['', 'Controlled read snapshots have table autovacuum disabled; fresh is ANALYZE only, vacuumed adds VACUUM ANALYZE. Both are cloned from the same seed. List-page requests 500 keys and exercises Kine’s count for pagination. Count-history is two full key generations before the seed head. Compaction uses the normal 1,000-revision batches, retains the id/deleted index, verifies the target was reached, rejects compacted reads, and verifies current data.']
else:
    lines += ['| Variant | Validated writes/s | List p95 ms | Count p95 ms | Autovacuums/trial |', '| --- | ---: | ---: | ---: | ---: |']
    for variant in dict.fromkeys(x['variant']['name'] for x in rows):
        group = [x for x in rows if x['variant']['name'] == variant]
        d = {'variant': variant, 'writes_per_second': med(x['writes_per_second'] for x in group), 'list_p95_ms': med(x['reads']['list-page']['p95_ms'] for x in group), 'count_p95_ms': med(x['reads']['count']['p95_ms'] for x in group), 'autovacuums': med(x['after']['autovacuum_count']-x['before']['autovacuum_count'] for x in group)}
        summary.append(d)
        lines.append(f"| {variant} | {d['writes_per_second']:.0f} | {d['list_p95_ms']:.2f} | {d['count_p95_ms']:.2f} | {d['autovacuums']:.0f} |")
    lines += ['', 'Autovacuum is ON. All variants use a common 1s launcher cadence to observe maintenance in short runs; this is not the PostgreSQL default cadence. Each full trial runs for at least 25s with 8 CAS writers and one reader alternating list/count. Validated writes/s includes batch setup, watch draining and final-state reads. It is not directly comparable to the isolated write suite. Eager vacuum lowers table vacuum/analyze thresholds to 100 + 2%. Raw request latencies, batch measurements, visibility and vacuum counters are retained.']
lines += ['', 'Durability stays enabled. These are single-instance, loopback measurements on shared GitHub runners. No result establishes a production-wide default.']
text = '\n'.join(lines) + '\n'
Path('experiments-summary.md').write_text(text)
if os.environ.get('GITHUB_STEP_SUMMARY'):
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as f:
        f.write(text)
print(text)
print('EXPERIMENT_RESULT ' + json.dumps({'suite': r['suite'], 'profile': r['profile'], 'completed': r.get('completed', False), 'seconds': r['seconds'], 'machine': r['machine'], 'summary': summary}))
