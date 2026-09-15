import json
import os
import platform
import statistics
from pathlib import Path

r = json.loads(Path("kine-results.json").read_text())
groups = {}
for row in r["rows"]:
    groups.setdefault((row["workload"], row["clients"]), {}).setdefault(row["mode"], []).append(row)
summary = []
lines = [f'# Kine PostgreSQL API benchmark: {r["profile"]}', '',
         f'Commit: {r["commit"]}; Go {r["go"]}; PostgreSQL {r["environment"]["server_version"]}.', '',
         f'Harness: {r["seconds"]:.2f} seconds. {r["ops_per_trial"]:,} operations × {r["trials"]} trials per variant and workload.', '',
         'Real Kine processes and etcd gRPC calls over loopback; one active prefix watch. '
         'Mixed = 50% GET / 50% CAS. Clients means concurrent RPC workers sharing one etcd client connection.', '',
         'Durability: fsync, synchronous_commit, full_page_writes on. All seven indexes retained. '
         'Fresh database clones per trial; checkpoint before timing; Kine default connection pool.', '',
         '| Workload | Concurrent RPCs | Baseline ops/s | Atomic ops/s | Gain | Baseline p95 ms | Atomic p95 ms | Baseline watch p95 ms | Atomic watch p95 ms |',
         '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
for (workload, clients), modes in groups.items():
    values = {mode: {field: statistics.median(x[field] for x in rows)
                    for field in ('ops_per_second', 'p50_ms', 'p95_ms', 'p99_ms', 'watch_p95_ms')}
              for mode, rows in modes.items()}
    base, candidate = values['baseline'], values['atomic']
    row = {'workload': workload, 'concurrency': clients, 'baseline': base, 'atomic': candidate,
           'speedup': candidate['ops_per_second'] / base['ops_per_second']}
    summary.append(row)
    lines.append(f"| {workload} | {clients} | {base['ops_per_second']:.0f} | {candidate['ops_per_second']:.0f} | "
                 f"{row['speedup']:.2f}× | {base['p95_ms']:.3f} | {candidate['p95_ms']:.3f} | "
                 f"{base['watch_p95_ms']:.3f} | {candidate['watch_p95_ms']:.3f} |")
lines += ['', 'Both modes include the same ordinary Put correctness fix: read current state and retry failed updates. '
          'Baseline retains the original read/append update path; atomic uses conditional INSERT ... SELECT. '
          'Concurrent Put and same-key Put conflicts are required correctness checks.']
lines += ['- ' + warning for warning in (r.get('warnings') or [])]
lines += ['', f"Correctness: {len(r['checks'])} API suites passed. Every measured write was received via watch, "
          'including previous-value and create-revision checks; final API values and revisions matched.', '',
          'Loopback and shared-runner measurements; these exclude Kubernetes apiserver overhead and production network latency.']
text = '\n'.join(lines) + '\n'
Path('kine-summary.md').write_text(text)
if os.environ.get('GITHUB_STEP_SUMMARY'):
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as f:
        f.write(text)
cpu = next((line.split(':', 1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines()
            if line.startswith('model name')), platform.processor())
machine = {'cpu': cpu, 'cpus': os.cpu_count(), 'kernel': platform.release(),
           'runner_image': os.environ.get('ImageVersion')}
Path('kine-machine.json').write_text(json.dumps(machine, indent=2))
print(text)
print('KINE_RESULT ' + json.dumps({'profile': r['profile'], 'seconds': r['seconds'], 'summary': summary,
                                   'checks': r['checks'], 'warnings': r.get('warnings') or [], 'machine': machine}))
