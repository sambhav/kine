import fs from 'node:fs';
import os from 'node:os';
const r=JSON.parse(fs.readFileSync(process.argv[2],'utf8'));
const median=xs=>{const s=[...xs].sort((a,b)=>a-b),m=Math.floor(s.length/2);return s.length%2?s[m]:(s[m-1]+s[m])/2;};
const groups=new Map();
for(const x of r.writes){const key=`${x.indexes} indexes / ${x.mode}`;if(!groups.has(key))groups.set(key,[]);groups.get(key).push(x);}
const summary=Object.fromEntries([...groups].map(([key,rows])=>[key,Object.fromEntries(['ops_per_second','p50_ms','p95_ms','p99_ms','wal_bytes'].map(k=>[k,median(rows.map(x=>x[k]))]))]));
const baseline=summary['7 indexes / baseline'];
const lines=[`# PostgreSQL ${r.profile} benchmark`, '', `Commit: ${r.commit||r.baseline_commit}`, '',
  `Engine: ${r.environment.version}`, '',
  `Durability: fsync=${r.environment.fsync}, synchronous_commit=${r.environment.synchronous_commit}, full_page_writes=${r.environment.full_page_writes}.`, '',
  `Fixture: ${r.keys.toLocaleString()} keys × ${r.versions} revisions; ${r.setup.fixture} in ${(r.setup.total_ms/1000).toFixed(2)} s (${(r.setup.snapshot_ms/1000).toFixed(2)} s creating cache snapshot).`, '',
  `Harness: ${(r.total_ms/1000).toFixed(2)} s. ${r.trials} write trials per variant; ${r.writeOps} updates per trial. SQL-level benchmark; Kine gRPC and watch delivery are not included.`, '',
  '| Variant | Updates/s | vs baseline | p50 ms | p95 ms | p99 ms | WAL bytes/update |',
  '| --- | ---: | ---: | ---: | ---: | ---: | ---: |'];
for(const [key,x] of Object.entries(summary))lines.push(`| ${key} | ${x.ops_per_second.toFixed(0)} | ${(x.ops_per_second/baseline.ops_per_second).toFixed(2)}× | ${x.p50_ms.toFixed(3)} | ${x.p95_ms.toFixed(3)} | ${x.p99_ms.toFixed(3)} | ${(x.wal_bytes/r.writeOps).toFixed(0)} |`);
if(r.concurrent?.length){
 lines.push('', '| Clients | Indexes | Mode | Updates/s | p50 ms | p95 ms | p99 ms |', '| ---: | ---: | --- | ---: | ---: | ---: | ---: |');
 for(const c of [8,32])for(const indexes of [7,4])for(const mode of ['baseline','atomic']){
  const rows=r.concurrent.filter(x=>x.concurrency===c&&x.indexes===indexes&&x.mode===mode);
  lines.push(`| ${c} | ${indexes} | ${mode} | ${median(rows.map(x=>x.ops_per_second)).toFixed(0)} | ${median(rows.map(x=>x.p50_ms)).toFixed(3)} | ${median(rows.map(x=>x.p95_ms)).toFixed(3)} | ${median(rows.map(x=>x.p99_ms)).toFixed(3)} |`);
 }
 lines.push('', `Concurrent correctness: ${r.correctness.concurrent_cas_races} barrier-controlled CAS races, one winner each; all revision links and old values validated in ${r.correctness.concurrent_trials_valid} trials.`);
}
lines.push('', '| Indexes | State | Count | Baseline ms | Candidate ms | Speedup |','| ---: | --- | --- | ---: | ---: | ---: |');
for(const x of r.reads.filter(x=>'candidate_ms' in x))lines.push(`| ${x.indexes} | ${x.visibility} | ${x.workload} | ${x.baseline_ms.toFixed(3)} | ${x.candidate_ms.toFixed(3)} | ${x.speedup.toFixed(2)}× |`);
lines.push('',`Correctness: ${r.correctness.comparisons} SQL comparisons; ${r.correctness.write_trials_identical} matching write-trial checksums; ${r.correctness.mutation_checks} mutation checks.`, '',
  'Numbers are paired within one runner. Small quick-run differences are screening results; confirm with full runs.');
const text=lines.join('\n')+'\n';
fs.writeFileSync('benchmark-summary.md',text);
if(process.env.GITHUB_STEP_SUMMARY)fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY,text);
const machine={cpu:os.cpus()[0]?.model,cpus:os.cpus().length,memory_bytes:os.totalmem(),kernel:os.release(),node:process.version,runner_image:process.env.ImageVersion||null};
fs.writeFileSync('benchmark-machine.json',JSON.stringify(machine,null,2));
console.log(text);console.log('BENCHMARK_RESULT '+JSON.stringify({profile:r.profile,total_ms:r.total_ms,setup:r.setup,summary,concurrent:r.concurrent,correctness:r.correctness,machine}));
