import fs from 'node:fs';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { execFileSync } from 'node:child_process';
import {createFixture, extraIndexes} from './fixture.mjs';
import {concurrentSuite} from './concurrent.mjs';

const require = createRequire(process.env.BENCH_MODULE_ROOT
  ? path.join(process.env.BENCH_MODULE_ROOT, 'package.json') : import.meta.url);
const out = process.env.BENCH_OUTPUT || path.resolve('results.json');
const keys = Number(process.env.BENCH_KEYS || 20000);
const versions = Number(process.env.BENCH_VERSIONS || 15);
const writeOps = Number(process.env.BENCH_WRITE_OPS || 2000);
const repetitions = Number(process.env.BENCH_REPETITIONS || 9);
const trials = Number(process.env.BENCH_TRIALS || 5);
const benchmarkStart = performance.now();
const fixtureCache = process.env.BENCH_FIXTURE_CACHE;
const concurrentOnly=process.env.BENCH_CONCURRENT_ONLY==='1';
assert(versions >= 9, 'the deletion/recreation fixture requires at least nine versions');
for (const n of [keys, versions, writeOps, repetitions, trials]) assert(Number.isSafeInteger(n) && n > 0);
let db;
if (process.env.DATABASE_URL) {
  const {Client} = require('pg');
  db = new Client({connectionString:process.env.DATABASE_URL});
  await db.connect();
  db.close = () => db.end();
} else {
  const {PGlite} = require('@electric-sql/pglite');
  db = await PGlite.create();
}
const q = async (sql, args=[]) => (await db.query(sql,args)).rows;
const literal = x => x === null ? 'NULL' : typeof x === 'number' || typeof x === 'boolean'
  ? String(x) : x instanceof Uint8Array ? `'\\x${Buffer.from(x).toString('hex')}'::bytea`
  : `'${String(x).replaceAll("'", "''")}'`;
const prepared = new Map();
async function exec(name, sql, args=[], explain=false) {
  if (!prepared.has(name)) { await q(`PREPARE ${name} AS ${sql}`); prepared.set(name,sql); }
  assert.equal(prepared.get(name), sql);
  return q(`${explain ? 'EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) ' : ''}EXECUTE ${name}${args.length ? `(${args.map(literal).join(',')})` : ''}`);
}
const median=xs=>{const s=[...xs].sort((a,b)=>a-b),m=Math.floor(s.length/2);return s.length%2?s[m]:(s[m-1]+s[m])/2;};
const percentile = (xs,p) => [...xs].sort((a,b)=>a-b)[Math.min(xs.length-1,Math.ceil(xs.length*p)-1)];
const results = {baseline_commit:'35f4319798cf3543e49f86c775a1f8c5c23b1e80',started:new Date().toISOString(),
  engine:process.env.DATABASE_URL?'native-postgresql':'pglite-wasm-memory',keys,versions,writeOps,repetitions,trials,profile:process.env.BENCH_PROFILE||'custom',commit:process.env.GITHUB_SHA||null,reads:[],writes:[],sizes:[],correctness:{comparisons:0},plans:{}};
const save = () => fs.writeFileSync(out,JSON.stringify(results,null,2));
const report = (kind,x) => {console.log(JSON.stringify({kind,...x}));save();};
await q(`SET max_parallel_workers_per_gather=0`);
await q(`SET work_mem='64MB'`);
results.environment = (await q(`SELECT version(), current_setting('fsync') fsync,
  current_setting('synchronous_commit') synchronous_commit,
  current_setting('full_page_writes') full_page_writes,
  current_setting('shared_buffers') shared_buffers,
  current_setting('work_mem') work_mem,
  current_setting('max_parallel_workers_per_gather') parallel_workers`))[0];
report('environment',results.environment);
if(process.env.DATABASE_URL){
  for(const name of ['fsync','synchronous_commit','full_page_writes']) assert.equal(results.environment[name],'on',`${name} must remain on for native benchmarks`);
}
const setupStart=performance.now();
let fixtureMode='generated';
let snapshotMs=0;
if(fixtureCache){
 assert(process.env.DATABASE_URL, 'fixture snapshots require native PostgreSQL');
 fs.mkdirSync(fixtureCache,{recursive:true});
 const snapshot=path.join(fixtureCache,`fixture-${keys}-${versions}.dump`);
 if(fs.existsSync(snapshot)){
  execFileSync('pg_restore',['--dbname',process.env.DATABASE_URL,'--exit-on-error','--no-owner','--no-privileges',snapshot],{stdio:'inherit'});
  await q(`SET search_path=kine_bench`);
  await q(`ANALYZE kine`);
  fixtureMode='restored';
 }else{
  await createFixture(q,keys,versions);
  const snapshotStart=performance.now();
  execFileSync('pg_dump',['--dbname',process.env.DATABASE_URL,'--schema=kine_bench','--format=custom','--compress=1','--no-owner','--no-privileges','--file',snapshot],{stdio:'inherit'});
  snapshotMs=performance.now()-snapshotStart;
 }
}else await createFixture(q,keys,versions);
results.setup={fixture:fixtureMode,total_ms:performance.now()-setupStart,snapshot_ms:snapshotMs};
report('setup',results.setup);
const current = `SELECT MAX(id) AS current_rev FROM kine`;
const compact = `SELECT MAX(prev_revision) AS compact_rev FROM kine WHERE name='compact_rev_key'`;
const cols='id,name,created,deleted,create_revision,prev_revision,lease';
function countSQL(optimized, historical=false, all=false) {
  let i=1;
  let where=all ? '' : `name >= $${i++} AND name < $${i++}`;
  if(historical) where += `${where ? ' AND ' : ''}id <= $${i++}`;
  where=where ? `WHERE ${where}` : '';
  const include=`$${i}`;
  const meta=`(${current}) AS current_rev,${historical ? `(${compact}) AS compact_rev,` : ''}`;
  if(optimized) return `SELECT ${meta} COUNT(*) AS count FROM
    (SELECT DISTINCT ON(name) name,id,deleted FROM kine ${where} ORDER BY name,id DESC) latest
    WHERE deleted=0 OR ${include}`;
  return `SELECT ${meta} COUNT(id) AS count FROM kine
    INNER JOIN (SELECT MAX(id) AS id FROM kine ${where} GROUP BY name) latest USING(id)
    WHERE deleted=0 OR ${include}`;
}
const listSQL = `SELECT (${current}) AS current_rev, (${compact}) AS compact_rev,${cols},value
 FROM (SELECT dkv.id AS list_id FROM
  (SELECT DISTINCT ON(name) name,id,deleted FROM kine WHERE name >= $1 AND name < $2 AND id <= $3 ORDER BY name,id DESC) dkv
  WHERE deleted=0 ORDER BY name LIMIT 501) mkv JOIN kine ON kine.id=mkv.list_id ORDER BY kine.name`;
const getSQL = `SELECT current_rev,compact_rev,${cols},value
 FROM (${current}) current_rev,(${compact}) compact_rev,kine
 WHERE id=(SELECT MAX(id) FROM kine WHERE name=$1) AND (deleted=0 OR $2)`;
const afterSQL = `SELECT current_rev,compact_rev,${cols},value,old_value
 FROM (${current}) current_rev,(${compact}) compact_rev,kine WHERE id>$1 ORDER BY id LIMIT 500`;
const start='/registry/bench/'; const end='/registry/bench0'; const head=keys*versions+1;
// Equivalence checks include tombstones, deletion/recreation, historical revisions,
// empty and partial ranges, and both includeDeleted settings.
for(const historical of [false,true]) for(const all of [false,true]) {
  for(const include of [false,true]) for(const rev of (historical?[1,keys*7+1,keys*8+1,head]:[head])) {
    for(const range of (all?[[]]:[[start,end],[`${start}00000100`,`${start}00000199`],['missing','missing0']])) {
      const args=[...range,...(historical?[rev]:[]),include];
      const a=await exec(`c_old_${+historical}_${+all}`,countSQL(false,historical,all),args);
      const b=await exec(`c_new_${+historical}_${+all}`,countSQL(true,historical,all),args);
      assert.deepEqual(b,a); results.correctness.comparisons++;
    }
  }
}
report('correctness',results.correctness);
async function readSuite(indexes,visibility) {
  const specs=[
    ['count_current',countSQL(false),countSQL(true),[start,end,false]],
    ['count_revision',countSQL(false,true),countSQL(true,true),[start,end,keys*8+1,false]],
    ['count_small_range',countSQL(false,true),countSQL(true,true),[`${start}00000100`,`${start}00000200`,head,false]],
    ['count_all',countSQL(false,false,true),countSQL(true,false,true),[false]]
  ];
  for(const [label,baseline,candidate,args] of specs) {
    const samples=[[],[]];const elapsed=[[],[]];let plans=[];
    for(let warm=0;warm<6;warm++) for(let v=0;v<2;v++) await exec(`read_${label}_${v}`,[baseline,candidate][v],args);
    for(let n=0;n<repetitions;n++) for(const v of (n%2?[1,0]:[0,1])) {
      const t=performance.now();
      const r=await exec(`read_${label}_${v}`,[baseline,candidate][v],args,true);
      elapsed[v].push(performance.now()-t);
      const plan=r[0]['QUERY PLAN'][0]; samples[v].push(plan['Execution Time']); plans[v]=plan;
    }
    const row={indexes,visibility,workload:label,baseline_ms:median(samples[0]),candidate_ms:median(samples[1]),
      speedup:median(samples[0])/median(samples[1]),samples_ms:samples,elapsed_ms:elapsed};
    results.reads.push(row);results.plans[`${indexes}_${visibility}_${label}`]=plans;report('read',row);
  }
  for(const [name,sql,args] of [['list501',listSQL,[start,end,head]],['get',getSQL,[`${start}00000101`,false]],['watch500',afterSQL,[head-1000]]]) {
    for(let i=0;i<6;i++) await exec(name,sql,args);
    const samples=[];let plan;
    for(let i=0;i<repetitions;i++) {plan=(await exec(name,sql,args,true))[0]['QUERY PLAN'][0]; samples.push(plan['Execution Time']);}
    const row={indexes,visibility,workload:name,baseline_ms:median(samples),samples_ms:samples};
    results.reads.push(row);results.plans[`${indexes}_${visibility}_${name}`]=plan;report('read',row);
  }
}
async function size(indexes){const row={indexes,...(await q(`SELECT pg_relation_size('kine') heap_bytes,pg_indexes_size('kine') index_bytes,pg_total_relation_size('kine') total_bytes FROM kine LIMIT 1`))[0]};results.sizes.push(row);report('size',row);}
if(process.env.BENCH_PHASE==='fresh-four'){
  for(const name of ['kine_name_index','kine_name_id_index','kine_prev_revision_index']) await q(`DROP INDEX ${name}`);
  await readSuite(4,'freshly-written');await size(4);
  results.finished=new Date().toISOString();results.total_ms=performance.now()-benchmarkStart;save();
  await q(`DROP SCHEMA kine_bench CASCADE`);await db.close();process.exit(0);
}
if(!concurrentOnly){
await readSuite(7,'freshly-written');
await q(`VACUUM (ANALYZE) kine`);
await readSuite(7,'vacuumed');await size(7);
for(const name of ['kine_name_index','kine_name_id_index','kine_prev_revision_index']) await q(`DROP INDEX ${name}`);
await readSuite(4,'vacuumed');await size(4);
}
// Separate write fixture. Every measured trial starts from identical live keys.
await q(`TRUNCATE kine RESTART IDENTITY`);
const fixtureKeys=1000;
async function resetWrites(indexes){
  await q(`TRUNCATE kine RESTART IDENTITY`);
  for(const name of ['kine_name_index','kine_name_id_index','kine_prev_revision_index']) await q(`DROP INDEX IF EXISTS ${name}`);
  if(indexes===7) for(const sql of extraIndexes) await q(sql);
  await q(`INSERT INTO kine(name,created,deleted,create_revision,prev_revision,lease,value,old_value)
   VALUES ('compact_rev_key',1,0,1,0,0,''::bytea,NULL)`);
  await q(`INSERT INTO kine(name,created,deleted,create_revision,prev_revision,lease,value,old_value)
    SELECT '/registry/write/'||lpad(i::text,8,'0'),1,0,i+1,0,0,decode(repeat(md5(i::text),32),'hex'),NULL FROM generate_series(1,${fixtureKeys}) i`);
  await q(`VACUUM (ANALYZE) kine`);
}
const insertSQL=`INSERT INTO kine(name,created,deleted,create_revision,prev_revision,lease,value,old_value)
 SELECT $1,0,0,$2,$3,$4,$5,(SELECT value FROM kine WHERE id=$3) RETURNING id`;
// Prototype successful CAS fast path; no production claim for concurrent conflicts.
// The existing unique(name,prev_revision) constraint remains authoritative.
const atomicSQL=`INSERT INTO kine(name,created,deleted,create_revision,prev_revision,lease,value,old_value)
 SELECT name,0,0,CASE WHEN created=1 THEN id ELSE create_revision END,id,$3,$4,value
 FROM (SELECT * FROM kine WHERE name=$1 ORDER BY id DESC LIMIT 1) latest
 WHERE id=$2 AND deleted=0 RETURNING id`;
async function mutation(mode,key,rev,value){
 if(mode==='atomic') return Number((await exec('mut_atomic',atomicSQL,[key,rev,0,value]))[0]?.id||0);
 const previous=(await exec('mut_get',getSQL,[key,false]))[0];
 if(!previous || Number(previous.id)!==rev)return 0;
 return Number((await exec('mut_insert',insertSQL,[key,Number(previous.created)===1?Number(previous.id):Number(previous.create_revision),rev,0,value]))[0].id);
}
async function signature(){return (await q(`SELECT count(*) rows,md5(string_agg(
  row_to_json(kine)::text,E'\n' ORDER BY id)) digest FROM kine`))[0];}
let expectedSignature;
for(let trial=0;trial<(concurrentOnly?0:trials);trial++) {
 const configs=trial%2?[[4,'atomic'],[4,'baseline'],[7,'atomic'],[7,'baseline']]:[[7,'baseline'],[7,'atomic'],[4,'baseline'],[4,'atomic']];
 for(const [indexes,mode] of configs){
  await resetWrites(indexes);
  // Warm the prepared plans using failed comparisons, so the fixture stays identical.
  for(let i=0;i<10;i++)assert.equal(await mutation(mode,'/registry/write/00000001',-1,Buffer.alloc(512)),0);
  const revs=Array.from({length:fixtureKeys},(_,i)=>i+2);const latencies=[];
  const value=Buffer.from(Array.from({length:512},(_,i)=>(i*73+19)%256));
  const before=(await q(`SELECT pg_current_wal_insert_lsn() AS lsn`))[0].lsn;
  const t=performance.now();
  for(let i=0;i<writeOps;i++){
   const k=i%fixtureKeys;const key='/registry/write/'+String(k+1).padStart(8,'0');
   const opStart=performance.now();const rev=await mutation(mode,key,revs[k],value);
   latencies.push(performance.now()-opStart);assert(rev>revs[k]);revs[k]=rev;
  }
  const ms=performance.now()-t;
  const wal=Number((await q(`SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1) bytes`,[before]))[0].bytes);
  const sig=await signature();if(expectedSignature)assert.deepEqual(sig,expectedSignature);else expectedSignature=sig;
  const row={trial,indexes,mode,ops:writeOps,total_ms:ms,ops_per_second:writeOps/ms*1000,
   p50_ms:percentile(latencies,.5),p95_ms:percentile(latencies,.95),p99_ms:percentile(latencies,.99),wal_bytes:wal,signature:sig};
  results.writes.push(row);report('write',row);
 }
}
results.correctness.write_trials_identical=results.writes.length;
// Failed CAS, absent/deleted keys, previous value preservation, and uniqueness.
await resetWrites(7);
assert.equal(await mutation('atomic','missing',1,Buffer.from('x')),0);
assert.equal(await mutation('atomic','/registry/write/00000001',1,Buffer.from('x')),0);
const old=(await exec('mut_get',getSQL,['/registry/write/00000001',false]))[0];
const newRev=await mutation('atomic','/registry/write/00000001',2,Buffer.from('changed'));
const inserted=(await q(`SELECT * FROM kine WHERE id=$1`,[newRev]))[0];
assert.deepEqual(Buffer.from(inserted.old_value),Buffer.from(old.value));
assert.equal(Number(inserted.create_revision),2);
assert.equal(await mutation('atomic','/registry/write/00000001',2,Buffer.from('stale')),0);
let uniqueError=false;
try{await exec('mut_insert',insertSQL,['/registry/write/00000001',2,2,0,Buffer.from('conflict')]);}catch(e){assert.equal(e.code,'23505');uniqueError=true;}
assert(uniqueError);
await q(`INSERT INTO kine(name,created,deleted,create_revision,prev_revision,lease,value,old_value)
 SELECT name,0,1,create_revision,id,0,NULL,value FROM kine WHERE id=$1`,[newRev]);
const tombstone=Number((await q(`SELECT max(id) AS id FROM kine WHERE name='/registry/write/00000001'`))[0].id);
assert.equal(await mutation('atomic','/registry/write/00000001',tombstone,Buffer.from('deleted')),0);
results.correctness.mutation_checks=7;
if(process.env.DATABASE_URL && ['full','load'].includes(process.env.BENCH_PROFILE)) {
 await concurrentSuite({Client:require('pg').Client,q,resetWrites,getSQL,insertSQL,atomicSQL,writeOps,report,results});
}
results.finished=new Date().toISOString();results.total_ms=performance.now()-benchmarkStart;save();
console.log('All comparisons passed. Results: '+out);
await q(`DROP SCHEMA kine_bench CASCADE`);
await db.close();
