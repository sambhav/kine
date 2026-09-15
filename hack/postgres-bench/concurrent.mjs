import assert from 'node:assert/strict';
import {performance} from 'node:perf_hooks';

// Native-only second stage. Each worker owns disjoint keys for timing; a
// separate barrier-controlled race checks that same-revision CAS has one winner.
export async function concurrentSuite({Client, q, resetWrites, getSQL, insertSQL,
  atomicSQL, writeOps, report, results}) {
  const clients=Array.from({length:32},()=>new Client({connectionString:process.env.DATABASE_URL}));
  await Promise.all(clients.map(async c=>{
    await c.connect();
    await c.query("SET search_path=kine_bench; SET work_mem='64MB'; SET max_parallel_workers_per_gather=0");
  }));
  const run=async(c,mode,key,rev,value)=>{
    if(mode==='atomic') return Number((await c.query({name:'cas',text:atomicSQL,values:[key,rev,0,value]})).rows[0]?.id||0);
    const old=(await c.query({name:'get',text:getSQL,values:[key,false]})).rows[0];
    if(!old||Number(old.id)!==rev)return 0;
    return Number((await c.query({name:'insert',text:insertSQL,values:[key,Number(old.created)===1?Number(old.id):Number(old.create_revision),rev,0,value]})).rows[0].id);
  };
  results.concurrent=[];
  try {
    for(const indexes of [7,4]) {
      await resetWrites(indexes);
      // Queue all contenders behind one table lock before releasing them.
      // This forces overlap, including the baseline GET/INSERT race window.
      for(const mode of ['baseline','atomic']) {
        const key='/registry/write/00000001';
        const rev=Number((await q('SELECT max(id) id FROM kine WHERE name=$1',[key]))[0].id);
        await q('BEGIN');
        await q('LOCK TABLE kine IN SHARE MODE');
        let settled;
        try {
          const contenders=clients.slice(0,8).map(c=>run(c,mode,key,rev,Buffer.from('race')));
          settled=Promise.allSettled(contenders);
          const deadline=performance.now()+5000;
          while(true){
            const waiting=Number((await q(`SELECT count(*) n FROM pg_locks
              WHERE relation='kine'::regclass AND mode='RowExclusiveLock' AND NOT granted`))[0].n);
            if(waiting===8)break;
            assert(performance.now()<deadline,'CAS contenders did not reach the lock barrier');
            await new Promise(resolve=>setTimeout(resolve,5));
          }
        } finally { await q('ROLLBACK'); }
        const outcomes=await settled;
        assert.equal(outcomes.filter(x=>x.status==='fulfilled'&&x.value>0).length,1);
        for(const x of outcomes)if(x.status==='rejected')assert.equal(x.reason.code,'23505');
        const successors=Number((await q('SELECT count(*) n FROM kine WHERE name=$1 AND prev_revision=$2',[key,rev]))[0].n);
        assert.equal(successors,1);
      }
    }
    results.correctness.concurrent_cas_races=4;
    const percentile=(xs,p)=>xs[Math.min(xs.length-1,Math.ceil(xs.length*p)-1)];
    const value=Buffer.alloc(512,73);
    for(const concurrency of [8,32]) for(let trial=0;trial<3;trial++) {
      const configs=trial%2?[[4,'atomic'],[4,'baseline'],[7,'atomic'],[7,'baseline']]:[[7,'baseline'],[7,'atomic'],[4,'baseline'],[4,'atomic']];
      for(const [indexes,mode] of configs) {
        await resetWrites(indexes);
        await Promise.all(clients.slice(0,concurrency).map(async c=>{
          for(let i=0;i<6;i++)assert.equal(await run(c,mode,'missing',-1,value),0);
        }));
        const before=(await q('SELECT pg_current_wal_insert_lsn() lsn'))[0].lsn;
        const latencies=[];
        const start=performance.now();
        await Promise.all(clients.slice(0,concurrency).map(async(c,worker)=>{
          const owned=Array.from({length:Math.ceil((1000-worker)/concurrency)},(_,i)=>worker+i*concurrency);
          const revs=owned.map(k=>k+2);
          for(let i=worker,n=0;i<writeOps;i+=concurrency,n++){
            const j=n%owned.length,key='/registry/write/'+String(owned[j]+1).padStart(8,'0');
            const t=performance.now();const rev=await run(c,mode,key,revs[j],value);
            latencies.push(performance.now()-t);assert(rev>revs[j]);revs[j]=rev;
          }
        }));
        const ms=performance.now()-start;
        const wal=Number((await q('SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1) bytes',[before]))[0].bytes);
        // IDs are allocated in nondeterministic order. Validate every link and
        // old value rather than comparing checksums that depend on ID ordering.
        const check=(await q(`SELECT count(*) n, bool_and(
          p.id IS NOT NULL AND k.name=p.name AND k.old_value=p.value
          AND k.create_revision=p.create_revision AND k.value=$1
          AND k.id>p.id AND k.deleted=0) valid
          FROM kine k LEFT JOIN kine p ON p.id=k.prev_revision WHERE k.created=0`,[value]))[0];
        assert.equal(Number(check.n),writeOps);assert.equal(check.valid,true);
        latencies.sort((a,b)=>a-b);
        const row={concurrency,trial,indexes,mode,ops:writeOps,total_ms:ms,
          ops_per_second:writeOps/ms*1000,p50_ms:percentile(latencies,.5),
          p95_ms:percentile(latencies,.95),p99_ms:percentile(latencies,.99),wal_bytes:wal};
        results.concurrent.push(row);report('concurrent',row);
      }
    }
    results.correctness.concurrent_trials_valid=results.concurrent.length;
  } finally { await Promise.all(clients.map(c=>c.end())); }
}
