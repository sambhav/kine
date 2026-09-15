export const extraIndexes = [
  `CREATE INDEX kine_name_index ON kine(name)`,
  `CREATE INDEX kine_name_id_index ON kine(name,id)`,
  `CREATE INDEX kine_prev_revision_index ON kine(prev_revision)`];

export async function createFixture(q, keys, versions) {
  await q(`CREATE SCHEMA kine_bench`);
  await q(`SET search_path=kine_bench`);
  await q(`CREATE TABLE kine(id BIGSERIAL PRIMARY KEY,name text COLLATE "C",created integer,deleted integer,
    create_revision bigint,prev_revision bigint,lease integer,value bytea,old_value bytea) WITH (autovacuum_enabled=false)`);
  await q(`CREATE INDEX kine_id_deleted_index ON kine(id,deleted)`);
  await q(`CREATE UNIQUE INDEX kine_name_prev_revision_uindex ON kine(name,prev_revision)`);
  await q(`CREATE INDEX kine_list_query_index ON kine(name,id DESC,deleted)`);
  for (const sql of extraIndexes) await q(sql);
  await q(`INSERT INTO kine VALUES (1,'compact_rev_key',1,0,1,0,0,''::bytea,NULL)`);
  await q(`INSERT INTO kine(id,name,created,deleted,create_revision,prev_revision,lease,value,old_value)
  SELECT (v-1)*${keys}+k+1, '/registry/bench/'||lpad(k::text,8,'0'),
   CASE WHEN v=1 OR (v=9 AND k%19=0) THEN 1 ELSE 0 END,
   CASE WHEN (v=8 AND k%19=0) OR (v=${versions} AND k%17=0) THEN 1 ELSE 0 END,
   CASE WHEN v>=9 AND k%19=0 THEN 8*${keys}+k+1 ELSE k+1 END,
   CASE WHEN v=1 THEN 0 ELSE (v-2)*${keys}+k+1 END,0,
   CASE WHEN (v=8 AND k%19=0) OR (v=${versions} AND k%17=0) THEN NULL ELSE decode(repeat(md5(k||'/'||v),32),'hex') END,
   CASE WHEN v=1 OR (v=9 AND k%19=0) THEN NULL ELSE decode(repeat(md5(k||'/'||(v-1)),32),'hex') END
   FROM generate_series(1,${versions}) v CROSS JOIN generate_series(1,${keys}) k
   ORDER BY v,k`);
  await q(`SELECT setval('kine_id_seq',(SELECT max(id) FROM kine))`);
  await q(`ANALYZE kine`);
}
