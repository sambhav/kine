#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
command -v docker >/dev/null
docker info >/dev/null
npm ci --ignore-scripts
bench_container="kine-postgres-bench-$$"
cleanup() { docker rm -f "$bench_container" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker run --detach --rm --name "$bench_container" \
  --publish 127.0.0.1::5432 --shm-size=512m \
  --env POSTGRES_PASSWORD=kine-local-benchmark --env POSTGRES_DB=kine_bench \
  postgres:18.3 \
  -c fsync=on -c synchronous_commit=on -c full_page_writes=on \
  -c shared_buffers=256MB -c max_parallel_workers_per_gather=0 >/dev/null
for ((i=0;i<60;i++)); do
  if docker exec "$bench_container" pg_isready -U postgres -d kine_bench >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec "$bench_container" pg_isready -U postgres -d kine_bench
bench_port=$(docker port "$bench_container" 5432/tcp)
bench_port=${bench_port##*:}
export DATABASE_URL="postgresql://postgres:kine-local-benchmark@127.0.0.1:$bench_port/kine_bench"
export BENCH_OUTPUT="${BENCH_OUTPUT:-$PWD/native-results.json}"
docker inspect --format '{{.Image}}' "$bench_container" > "${BENCH_OUTPUT%.json}-image.txt"
node benchmark.mjs
