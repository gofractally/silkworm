#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
stop_block="${1:-5790000}"
tables="${SILKWORM_KV_TRACE_TABLES:-Code,PlainCodeHash,PlainState}"
snapshots_dir="${SNAPSHOTS_DIR:-/Users/dlarimer/eth-bench/psitri/snapshots}"
run_root="${RUN_ROOT:-/Users/dlarimer/eth-bench/dual-kv-trace-$(date +%Y%m%d-%H%M%S)}"
native_bin="${NATIVE_SILKWORM_BIN:-$repo_root/build-mdbx/cmd/silkworm}"
psitri_bin="${PSITRI_SILKWORM_BIN:-$repo_root/build/cmd/silkworm}"

native_dir="$run_root/native"
psitri_dir="$run_root/psitri"
native_fifo="$run_root/native.kv.trace"
psitri_fifo="$run_root/psitri.kv.trace"
compare_log="$run_root/compare.log"
native_log="$run_root/native.stdout.log"
psitri_log="$run_root/psitri.stdout.log"

mkdir -p "$native_dir" "$psitri_dir"
ln -s "$snapshots_dir" "$native_dir/snapshots"
ln -s "$snapshots_dir" "$psitri_dir/snapshots"
mkfifo "$native_fifo" "$psitri_fifo"

cleanup() {
  if [[ -n "${native_pid:-}" ]] && kill -0 "$native_pid" 2>/dev/null; then
    kill -TERM "$native_pid" 2>/dev/null || true
  fi
  if [[ -n "${psitri_pid:-}" ]] && kill -0 "$psitri_pid" 2>/dev/null; then
    kill -TERM "$psitri_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "run_root=$run_root"
echo "stop_block=$stop_block"
echo "tables=$tables"
echo "native_bin=$native_bin"
echo "psitri_bin=$psitri_bin"

python3 "$repo_root/tools/compare_kv_trace.py" "$native_fifo" "$psitri_fifo" >"$compare_log" 2>&1 &
compare_pid=$!

common_args=(
  --chain mainnet
  --snapshots.enabled
  --snapshots.no_downloader
  --batchsize 64MB
  --log.stdout
  --log.nocolor
  --log.utc
)

env STOP_AT_BLOCK="$stop_block" \
  SILKWORM_KV_TRACE="$native_fifo" \
  SILKWORM_KV_TRACE_TABLES="$tables" \
  "$native_bin" \
  --datadir "$native_dir" \
  --private.api.addr 127.0.0.1:9490 \
  --sentry.api.addr 127.0.0.1:9491 \
  --exec.api.addr 127.0.0.1:9492 \
  --port 30433 \
  "${common_args[@]}" >"$native_log" 2>&1 &
native_pid=$!

env STOP_AT_BLOCK="$stop_block" \
  SILKWORM_KV_TRACE="$psitri_fifo" \
  SILKWORM_KV_TRACE_TABLES="$tables" \
  "$psitri_bin" \
  --datadir "$psitri_dir" \
  --private.api.addr 127.0.0.1:9590 \
  --sentry.api.addr 127.0.0.1:9591 \
  --exec.api.addr 127.0.0.1:9592 \
  --port 30533 \
  "${common_args[@]}" >"$psitri_log" 2>&1 &
psitri_pid=$!

set +e
wait "$compare_pid"
compare_rc=$?
set -e

cleanup
wait "$native_pid" 2>/dev/null || true
wait "$psitri_pid" 2>/dev/null || true

echo "compare_rc=$compare_rc"
echo "compare_log=$compare_log"
tail -20 "$compare_log" || true
exit "$compare_rc"
