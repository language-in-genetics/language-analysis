#!/usr/bin/env bash
set -euo pipefail

WORK_ROOT="${WORK_ROOT:-/home/languageingenetics/crossref-prefilter-debug}"
DEBUG_DIR="${DEBUG_DIR:-$WORK_ROOT/debug}"
PIPELOG="${PIPELOG:-}"
WRAPPER_PID="${WRAPPER_PID:-}"
INTERVAL="${INTERVAL:-5}"
PG_INTERVAL="${PG_INTERVAL:-30}"
SNAPSHOT_TOP_N="${SNAPSHOT_TOP_N:-40}"
SYNC_DEBUG="${SYNC_DEBUG:-1}"

mkdir -p "$DEBUG_DIR/snapshots" "$DEBUG_DIR/proc"

SUMMARY_TSV="$DEBUG_DIR/summary.tsv"
MONITOR_LOG="$DEBUG_DIR/monitor.log"
CURRENT_HEARTBEAT="$DEBUG_DIR/heartbeat-current.txt"

if [[ ! -s "$SUMMARY_TSV" ]]; then
  printf 'epoch\tiso\tboot_id\twrapper_pid\tworker_pids\tprogress\tcache_tmp_bytes\tcache_bytes\tmem_available_kb\tmem_free_kb\tdirty_kb\twriteback_kb\tactive_anon_kb\tinactive_anon_kb\tactive_file_kb\tinactive_file_kb\tslab_kb\tcommitted_as_kb\tmem_psi\tio_psi\tcpu_psi\tloadavg\n' > "$SUMMARY_TSV"
fi

log() {
  printf '%s %s\n' "$(date -Is)" "$*" | tee -a "$MONITOR_LOG" >/dev/null
}

sync_file() {
  [[ "$SYNC_DEBUG" == "1" ]] || return 0
  [[ -e "$1" ]] || return 0
  sync -d "$1" 2>/dev/null || sync "$1" 2>/dev/null || true
}

meminfo_value() {
  local key="$1"
  awk -v key="$key" '$1 == key ":" { print $2; found=1; exit } END { if (!found) print 0 }' /proc/meminfo
}

psi_line() {
  local path="$1"
  if [[ -r "$path" ]]; then
    tr '\n' ';' < "$path"
  else
    printf 'unavailable'
  fi
}

cache_tmp_path() {
  if [[ -n "${CACHE_PATH:-}" ]]; then
    printf '%s.tmp' "$CACHE_PATH"
  else
    find "$WORK_ROOT" -maxdepth 1 -type f -name 'current-doi-cache.sqlite.tmp' -print -quit 2>/dev/null || true
  fi
}

cache_path() {
  if [[ -n "${CACHE_PATH:-}" ]]; then
    printf '%s' "$CACHE_PATH"
  else
    find "$WORK_ROOT" -maxdepth 1 -type f -name 'current-doi-cache.sqlite' -print -quit 2>/dev/null || true
  fi
}

file_size() {
  local path="$1"
  if [[ -n "$path" && -e "$path" ]]; then
    stat -c '%s' "$path" 2>/dev/null || printf -- '-1'
  else
    printf -- '-1'
  fi
}

worker_pids() {
  pgrep -u "$(id -u)" -f 'crossrefcachebuild|crossrefclassify' 2>/dev/null | tr '\n' ',' | sed 's/,$//' || true
}

is_running() {
  if [[ -n "$WRAPPER_PID" ]] && kill -0 "$WRAPPER_PID" 2>/dev/null; then
    return 0
  fi
  [[ -n "$(worker_pids)" ]]
}

copy_proc_file() {
  local pid="$1"
  local name="$2"
  local dest="$3"
  if [[ -r "/proc/$pid/$name" ]]; then
    cp "/proc/$pid/$name" "$dest/$pid-$name" 2>/dev/null || true
  fi
}

write_pid_snapshot() {
  local pid="$1"
  local dest="$2"
  mkdir -p "$dest"
  {
    printf 'pid=%s\n' "$pid"
    printf 'cmdline='
    tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true
    printf '\n'
    printf 'wchan='
    cat "/proc/$pid/wchan" 2>/dev/null || true
    printf '\n'
  } > "$dest/$pid-meta.txt"
  copy_proc_file "$pid" status "$dest"
  copy_proc_file "$pid" stat "$dest"
  copy_proc_file "$pid" statm "$dest"
  copy_proc_file "$pid" io "$dest"
  copy_proc_file "$pid" smaps_rollup "$dest"
}

write_pg_snapshot() {
  local out="$1"
  timeout 8 psql -XAt -d "${PGDATABASE:-crossref}" <<'SQL' > "$out" 2>&1 || true
SELECT now(), pid, backend_type, state, wait_event_type, wait_event,
       EXTRACT(epoch FROM now() - COALESCE(query_start, backend_start))::bigint AS age_s,
       left(regexp_replace(query, E'[\\n\\r\\t]+', ' ', 'g'), 240)
FROM pg_stat_activity
WHERE datname = current_database()
ORDER BY age_s DESC NULLS LAST, pid;
SQL
}

log "starting monitor: work_root=$WORK_ROOT debug_dir=$DEBUG_DIR wrapper_pid=${WRAPPER_PID:-none} interval=$INTERVAL"

last_pg_epoch=0
while is_running; do
  epoch="$(date +%s)"
  iso="$(date -Is)"
  boot_id="$(cat /proc/sys/kernel/random/boot_id 2>/dev/null || true)"
  pids="$(worker_pids)"
  tmp_path="$(cache_tmp_path)"
  final_cache_path="$(cache_path)"
  tmp_size="$(file_size "$tmp_path")"
  final_size="$(file_size "$final_cache_path")"
  progress=""
  if [[ -n "$PIPELOG" && -r "$PIPELOG" ]]; then
    progress="$(grep -E 'loaded [0-9]+ DOI cache records|finalizing SQLite cache|classification complete|starting Crossref prefilter pipeline' "$PIPELOG" | tail -n 1 || true)"
  fi

  mem_available="$(meminfo_value MemAvailable)"
  mem_free="$(meminfo_value MemFree)"
  dirty="$(meminfo_value Dirty)"
  writeback="$(meminfo_value Writeback)"
  active_anon="$(meminfo_value 'Active(anon)')"
  inactive_anon="$(meminfo_value 'Inactive(anon)')"
  active_file="$(meminfo_value 'Active(file)')"
  inactive_file="$(meminfo_value 'Inactive(file)')"
  slab="$(meminfo_value Slab)"
  committed_as="$(meminfo_value Committed_AS)"
  mem_psi="$(psi_line /proc/pressure/memory)"
  io_psi="$(psi_line /proc/pressure/io)"
  cpu_psi="$(psi_line /proc/pressure/cpu)"
  loadavg="$(cat /proc/loadavg)"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$epoch" "$iso" "$boot_id" "${WRAPPER_PID:-}" "$pids" "$progress" "$tmp_size" "$final_size" \
    "$mem_available" "$mem_free" "$dirty" "$writeback" "$active_anon" "$inactive_anon" \
    "$active_file" "$inactive_file" "$slab" "$committed_as" "$mem_psi" "$io_psi" "$cpu_psi" "$loadavg" \
    >> "$SUMMARY_TSV"

  snapshot="$DEBUG_DIR/snapshots/$epoch.txt"
  {
    printf 'timestamp=%s\n' "$iso"
    printf 'epoch=%s\nboot_id=%s\nwrapper_pid=%s\nworker_pids=%s\n' "$epoch" "$boot_id" "${WRAPPER_PID:-}" "$pids"
    printf 'progress=%s\ncache_tmp=%s\ncache_tmp_bytes=%s\ncache=%s\ncache_bytes=%s\n' "$progress" "$tmp_path" "$tmp_size" "$final_cache_path" "$final_size"
    printf '\n== uptime ==\n'
    uptime || true
    printf '\n== free ==\n'
    free -h || true
    printf '\n== meminfo selected ==\n'
    grep -E '^(MemTotal|MemFree|MemAvailable|Buffers|Cached|SwapCached|Active|Inactive|Dirty|Writeback|AnonPages|Mapped|Shmem|KReclaimable|Slab|SReclaimable|SUnreclaim|KernelStack|PageTables|Committed_AS|CommitLimit|VmallocUsed):' /proc/meminfo || true
    printf '\n== pressure ==\n'
    for f in /proc/pressure/cpu /proc/pressure/io /proc/pressure/memory; do
      printf '%s\n' "$f"
      cat "$f" 2>/dev/null || true
    done
    printf '\n== vmstat counters ==\n'
    grep -E '^(pgfault|pgmajfault|oom_kill|compact_|pgscan|pgsteal|pswp|numa_|allocstall)' /proc/vmstat || true
    printf '\n== vmstat sample ==\n'
    vmstat -SM 1 2 2>/dev/null || true
    printf '\n== iostat sample ==\n'
    iostat -xz 1 2 2>/dev/null || true
    printf '\n== top rss processes ==\n'
    ps -eo pid,ppid,user,state,ni,pri,psr,%cpu,%mem,rss,vsz,wchan:24,comm,args --sort=-rss | head -n "$SNAPSHOT_TOP_N" || true
    printf '\n== target processes ==\n'
    ps -u "$(id -u)" -o pid,ppid,state,ni,pri,psr,%cpu,%mem,rss,vsz,wchan:24,etime,cmd | grep -E 'crossrefcachebuild|crossrefclassify|run_crossref_prefilter_pipeline|monitor_crossref_prefilter_debug' | grep -v grep || true
    printf '\n== disk ==\n'
    df -h /home /dbtemp "$WORK_ROOT" 2>/dev/null || true
    printf '\n== pipeline tail ==\n'
    if [[ -n "$PIPELOG" && -r "$PIPELOG" ]]; then
      tail -n 80 "$PIPELOG" || true
    fi
  } > "$snapshot"

  proc_dest="$DEBUG_DIR/proc/$epoch"
  for pid in ${pids//,/ }; do
    [[ -n "$pid" ]] && write_pid_snapshot "$pid" "$proc_dest"
  done
  if [[ -n "$WRAPPER_PID" ]] && [[ -d "/proc/$WRAPPER_PID" ]]; then
    write_pid_snapshot "$WRAPPER_PID" "$proc_dest"
  fi

  if (( epoch - last_pg_epoch >= PG_INTERVAL )); then
    write_pg_snapshot "$DEBUG_DIR/postgres-$epoch.txt"
    last_pg_epoch="$epoch"
  fi

  {
    printf 'timestamp=%s\n' "$iso"
    printf 'epoch=%s\n' "$epoch"
    printf 'worker_pids=%s\n' "$pids"
    printf 'progress=%s\n' "$progress"
    printf 'cache_tmp_bytes=%s\n' "$tmp_size"
    printf 'mem_available_kb=%s\n' "$mem_available"
  } > "$CURRENT_HEARTBEAT"

  sync_file "$SUMMARY_TSV"
  sync_file "$snapshot"
  sync_file "$CURRENT_HEARTBEAT"
  sync_file "$DEBUG_DIR/kernel-follow.log"
  sync_file "$DEBUG_DIR/journal-follow.log"
  sync_file "$DEBUG_DIR/dmesg-follow.log"
  sleep "$INTERVAL"
done

log "monitor stopping: wrapper_pid=${WRAPPER_PID:-none}; no target process remains"
sync_file "$MONITOR_LOG"
