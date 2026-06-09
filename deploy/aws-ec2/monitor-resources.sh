#!/usr/bin/env bash
set -euo pipefail

DURATION_SECONDS="${1:-1200}"
INTERVAL_SECONDS="${2:-5}"
OUTPUT_FILE="${3:-${HOME}/airflow/resource-monitor-$(date -u +'%Y%m%dT%H%M%SZ').csv}"

for value_name in DURATION_SECONDS INTERVAL_SECONDS; do
  value="${!value_name}"
  if [[ ! "${value}" =~ ^[1-9][0-9]*$ ]]; then
    echo "${value_name} must be a positive integer, received: ${value}" >&2
    exit 2
  fi
done

mkdir -p "$(dirname "${OUTPUT_FILE}")"
umask 077
printf '%s\n' \
  'timestamp_utc,mem_available_mib,mem_used_mib,swap_used_mib,load_1m,cpu_used_percent,scheduler_cgroup_mib,webserver_cgroup_mib,postgres_rss_mib,airflow_process_rss_mib' \
  >"${OUTPUT_FILE}"

read_cpu() {
  read -r _ user nice system idle iowait irq softirq steal _ < /proc/stat
  CPU_IDLE=$((idle + iowait))
  CPU_TOTAL=$((user + nice + system + idle + iowait + irq + softirq + steal))
}

memory_current_bytes() {
  local unit="$1"
  local value
  value="$(systemctl show "${unit}" --property=MemoryCurrent --value 2>/dev/null || true)"
  [[ "${value}" =~ ^[0-9]+$ ]] && printf '%s' "${value}" || printf '0'
}

process_rss_kib() {
  local pattern="$1"
  ps -eo rss=,args= | awk -v pattern="${pattern}" '$0 ~ pattern {sum += $1} END {print sum + 0}'
}

read_cpu
previous_idle="${CPU_IDLE}"
previous_total="${CPU_TOTAL}"
start_epoch="$(date +%s)"
end_epoch=$((start_epoch + DURATION_SECONDS))
trap 'end_epoch=0' INT TERM

printf 'Recording EC2 resources every %s seconds for %s seconds.\n' \
  "${INTERVAL_SECONDS}" "${DURATION_SECONDS}"
printf 'Output: %s\n' "${OUTPUT_FILE}"
printf 'Leave this terminal open while the Airflow task runs. Press Control+C to stop early.\n'

while (( $(date +%s) < end_epoch )); do
  sleep "${INTERVAL_SECONDS}" || true
  (( $(date +%s) < end_epoch )) || break
  read_cpu
  total_delta=$((CPU_TOTAL - previous_total))
  idle_delta=$((CPU_IDLE - previous_idle))
  if (( total_delta > 0 )); then
    cpu_used="$(awk -v total="${total_delta}" -v idle="${idle_delta}" 'BEGIN {printf "%.1f", 100 * (total - idle) / total}')"
  else
    cpu_used="0.0"
  fi
  previous_idle="${CPU_IDLE}"
  previous_total="${CPU_TOTAL}"

  mem_total_kib="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
  mem_available_kib="$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)"
  swap_total_kib="$(awk '/^SwapTotal:/ {print $2}' /proc/meminfo)"
  swap_free_kib="$(awk '/^SwapFree:/ {print $2}' /proc/meminfo)"
  scheduler_bytes="$(memory_current_bytes telehitch-airflow-scheduler.service)"
  webserver_bytes="$(memory_current_bytes telehitch-airflow-webserver.service)"
  postgres_rss_kib="$(process_rss_kib '[p]ostgres')"
  airflow_rss_kib="$(process_rss_kib '[a]irflow')"
  load_1m="$(awk '{print $1}' /proc/loadavg)"

  awk -v timestamp="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
      -v available="${mem_available_kib}" \
      -v used="$((mem_total_kib - mem_available_kib))" \
      -v swap_used="$((swap_total_kib - swap_free_kib))" \
      -v load_value="${load_1m}" \
      -v cpu="${cpu_used}" \
      -v scheduler="${scheduler_bytes}" \
      -v webserver="${webserver_bytes}" \
      -v postgres="${postgres_rss_kib}" \
      -v airflow="${airflow_rss_kib}" \
      'BEGIN {printf "%s,%.1f,%.1f,%.1f,%s,%s,%.1f,%.1f,%.1f,%.1f\n", timestamp, available/1024, used/1024, swap_used/1024, load_value, cpu, scheduler/1048576, webserver/1048576, postgres/1024, airflow/1024}' \
      >>"${OUTPUT_FILE}"
done

printf '\nResource summary:\n'
awk -F, 'NR == 2 {
  min_available=$2; max_used=$3; max_swap=$4; max_load=$5; max_cpu=$6;
  max_scheduler=$7; max_webserver=$8; max_postgres=$9; max_airflow=$10
}
NR > 2 {
  if ($2 < min_available) min_available=$2;
  if ($3 > max_used) max_used=$3;
  if ($4 > max_swap) max_swap=$4;
  if ($5 > max_load) max_load=$5;
  if ($6 > max_cpu) max_cpu=$6;
  if ($7 > max_scheduler) max_scheduler=$7;
  if ($8 > max_webserver) max_webserver=$8;
  if ($9 > max_postgres) max_postgres=$9;
  if ($10 > max_airflow) max_airflow=$10
}
END {
  if (NR < 2) { print "No samples were recorded."; exit }
  printf "Minimum available RAM: %.1f MiB\n", min_available;
  printf "Maximum used RAM estimate: %.1f MiB\n", max_used;
  printf "Maximum swap used: %.1f MiB\n", max_swap;
  printf "Maximum 1-minute load: %.2f\n", max_load;
  printf "Maximum sampled CPU use: %.1f%%\n", max_cpu;
  printf "Maximum scheduler cgroup memory: %.1f MiB\n", max_scheduler;
  printf "Maximum webserver cgroup memory: %.1f MiB\n", max_webserver;
  printf "Maximum PostgreSQL RSS: %.1f MiB\n", max_postgres;
  printf "Maximum Airflow process RSS: %.1f MiB\n", max_airflow
}' "${OUTPUT_FILE}"

printf '\nKernel OOM evidence since monitoring began:\n'
oom_pattern='out of memory|oom-kill|killed process'
if sudo -n true 2>/dev/null; then
  if sudo -n journalctl -k --since "@${start_epoch}" --no-pager | grep -Eiq "${oom_pattern}"; then
    sudo -n journalctl -k --since "@${start_epoch}" --no-pager | grep -Ei "${oom_pattern}" || true
    echo "WARNING: the kernel reported an out-of-memory event."
  else
    echo "No kernel OOM events found."
  fi
elif journalctl -k --since "@${start_epoch}" --no-pager >/dev/null 2>&1; then
  if journalctl -k --since "@${start_epoch}" --no-pager | grep -Eiq "${oom_pattern}"; then
    journalctl -k --since "@${start_epoch}" --no-pager | grep -Ei "${oom_pattern}" || true
    echo "WARNING: the kernel reported an out-of-memory event."
  else
    echo "No kernel OOM events found."
  fi
else
  echo "OOM check skipped: kernel logs require sudo, and passwordless sudo is unavailable."
  echo "RAM, swap, CPU, load, and process-memory samples were still recorded successfully."
fi

printf '\nDetailed samples: %s\n' "${OUTPUT_FILE}"
