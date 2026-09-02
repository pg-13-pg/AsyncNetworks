#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: run_gateway_bench.sh --gateway URL --direct URL --output DIR --threads N [--soak]

Required options:
  --gateway URL   Gateway base URL, for example http://127.0.0.1:8080
  --direct URL    Direct upstream base URL, for example http://127.0.0.1:9001
  --output DIR    Parent directory for timestamped benchmark results
  --threads N     Positive wrk thread count

Optional:
  --soak          Run the 30-minute gateway soak after the fixed matrix
  -h, --help      Show this help without starting a benchmark

For soak process snapshots, set UCP_GATEWAY_PID when more than one
ucp_gateway process exists. UCP_GATEWAY_LOG may override logs/ucp_gateway.log.
EOF
}

die() {
    printf 'run_gateway_bench.sh: %s\n' "$*" >&2
    exit 2
}

gateway=""
direct=""
output=""
threads=""
soak=0

while (($# > 0)); do
    case "$1" in
        --gateway)
            (($# >= 2)) || die "--gateway requires a value"
            gateway="$2"
            shift 2
            ;;
        --direct)
            (($# >= 2)) || die "--direct requires a value"
            direct="$2"
            shift 2
            ;;
        --output)
            (($# >= 2)) || die "--output requires a value"
            output="$2"
            shift 2
            ;;
        --threads)
            (($# >= 2)) || die "--threads requires a value"
            threads="$2"
            shift 2
            ;;
        --soak)
            soak=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "unknown option: $1"
            ;;
    esac
done

[[ -n "$gateway" ]] || die "--gateway is required"
[[ -n "$direct" ]] || die "--direct is required"
[[ -n "$output" ]] || die "--output is required"
[[ "$threads" =~ ^[1-9][0-9]*$ ]] || die "--threads must be a positive integer"
command -v wrk >/dev/null 2>&1 || die "wrk is required"

gateway="${gateway%/}"
direct="${direct%/}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
run_dir="${output%/}/gateway-benchmark-${timestamp}-$$"
mkdir -p "$run_dir"

{
    printf 'captured_at_utc=%s\n' "$timestamp"
    printf 'gateway=%s\n' "$gateway"
    printf 'direct=%s\n' "$direct"
    printf 'threads=%s\n' "$threads"
    printf 'soak=%s\n\n' "$soak"
    printf '$ uname -a\n'
    uname -a 2>&1
    printf '\n$ cmake --version\n'
    cmake --version 2>&1
    printf '\n$ c++ --version\n'
    c++ --version 2>&1
    printf '\n$ lscpu\n'
    lscpu 2>&1
    printf '\n$ ulimit -n\n'
    ulimit -n
} >"$run_dir/environment.txt"

check_wrk_output() {
    local stdout_file="$1"
    local stderr_file="$2"
    if grep -Eq 'Socket errors:.*[[:space:]][1-9][0-9]*' \
        "$stdout_file" "$stderr_file"; then
        return 1
    fi
    if grep -Eq 'Non-2xx or 3xx responses:[[:space:]]*[1-9][0-9]*' \
        "$stdout_file" "$stderr_file"; then
        return 1
    fi
}

run_wrk() {
    local label="$1"
    local connections="$2"
    local duration="$3"
    local url="$4"
    local stdout_file="$run_dir/${label}.stdout.txt"
    local stderr_file="$run_dir/${label}.stderr.txt"

    printf 'Running %-38s %s\n' "$label" "$url"
    local status=0
    wrk --latency -t "$threads" -c "$connections" -d "$duration" "$url" \
        >"$stdout_file" 2>"$stderr_file" || status=$?
    if ((status != 0)); then
        printf 'wrk exited %d for %s; see %s and %s\n' \
            "$status" "$label" "$stdout_file" "$stderr_file" >&2
        return "$status"
    fi
    if ! check_wrk_output "$stdout_file" "$stderr_file"; then
        printf 'wrk reported request errors for %s; result is invalid\n' \
            "$label" >&2
        return 1
    fi
}

for size in 1024 4096 16384; do
    for mode in direct gateway; do
        if [[ "$mode" == "direct" ]]; then
            url="$direct/bytes/$size"
        else
            url="$gateway/api/bytes/$size"
        fi
        run_wrk "$mode-${size}-warmup-c64" 64 10s "$url"
        run_wrk "$mode-${size}-measured-c64" 64 30s "$url"
        run_wrk "$mode-${size}-measured-c256" 256 30s "$url"
        run_wrk "$mode-${size}-measured-c1024" 1024 30s "$url"
    done
done

gateway_pid=""
gateway_pid_reason=""
find_gateway_pid() {
    if [[ -n "${UCP_GATEWAY_PID:-}" ]]; then
        if [[ "$UCP_GATEWAY_PID" =~ ^[1-9][0-9]*$ \
            && -d "/proc/$UCP_GATEWAY_PID" ]]; then
            gateway_pid="$UCP_GATEWAY_PID"
        else
            gateway_pid_reason="UCP_GATEWAY_PID is not a live process"
        fi
        return
    fi

    local -a candidates=()
    mapfile -t candidates < <(pgrep -x ucp_gateway || true)
    if ((${#candidates[@]} == 1)); then
        gateway_pid="${candidates[0]}"
    elif ((${#candidates[@]} == 0)); then
        gateway_pid_reason="no ucp_gateway process found"
    else
        gateway_pid_reason="multiple ucp_gateway processes found; set UCP_GATEWAY_PID"
    fi
}

record_process_snapshot() {
    local phase="$1"
    local destination="$run_dir/soak-${phase}-process.txt"
    {
        printf 'captured_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        if [[ -z "$gateway_pid" ]]; then
            printf 'gateway_pid=Not collected\nreason=%s\n' "$gateway_pid_reason"
            return
        fi
        printf 'gateway_pid=%s\n' "$gateway_pid"
        if [[ -r "/proc/$gateway_pid/status" ]]; then
            grep -E '^(VmRSS|VmPeak|Threads):' "/proc/$gateway_pid/status" \
                || printf 'rss=Not collected\n'
        else
            printf 'rss=Not collected\nreason=process status is unreadable\n'
        fi
        local fd_count=""
        if [[ -d "/proc/$gateway_pid/fd" ]] \
            && fd_count="$(find "/proc/$gateway_pid/fd" -mindepth 1 \
                -maxdepth 1 -type l 2>/dev/null | wc -l)"; then
            printf 'fd_count=%s\n' "$fd_count"
        else
            printf 'fd_count=Not collected\nreason=process fd directory is unreadable\n'
        fi
    } >"$destination"
}

capture_gateway_log() {
    local phase="$1"
    local source="${UCP_GATEWAY_LOG:-logs/ucp_gateway.log}"
    local destination="$run_dir/soak-${phase}-gateway.log"
    if [[ -r "$source" ]]; then
        cp "$source" "$destination"
    else
        printf 'Not collected: gateway log is not readable at %s\n' \
            "$source" >"$destination"
    fi
}

if ((soak)); then
    find_gateway_pid
    record_process_snapshot before
    capture_gateway_log before
    soak_status=0
    run_wrk "gateway-4096-soak-c256" 256 30m \
        "$gateway/api/bytes/4096" || soak_status=$?
    record_process_snapshot after
    capture_gateway_log after
    ((soak_status == 0)) || exit "$soak_status"
fi

printf 'Benchmark evidence written to %s\n' "$run_dir"
