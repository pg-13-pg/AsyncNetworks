# UCP Proxy Core Benchmark

Status: Not measured

This report compares the deterministic benchmark upstream directly with the
same upstream reached through UCP Proxy Core. Cells remain `Not measured`
until the commands below have produced raw output on the machine being
reported. Runs with socket errors or non-2xx/3xx responses are invalid and
must not be used to calculate a proxy-overhead percentage.

## Result Tables

### 1 KiB response

| Path | Connections | RPS | P50 | P95 | P99 | Errors | CPU | RSS | fd peak | Pool reuse rate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Direct | 64 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | N/A |
| Direct | 256 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | N/A |
| Direct | 1024 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | N/A |
| Proxy | 64 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |
| Proxy | 256 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |
| Proxy | 1024 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |

### 4 KiB response

| Path | Connections | RPS | P50 | P95 | P99 | Errors | CPU | RSS | fd peak | Pool reuse rate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Direct | 64 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | N/A |
| Direct | 256 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | N/A |
| Direct | 1024 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | N/A |
| Proxy | 64 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |
| Proxy | 256 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |
| Proxy | 1024 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |

### 16 KiB response

| Path | Connections | RPS | P50 | P95 | P99 | Errors | CPU | RSS | fd peak | Pool reuse rate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Direct | 64 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | N/A |
| Direct | 256 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | N/A |
| Direct | 1024 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | N/A |
| Proxy | 64 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |
| Proxy | 256 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |
| Proxy | 1024 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |

Record one table set for `workers = 1` and another for the intended worker
count. Do not combine measurements from different builds, worker counts, CPU
governors, or host-load conditions.

## Build

```bash
cmake -S . -B build-gateway-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=ON
cmake --build build-gateway-release -j2
ctest --test-dir build-gateway-release --output-on-failure
```

Record the exact commit and environment with the raw results:

```bash
git rev-parse HEAD
uname -a
cmake --version
c++ --version
lscpu
ulimit -n
```

## Start The Test Topology

Use three terminals. The mock accepts the fixed benchmark paths
`/bytes/<size>` and `/api/bytes/<size>` for sizes 1024, 4096, and 16384. The
second spelling is required because this proxy core performs prefix matching
but intentionally does not rewrite request paths.

```bash
build-gateway-release/bin/gateway_mock_upstream --port 9001
```

```bash
build-gateway-release/bin/gateway_mock_upstream --port 9002
```

```bash
build-gateway-release/bin/ucp_gateway config/gateway.conf
```

Verify both paths before applying load:

```bash
curl --fail --output /dev/null http://127.0.0.1:9001/bytes/1024
curl --fail --output /dev/null http://127.0.0.1:8080/api/bytes/1024
```

## Fixed Load Matrix

The benchmark driver runs a 10-second warm-up at 64 connections, then
30-second measured runs at 64, 256, and 1024 connections for every payload and
both paths:

```bash
bench/run_gateway_bench.sh \
  --gateway http://127.0.0.1:8080 \
  --direct http://127.0.0.1:9001 \
  --output benchmark-results \
  --threads "$(nproc)"
```

The exact commands for one 4 KiB pair are:

```bash
wrk --latency -t "$(nproc)" -c 64 -d 10s \
  http://127.0.0.1:9001/bytes/4096
wrk --latency -t "$(nproc)" -c 64 -d 30s \
  http://127.0.0.1:9001/bytes/4096
wrk --latency -t "$(nproc)" -c 256 -d 30s \
  http://127.0.0.1:9001/bytes/4096
wrk --latency -t "$(nproc)" -c 1024 -d 30s \
  http://127.0.0.1:9001/bytes/4096

wrk --latency -t "$(nproc)" -c 64 -d 10s \
  http://127.0.0.1:8080/api/bytes/4096
wrk --latency -t "$(nproc)" -c 64 -d 30s \
  http://127.0.0.1:8080/api/bytes/4096
wrk --latency -t "$(nproc)" -c 256 -d 30s \
  http://127.0.0.1:8080/api/bytes/4096
wrk --latency -t "$(nproc)" -c 1024 -d 30s \
  http://127.0.0.1:8080/api/bytes/4096
```

Repeat that exact sequence for 1024 and 16384 byte responses. Preserve every
raw stdout/stderr file. Report an invalid run rather than deleting its error
lines.

## 30-Minute Soak

Set `UCP_GATEWAY_PID` when automatic process detection is ambiguous. The
driver records gateway RSS, descriptor count, and available metrics before and
after the soak:

```bash
UCP_GATEWAY_PID="$(pgrep -n -x ucp_gateway)" \
bench/run_gateway_bench.sh \
  --gateway http://127.0.0.1:8080 \
  --direct http://127.0.0.1:9001 \
  --output benchmark-results \
  --threads "$(nproc)" \
  --soak
```

The load command executed by the soak is:

```bash
wrk --latency -t "$(nproc)" -c 256 -d 30m \
  http://127.0.0.1:8080/api/bytes/4096
```

Pool reuse rate is calculated only from gateway metric deltas:
`pool_reuses / pool_acquisitions`. Record `Not measured` when the metric log is
unavailable; do not infer reuse from RPS.

## Interpretation

Compare direct and proxy rows only at the same payload, concurrency, thread
count, and host state. Throughput without clean HTTP and socket-error counts is
not a valid result. A local-loopback measurement demonstrates software-path
cost; it does not predict production network throughput or prove a universal
latency bound.
