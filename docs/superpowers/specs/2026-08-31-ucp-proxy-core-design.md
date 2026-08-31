# UCP Proxy Core Design

## Status

- Date: 2026-08-31
- Scope: one-week, learning-oriented autumn recruitment project
- Decision: build a high-performance HTTP/1.1 reverse-proxy data-plane core, not a complete API gateway

## 1. Context

The repository already provides a C++20 coroutine server framework based on
`io_uring`, including an `EventLoop`, `TcpServer`, `TcpConnection`, registered
buffers, a bounded cross-thread task queue, simple HTTP framing, logging, and
benchmark programs.

The project should demonstrate low-level C++ backend skills that complement the
separate Raft KV project. The Raft KV project remains focused on consensus,
replication, persistence, snapshots, and failover. UCP Proxy Core focuses on
asynchronous networking, coroutine lifetime, backpressure, connection reuse,
error handling, and reproducible performance measurements.

The current network framework is not yet a safe base for a proxy. In particular:

- a full cross-thread task queue can silently drop lifecycle work;
- each connection embeds only one read, write, and timeout context;
- SQE exhaustion can leave a suspended coroutine without a completion path;
- partial writes and the current output-buffer backpressure semantics are not
  sufficient for proxy traffic;
- pending I/O is not cancelled and drained through an explicit shutdown state
  machine;
- there is no asynchronous outbound connect path;
- the current HTTP codec does not retain enough request and response framing
  information for transparent forwarding;
- CTest currently registers no automated tests.

The design therefore treats the proxy as a thin vertical slice over a corrected
network runtime, not as a collection of API-management features.

## 2. Goals

The first deliverable will:

1. Make every submitted I/O operation complete exactly once with success,
   EOF, cancellation, timeout, or an explicit error.
2. Add asynchronous outbound TCP connection support.
3. Proxy bounded HTTP/1.1 requests and responses without buffering whole
   bodies.
4. Reuse upstream connections within the owning worker thread.
5. Apply explicit memory, queue, connection, and protocol limits.
6. Shut down without dangling coroutine handles, leaked descriptors, or live
   operations referencing destroyed objects.
7. Provide repeatable correctness, sanitizer, load, and soak tests.

The implementation should favor correctness and explainable ownership over a
headline QPS result.

## 3. Non-Goals

The first deliverable will not implement:

- a management UI or control-plane API;
- authentication, authorization, API keys, or OAuth;
- service discovery or dynamic configuration;
- TLS termination;
- HTTP/2, HTTP/3, QUIC, WebSocket, or CONNECT tunneling;
- chunked transfer encoding or concurrent processing of pipelined requests;
- distributed rate limiting, circuit breakers, or active health checks;
- a plugin system or service-mesh integration;
- cross-thread sharing of established connections;
- automatic retries after selecting an upstream.

These exclusions are part of the product boundary, not hidden incomplete work.

## 4. Architecture

```text
Client
  |
  v
TcpServer / downstream TcpConnection
  |
  v
ProxySession coroutine
  |-- HttpFramer
  |-- RouteTable
  |-- RoundRobinBalancer
  `-- worker-local UpstreamPool
                         |
                         v
                 AsyncConnect
                         |
                         v
                upstream TcpConnection
```

Each accepted downstream connection is assigned to one worker `EventLoop`.
The `ProxySession`, its upstream lease, all socket operations, and all buffers
remain on that worker until the session ends. This preserves connection
affinity and keeps the forwarding hot path free of cross-thread locks.

The system is divided into five modules:

### 4.1 Runtime

The runtime owns the `io_uring`, pending submissions, completion dispatch,
operation cancellation, and shutdown drain. It does not understand HTTP or
proxy routing.

### 4.2 Network

The network module owns established TCP connections, asynchronous connect,
bounded reads, complete writes, half-close, force-close, and socket options.
It reports errors but does not map them to HTTP status codes.

### 4.3 Protocol

The protocol module incrementally finds HTTP message boundaries, validates the
supported subset, extracts routing and connection metadata, and retains raw
header fields needed for forwarding. It does not choose an upstream.

### 4.4 Proxy

The proxy module owns a downstream request lifecycle, static route matching,
endpoint selection, upstream leases, streaming, and the mapping from transport
failures to HTTP errors.

### 4.5 Application and Observability

The application module loads immutable startup configuration, starts workers,
aggregates worker-local metrics, handles process signals, and coordinates
graceful shutdown.

## 5. Runtime Operation Model

The embedded connection-wide read, write, and timeout contexts will be replaced
for new code paths by one `IoOperation` per submitted operation.

An `IoOperation` records:

- a stable operation identifier and operation type;
- the file descriptor and buffer metadata required by the SQE;
- the awaiting coroutine handle;
- a weak association with the owning connection or session;
- the kernel result and normalized error;
- submission, completion, timeout, and cancellation state;
- enough state to ensure the coroutine is resumed at most once.

The `EventLoop` owns every in-flight operation until all CQEs that may refer to
it have reached a terminal state. A connection or coroutine may request
cancellation but may not free the operation early.

Linked I/O and timeout SQEs belong to one logical operation group. The first
terminal result selects the externally visible result; later timeout or cancel
CQEs only finish kernel cleanup and never resume the coroutine a second time.

Submission follows this contract:

1. Try to obtain and prepare the required SQE set.
2. If the SQ ring lacks capacity, place the operation in a bounded loop-local
   pending-submission queue.
3. If both paths are unavailable, do not suspend the coroutine; return an
   explicit resource-exhaustion result.
4. Every accepted operation therefore has a completion or cancellation path.

Normal cross-thread work uses a bounded queue and returns an enqueue result.
Lifecycle work such as connection destruction uses a separate guaranteed
control path. The control path may be lower volume and mutex protected; it must
never silently discard work.

I/O APIs return a structured result that distinguishes transferred bytes, EOF,
timeout, cancellation, connection reset, and local resource exhaustion. The
proxy does not infer error categories from ambiguous integer values.

## 6. Connection and Session Lifetime

One `ProxySession` owns the application-level lifetime of one downstream
connection. It processes no more than one request at a time.

```text
ReadHeaders
  -> Validate
  -> StreamRequestBody
  -> SelectUpstream
  -> AcquireConnection
  -> WriteAllRequest
  -> ReadResponseHeaders
  -> StreamResponseBody
  -> ReleaseOrCloseUpstream
  -> ReadHeaders or Close
```

The session has one cancellation source shared by its outstanding downstream
and upstream operations. A downstream EOF or error cancels upstream work. An
upstream failure closes or releases resources according to message progress.

`writeAll` loops until all bytes have been written or a terminal error occurs.
The source buffer remains owned by the awaiting coroutine or connection for the
entire operation.

Request and response bodies are forwarded in bounded chunks. Backpressure is
coupled: the proxy does not issue the next read from one side while the current
chunk is still queued for the other side. This bounds per-session memory and
naturally slows a fast producer when its consumer is slow.

An upstream connection can return to the pool only when:

- the complete response has been consumed;
- no operation remains in flight;
- neither peer requested connection close;
- the parser has no unexplained trailing state;
- the connection is not timed out, cancelled, or errored.

Otherwise the lease destroys the connection.

## 7. HTTP/1.1 Support Boundary

The protocol layer supports GET and POST with HTTP/1.1 keep-alive and explicit
`Content-Length` framing.

The incremental framer:

- recognizes a header block ending in `\r\n\r\n` across arbitrary read
  boundaries;
- extracts method, path, version, status, `Content-Length`, and `Connection`;
- preserves header lines needed for forwarding;
- rejects conflicting or invalid content lengths;
- rejects transfer encoding, Upgrade, and CONNECT;
- enforces configurable header and body limits before allocation growth;
- leaves bytes beyond the current message in the owning input buffer.

Only one request is processed at a time. If a client sends bytes for a later
request early, they remain buffered and are not parsed or forwarded until the
current response is complete. The first deliverable does not execute pipelined
requests concurrently.

Hop-by-hop connection headers are normalized for each proxy leg. Unsupported
hop-by-hop features are rejected instead of being forwarded ambiguously.

Responses with bodies require a valid `Content-Length`. Responses that cannot
have a body, such as 204 and 304, are treated as zero-length. Other
close-delimited responses are outside the first-deliverable boundary and make
the upstream connection unusable.

Default limits are:

- maximum header bytes: 16 KiB;
- maximum body bytes: 1 MiB;
- one in-flight request per downstream connection.

## 8. Routing and Upstream Pool

Configuration is loaded once at startup and remains immutable on worker hot
paths. A route contains a path prefix, an ordered list of upstream endpoints,
connect and response deadlines, and pool limits.

`RouteTable` performs longest-prefix matching. The first implementation uses a
small sorted table because route counts are expected to be low; a trie is not
required.

Every worker owns an `UpstreamPool` keyed by endpoint. Each endpoint bucket
tracks active and idle counts plus a deque of reusable connections. The pool:

1. reuses an eligible idle connection;
2. otherwise creates a connection with `AsyncConnect` while below its limit;
3. returns an overload result when the per-worker endpoint limit is exhausted;
4. caps idle connections and discards expired idle entries;
5. returns resources through an RAII `UpstreamLease`.

Round-robin state is worker-local. There is no global atomic selector. A failed
connection is discarded, but the first version does not mark an endpoint
unhealthy or retry the current request on another endpoint.

The initial configuration shape is:

```ini
[gateway]
listen_ip=0.0.0.0
listen_port=8080
header_limit=16384
body_limit=1048576
graceful_shutdown_ms=5000

[route.api]
prefix=/api/
upstreams=127.0.0.1:9001,127.0.0.1:9002
connect_timeout_ms=500
response_timeout_ms=3000
max_connections_per_worker=128
max_idle_per_worker=32
idle_timeout_ms=30000
```

## 9. Error Mapping

The proxy emits an HTTP error only if no downstream response bytes have been
sent. Once a response has started, a later upstream failure closes the
downstream connection because a second valid response cannot be produced.

| Condition | Result |
| --- | --- |
| Invalid HTTP syntax | 400 Bad Request |
| Header limit exceeded | 431 Request Header Fields Too Large |
| Body limit exceeded | 413 Payload Too Large |
| No matching route | 404 Not Found |
| Pool capacity exhausted | 503 Service Unavailable |
| Upstream connect or transport failure | 502 Bad Gateway |
| Upstream protocol failure before response | 502 Bad Gateway |
| Upstream connect or response deadline | 504 Gateway Timeout |
| Downstream closes | Cancel upstream work and send no response |
| Failure after response bytes are sent | Close downstream connection |

The first version performs no automatic retry. This avoids duplicating a POST
or a partially transmitted request. Retry policy can be designed separately
after request progress and idempotency are represented explicitly.

## 10. Shutdown

Graceful shutdown uses this order:

1. Stop accepting new connections.
2. Reject creation of new proxy sessions and upstream connections.
3. Allow active sessions to finish within the configured grace period.
4. Cancel remaining session operations after the deadline.
5. Consume terminal I/O, timeout, and cancellation CQEs.
6. Close downstream, active upstream, and idle upstream connections.
7. Unregister fixed buffers and release buffer memory.
8. Close the wakeup descriptor and destroy the `io_uring`.

Registered resources and operation storage must outlive every CQE that can
reference them.

## 11. Metrics

Workers update local counters to avoid contention. The application aggregates
them outside the forwarding hot path.

Required metrics are:

- accepted and active downstream connections;
- active and idle upstream connections;
- completed requests and responses by status class;
- connect, protocol, timeout, cancellation, overload, and queue-full errors;
- bytes received and forwarded in each direction;
- upstream pool acquisition count and reuse count;
- observed output-buffer and pending-task high-water marks;
- request latency histogram data sufficient for P50, P95, and P99 reporting.

A full Prometheus server is outside the first scope. A periodic text snapshot or
terminal report is sufficient if metric names and aggregation semantics remain
stable.

## 12. Testing

CTest is the authoritative local test entry point.

### 12.1 Unit Tests

Unit tests cover:

- headers split at every possible byte boundary;
- header plus partial body and multiple body chunks;
- extra bytes retained after a framed message;
- malformed start lines and headers;
- invalid, duplicate, and conflicting content lengths;
- header and body limits;
- keep-alive and connection-close decisions;
- longest-prefix route matching;
- deterministic worker-local round robin;
- upstream lease return versus discard rules.

### 12.2 Integration Tests

A controllable mock upstream covers:

- GET and POST forwarding;
- fragmented downstream requests and upstream responses;
- downstream and upstream keep-alive reuse;
- upstream refusal, reset, malformed response, and timeout;
- pool capacity exhaustion;
- downstream disconnect during connect, request upload, and response download;
- graceful shutdown with idle and active sessions;
- repeated start and stop with descriptor-count checks.

### 12.3 Sanitizers

ASan and UBSan runs must complete without findings. TSan is run separately and
documented because kernel-driven `io_uring` access can require suppression or
interpretation; a TSan limitation is not reported as a clean pass without
evidence.

## 13. Performance Evaluation

The installed `/usr/bin/wrk` is the primary external HTTP load generator. Tests
compare direct access to the mock upstream with access through UCP Proxy Core.

The standard matrix is:

| Dimension | Values |
| --- | --- |
| Concurrent connections | 64, 256, 1024 |
| Response payload | 1 KiB, 4 KiB, 16 KiB |
| Worker count | 1, available CPU core count |
| Warm-up | 10 seconds |
| Measured run | 30 seconds |
| Soak run | 30 minutes |

Each report records:

- requests per second;
- P50, P95, and P99 latency;
- request and connection errors;
- process CPU, RSS, and descriptor count;
- pool reuse rate and timeout counts;
- kernel, compiler, hardware, build type, worker count, and relevant runtime
  configuration.

The report contains raw commands and compares the same upstream payload and
load-generator conditions. No absolute throughput target is promised before a
baseline is measured.

## 14. One-Week Delivery Boundary

Work is prioritized in this order:

```text
I/O correctness
  > runnable end-to-end proxy path
  > cancellation and resource release
  > automated tests
  > upstream connection reuse
  > measured optimization
```

The target artifacts are:

1. a corrected operation and error-propagation path;
2. asynchronous connect and complete-write support;
3. the bounded incremental HTTP framer;
4. a sequential, streaming `ProxySession`;
5. static routing, worker-local round robin, and upstream reuse;
6. a mock upstream plus unit and integration CTest targets;
7. sanitizer configuration;
8. a reproducible `wrk` benchmark procedure and report template;
9. architecture, supported-protocol, and known-limit documentation.

If the week ends before all artifacts are complete, advanced pool behavior and
optimization are deferred. Operation lifetime, errors, tests, and shutdown are
not traded away to increase feature count.

## 15. Acceptance Criteria

The first deliverable is accepted when:

- all registered CTest tests pass repeatedly;
- ASan and UBSan report no errors in the covered scenarios;
- supported GET and POST traffic completes through two configured upstreams;
- partial reads and writes, keep-alive reuse, limits, timeouts, disconnects,
  and shutdown follow this specification;
- no accepted I/O operation can remain suspended without a completion or
  cancellation path;
- the proxy completes the load matrix without crashes, deadlocks, or permanent
  coroutine suspension;
- shutdown releases descriptors, operation storage, connections, and
  registered buffers;
- the README states supported features and exclusions without describing the
  project as a complete production API gateway;
- the benchmark report is reproducible and includes errors and tail latency,
  not only peak throughput.

## 16. Principal Risks and Decisions

- Correcting operation ownership may touch several existing awaitables. The
  implementation plan must introduce and test the new path in small increments
  instead of rewriting every optional zero-copy feature at once.
- The HTTP subset is intentionally narrow. Unsupported framing is rejected
  explicitly so it cannot silently corrupt a persistent connection.
- A per-worker connection pool can create more total upstream connections as
  worker count increases. Configuration and metrics expose that multiplication.
- A no-retry first version may return more 502 responses during endpoint
  failures, but it preserves request semantics and keeps the first milestone
  reviewable.
- Registered buffers and zero-copy features are optimizations. The baseline
  path must remain correct when registration is unavailable.
