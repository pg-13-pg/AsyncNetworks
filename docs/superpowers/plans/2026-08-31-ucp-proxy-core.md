# UCP Proxy Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a bounded HTTP/1.1 reverse-proxy core that demonstrates safe `io_uring` operation lifetime, C++20 coroutine I/O, asynchronous outbound connect, streaming backpressure, worker-local upstream reuse, and reproducible verification.

**Architecture:** Existing examples keep their legacy `asyncRead`/`asyncSend` path while the proxy uses a new namespaced safe path under `include/ucp`. Each kernel request owns an `IoOperation` retained by its `EventLoop`; a `ProxySession` pins downstream and upstream work to one worker and streams exactly one request at a time through a worker-local `UpstreamPool`.

**Tech Stack:** Linux, C++20 coroutines, liburing, TCP/HTTP/1.1, fmt, CMake/CTest, ASan/UBSan, wrk

**Spec:** `docs/superpowers/specs/2026-08-31-ucp-proxy-core-design.md`

## Global Constraints

- Build only on Linux with a kernel and liburing installation that support the repository's existing `io_uring` setup.
- Require CMake 3.16 or newer so CTest, `CONFIGURE_DEPENDS`, and sanitizer link options have consistent behavior.
- Keep C++20 as the language standard; do not introduce `std::expected` or another C++23 dependency.
- Keep the existing legacy read, write, sendfile, and zero-copy APIs compiling while the proxy is migrated to the new safe API.
- Do not rewrite sendfile, `IORING_OP_SEND_ZC`, or the custom memory pool during this plan.
- Support only HTTP/1.1 GET and POST, explicit `Content-Length`, and one active request per downstream connection.
- Reject transfer encoding, Upgrade, CONNECT, invalid framing, headers over 16 KiB, and bodies over 1 MiB by default.
- Keep every downstream connection, `ProxySession`, upstream connection, pool, and metric shard on one owning `EventLoop`.
- Use a static longest-prefix route table and worker-local round robin; do not add discovery, active health checks, or retries.
- Use bounded memory and queue limits; no accepted operation may suspend without a completion or cancellation path.
- Write the failing test first for every behavioral change, then implement only enough to pass it.
- Commit after every task using only the files named by that task.

## File Structure

New runtime files:

- `include/ucp/runtime/Result.hpp`: move-aware `Result<T>`, normalized `Error`, and `ErrorCode`.
- `include/ucp/runtime/Task.hpp`: lazy composable `Task<T>` plus top-level `DetachedTask`.
- `include/ucp/runtime/CompletionData.hpp`: tagged base for legacy and new CQE user data.
- `include/ucp/runtime/IoOperation.hpp`: exactly-once result selection and multi-CQE drain state.
- `src/ucp/runtime/IoOperation.cpp`: kernel-result normalization and operation state transitions.

New network files:

- `include/ucp/net/AsyncSocket.hpp`: read-some, write-some, and write-all coroutine interfaces.
- `src/ucp/net/AsyncSocket.cpp`: SQE preparation and `TcpConnection` integration.
- `include/ucp/net/AsyncConnect.hpp`: outbound connect awaitable and connection factory.
- `src/ucp/net/AsyncConnect.cpp`: nonblocking socket ownership and connect completion.

New proxy files:

- `include/ucp/proxy/HttpFramer.hpp`: bounded incremental request/response head parser.
- `src/ucp/proxy/HttpFramer.cpp`: supported HTTP subset and hop-by-hop normalization.
- `include/ucp/proxy/GatewayConfig.hpp`: validated immutable gateway and route configuration.
- `src/ucp/proxy/GatewayConfig.cpp`: conversion from the repository's `Config` values.
- `include/ucp/proxy/RouteTable.hpp`: endpoint, route, longest-prefix match, and worker-local round robin.
- `src/ucp/proxy/RouteTable.cpp`: parsing and selection logic.
- `include/ucp/proxy/UpstreamPool.hpp`: endpoint buckets and move-only RAII lease.
- `src/ucp/proxy/UpstreamPool.cpp`: asynchronous acquisition, reuse, capacity, and discard rules.
- `include/ucp/proxy/ProxySession.hpp`: one downstream request/response state machine.
- `src/ucp/proxy/ProxySession.cpp`: bounded streaming and HTTP error mapping.
- `include/ucp/proxy/GatewayMetrics.hpp`: cache-line-separated metric shard and snapshot.
- `include/ucp/proxy/GatewayServer.hpp`: worker contexts and process-level lifecycle.
- `src/ucp/proxy/GatewayServer.cpp`: worker initialization, session creation, drain, and stop.

New application and verification files:

- `gateway/ucp_gateway.cpp`: production-style process entry point and signal waiting.
- `config/gateway.conf`: two-upstream static example configuration.
- `tests/TestSupport.hpp`: dependency-free assertions and bounded wait helper.
- `tests/CMakeLists.txt`: one CTest executable per independently reviewable component.
- `tests/mock_http_upstream.hpp`: deterministic local upstream used by integration tests.
- `tests/*.cpp`: focused unit and integration test executables named in each task.
- `cmake/Sanitizers.cmake`: mutually exclusive ASan/UBSan and TSan flags.
- `bench/run_gateway_bench.sh`: reproducible direct-versus-proxy wrk matrix.
- `docs/tsan-notes.md`: exact ThreadSanitizer commands, results, and justified limitations.
- `docs/gateway-benchmark.md`: environment and result template.

Existing files modified by the plan:

- `CMakeLists.txt`: CTest, recursive `src/ucp` sources, gateway target, sanitizer option, and build-tree output directories.
- `include/IoContext.hpp`: tagged legacy completion data.
- `include/EventLoop.hpp`, `src/EventLoop.cpp`: control queue, pending submissions, operation registry, cancellation, and drain.
- `include/EventLoopThread.hpp`, `src/EventLoopThread.cpp`: initialize registered resources on the owning thread and always join a started worker.
- `include/Socket.hpp`, `src/Socket.cpp`: safe fd release and socket-option error reporting used by outbound connect.
- `include/TcpConnection.hpp`, `src/TcpConnection.cpp`: fd access, new safe awaitables, pending-operation tracking, and cancellation.
- `include/Acceptor.hpp`, `src/Acceptor.cpp`: explicit stop-accepting operation.
- `include/TcpServer.hpp`, `src/TcpServer.cpp`: worker initialization callback and graceful connection drain.
- `README.md`: positioning, supported subset, build/test commands, and known limits.

## One-Week Order

| Day | Tasks | Review gate |
| --- | --- | --- |
| 1 | 1-2 | Typed errors and composable coroutine tests pass |
| 2 | 3-4 | Exactly-once operation and EventLoop queue/submission tests pass |
| 3 | 5-6 | Socketpair read/write and outbound-connect tests pass |
| 4 | 7-8 | HTTP framing, configuration, routing, and selection tests pass |
| 5 | 9-10 | Upstream reuse and end-to-end proxy happy path pass |
| 6 | 11-12 | Shutdown, failure mapping, and sanitizer integration pass |
| 7 | 13 | Benchmark procedure and project documentation are reproducible |

This is an aggressive learning schedule, not a promise that all thirteen tasks
fit every machine and debugging session. Use these cut lines without reordering
work:

- Foundation checkpoint: Tasks 1-6. The safe I/O path is independently useful
  even if the proxy is not yet started.
- Runnable portfolio checkpoint: Tasks 1-11. The proxy has an end-to-end binary
  and documented protocol limits.
- Evidence checkpoint: Tasks 12-13. Failure, sanitizer, and performance claims
  become supportable.

At the end of the week, stop at the last passing commit. Do not skip a failing
test or operation-lifetime task to reach a later feature.

---

### Task 1: CTest Baseline and Typed Results

**Files:**
- Create: `include/ucp/runtime/Result.hpp`
- Create: `tests/TestSupport.hpp`
- Create: `tests/result_test.cpp`
- Create: `tests/CMakeLists.txt`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: C++20, existing `proactor_static` target.
- Produces: `ucp::ErrorCode`, `ucp::Error`, `ucp::Result<T>`, `ucp::IoResult`, `CHECK`, `CHECK_EQ`, and the `add_ucp_test()` CMake helper.

- [ ] **Step 1: Add the test harness and a failing result test**

Raise `cmake_minimum_required` to 3.16. Add `include(CTest)` and `add_subdirectory(tests)` under `if(BUILD_TESTING)` in the root CMake file. Change runtime/library output directories from the source tree to `${CMAKE_BINARY_DIR}/bin` and `${CMAKE_BINARY_DIR}/lib`, and remove the target-specific source-tree output overrides for `test_client` and `recommend_client`. Make the source collection include `src/ucp/*.cpp` recursively with `CONFIGURE_DEPENDS` while continuing to exclude `src/test.cpp` and `src/main.cpp`.

Use this helper in `tests/CMakeLists.txt`:

```cmake
function(add_ucp_test target source)
    add_executable(${target} ${source})
    target_link_libraries(${target} PRIVATE proactor_static pthread)
    target_include_directories(${target} PRIVATE ${PROJECT_SOURCE_DIR}/tests)
    add_test(NAME ${target} COMMAND ${target})
endfunction()

add_ucp_test(ucp_result_test result_test.cpp)
```

Create `tests/TestSupport.hpp` with terminating checks that print file, line, and expression. Create `tests/result_test.cpp`:

```cpp
#include "TestSupport.hpp"
#include "ucp/runtime/Result.hpp"

int main()
{
    auto ok = ucp::Result<std::size_t>::success(7);
    CHECK(ok.hasValue());
    CHECK_EQ(ok.value(), 7U);

    auto eof = ucp::Result<std::size_t>::failure({ucp::ErrorCode::eof, 0});
    CHECK(!eof.hasValue());
    CHECK_EQ(eof.error().code, ucp::ErrorCode::eof);

    auto moved = ucp::Result<std::string>::success("payload");
    CHECK_EQ(std::move(moved).takeValue(), "payload");
    return 0;
}
```

- [ ] **Step 2: Run the test build and verify the missing type fails**

Run:

```bash
cmake -S . -B build-gateway -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON
cmake --build build-gateway --target ucp_result_test -j2
```

Expected: compilation fails because `ucp/runtime/Result.hpp` does not exist.

- [ ] **Step 3: Implement the move-aware result type**

Implement this public surface in `Result.hpp` using `std::variant<T, Error>`:

```cpp
namespace ucp {

enum class ErrorCode {
    none,
    eof,
    cancelled,
    timedOut,
    connectionReset,
    resourceExhausted,
    notConnected,
    protocol,
    system
};

struct Error {
    ErrorCode code{ErrorCode::none};
    int systemError{0};
    std::string message;
};

template <typename T> class Result {
public:
    static Result success(T value);
    static Result failure(Error error);
    bool hasValue() const noexcept;
    explicit operator bool() const noexcept;
    T& value() &;
    const T& value() const&;
    T takeValue() &&;
    const Error& error() const&;
private:
    explicit Result(std::variant<T, Error> value);
    std::variant<T, Error> value_;
};

using IoResult = Result<std::size_t>;

} // namespace ucp
```

Make `value()` and `error()` throw `std::logic_error` on the wrong alternative so tests and callers fail explicitly rather than reading invalid state.

- [ ] **Step 4: Build and run the result test**

Run:

```bash
cmake --build build-gateway --target ucp_result_test -j2
ctest --test-dir build-gateway -R ucp_result_test --output-on-failure
```

Expected: `1/1` test passes.

- [ ] **Step 5: Commit the test baseline and result type**

```bash
git add CMakeLists.txt include/ucp/runtime/Result.hpp tests/CMakeLists.txt tests/TestSupport.hpp tests/result_test.cpp
git commit -m "test: establish typed result baseline"
```

### Task 2: Composable and Detached Coroutine Tasks

**Files:**
- Create: `include/ucp/runtime/Task.hpp`
- Create: `tests/task_test.cpp`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `ucp::Result<T>` only through caller return types; no EventLoop dependency.
- Produces: move-only lazy `ucp::Task<T>`, `ucp::Task<void>`, and eager `ucp::DetachedTask`.

- [ ] **Step 1: Write failing continuation and exception tests**

Register `ucp_task_test`, then create:

```cpp
#include "TestSupport.hpp"
#include "ucp/runtime/Task.hpp"
#include <stdexcept>

ucp::Task<int> valueTask() { co_return 42; }
ucp::Task<int> failingTask()
{
    throw std::runtime_error("task failure");
    co_return 0;
}
ucp::DetachedTask consumeValue(int& value)
{
    value = co_await valueTask();
}
ucp::DetachedTask consumeFailure(bool& caught)
{
    try { (void)co_await failingTask(); }
    catch (const std::runtime_error&) { caught = true; }
}

int main()
{
    int value = 0;
    bool caught = false;
    consumeValue(value);
    consumeFailure(caught);
    CHECK_EQ(value, 42);
    CHECK(caught);
    return 0;
}
```

- [ ] **Step 2: Verify the test fails because the task types are absent**

Run `cmake --build build-gateway --target ucp_task_test -j2`.

Expected: compilation fails on the missing `ucp/runtime/Task.hpp`.

- [ ] **Step 3: Implement coroutine ownership and continuation transfer**

Implement `Task<T>` with `initial_suspend = std::suspend_always`, a stored continuation, a stored `std::exception_ptr`, and `final_suspend` returning a final awaiter that resumes the continuation. The returned `Task` owns and destroys its coroutine handle. `operator co_await() &&` transfers the handle into the awaiter; lvalue awaiting is deleted.

Use this promise contract:

```cpp
struct FinalAwaiter {
    bool await_ready() const noexcept { return false; }
    template <typename Promise>
    std::coroutine_handle<> await_suspend(
        std::coroutine_handle<Promise> handle) const noexcept
    {
        return handle.promise().continuation
            ? handle.promise().continuation
            : std::noop_coroutine();
    }
    void await_resume() const noexcept {}
};
```

Specialize `Task<void>` with `return_void()`. Implement `DetachedTask` with eager start, automatic final destruction, and an `unhandled_exception()` that catches `std::exception` and prints a deterministic diagnostic to `stderr` without terminating the process. Top-level proxy tasks still catch errors at their own session boundary.

- [ ] **Step 4: Run the coroutine tests**

Run:

```bash
cmake --build build-gateway --target ucp_task_test -j2
ctest --test-dir build-gateway -R ucp_task_test --output-on-failure
```

Expected: the value and exception propagation checks pass.

- [ ] **Step 5: Commit the coroutine primitives**

```bash
git add include/ucp/runtime/Task.hpp tests/CMakeLists.txt tests/task_test.cpp
git commit -m "feat: add composable coroutine tasks"
```

### Task 3: Exactly-Once I/O Operation State

**Files:**
- Create: `include/ucp/runtime/CompletionData.hpp`
- Create: `include/ucp/runtime/IoOperation.hpp`
- Create: `src/ucp/runtime/IoOperation.cpp`
- Create: `tests/io_operation_test.cpp`
- Modify: `include/IoContext.hpp`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `ucp::IoResult`.
- Produces: tagged CQE data, `ucp::CompletionKind`, `ucp::IoOperation`, `CompletionDecision`, and exactly-once result selection independent of `EventLoop`.

- [ ] **Step 1: Write failing multi-CQE state tests**

Register `ucp_io_operation_test`. Test these exact sequences:

```cpp
int main()
{
    ucp::IoOperation read(1, ucp::OperationType::read, true);
    read.arm(2);
    auto first = read.onCompletion(ucp::CompletionKind::timeout, -ETIME);
    CHECK(first.resume);
    CHECK(!first.drained);
    CHECK_EQ(read.result().error().code, ucp::ErrorCode::timedOut);
    auto loser = read.onCompletion(ucp::CompletionKind::io, -ECANCELED);
    CHECK(!loser.resume);
    CHECK(loser.drained);

    ucp::IoOperation race(2, ucp::OperationType::read, true);
    race.arm(2);
    CHECK(!race.onCompletion(ucp::CompletionKind::io, -ECANCELED).resume);
    CHECK(race.onCompletion(ucp::CompletionKind::timeout, -ETIME).resume);
    CHECK(race.drained());

    ucp::IoOperation cancel(3, ucp::OperationType::write, false);
    cancel.arm(1);
    CHECK(cancel.requestCancel());
    CHECK(cancel.onCompletion(ucp::CompletionKind::io, -ECANCELED).resume);
    CHECK_EQ(cancel.result().error().code, ucp::ErrorCode::cancelled);
    return 0;
}
```

- [ ] **Step 2: Verify the state test fails before implementation**

Run `cmake --build build-gateway --target ucp_io_operation_test -j2`.

Expected: missing operation headers and types.

- [ ] **Step 3: Add tagged completion data**

Define:

```cpp
namespace ucp {
enum class CompletionDataKind : std::uint32_t { legacyContext, operationToken };
struct CompletionData {
    static constexpr std::uint32_t magicValue = 0x55435031U;
    std::uint32_t magic{magicValue};
    CompletionDataKind kind;
protected:
    explicit CompletionData(CompletionDataKind value) : kind(value) {}
};
}
```

Make existing `IoContext` publicly inherit `ucp::CompletionData` and initialize it with `legacyContext`. This allows `EventLoop` to distinguish old examples from the new path without guessing from pointer layout.

- [ ] **Step 4: Implement `IoOperation` state and result normalization**

Expose:

```cpp
enum class OperationType { read, write, connect, accept, cancel };
enum class CompletionKind { io, timeout, cancel };
struct CompletionDecision { bool resume; bool drained; };

struct CompletionToken : CompletionData {
    IoOperation* operation;
    CompletionKind completionKind;
};

class IoOperation {
public:
    IoOperation(std::uint64_t id, OperationType type, bool linkedTimeout);
    void setContinuation(std::coroutine_handle<> continuation) noexcept;
    void setCompletionCallback(std::function<void(const IoResult&)> callback);
    void arm(unsigned expectedCqes);
    void addExpectedCqe() noexcept;
    bool requestCancel() noexcept;
    CompletionDecision onCompletion(CompletionKind kind, int kernelResult);
    void reject(Error error);
    std::uint64_t id() const noexcept;
    const IoResult& result() const noexcept;
    std::coroutine_handle<> takeContinuation() noexcept;
    std::function<void(const IoResult&)> takeCompletionCallback();
    bool externallyCompleted() const noexcept;
    bool drained() const noexcept;
    CompletionToken& ioToken() noexcept;
    CompletionToken& timeoutToken() noexcept;
    CompletionToken& cancelToken() noexcept;
};
```

Use a mutex-free state machine because all completion and cancellation state transitions occur on the owning `EventLoop`. An operation has either a coroutine continuation or a callback, never both. Treat a linked I/O `-ECANCELED` as undecided until the timeout CQE arrives unless user cancellation was already requested. Decrement the pending CQE count on every completion and set `resume=true` only on the first externally selected result.

- [ ] **Step 5: Build and run operation tests**

Run:

```bash
cmake --build build-gateway --target ucp_io_operation_test -j2
ctest --test-dir build-gateway -R ucp_io_operation_test --output-on-failure
```

Expected: all timeout, cancellation, double-completion, and drain checks pass.

- [ ] **Step 6: Commit the operation state model**

```bash
git add include/IoContext.hpp include/ucp/runtime/CompletionData.hpp include/ucp/runtime/IoOperation.hpp src/ucp/runtime/IoOperation.cpp tests/CMakeLists.txt tests/io_operation_test.cpp
git commit -m "feat: model exactly-once io operations"
```

### Task 4: EventLoop Control Lane and Operation Submission

**Files:**
- Modify: `include/EventLoop.hpp`
- Modify: `src/EventLoop.cpp`
- Modify: `include/EventLoopThread.hpp`
- Modify: `src/EventLoopThread.cpp`
- Modify: `include/TcpServer.hpp`
- Modify: `src/TcpServer.cpp`
- Create: `tests/event_loop_operation_test.cpp`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `CompletionData`, `IoOperation`, existing `LockFreeQueue<Functor>`.
- Produces: result-returning `queueInLoop`, guaranteed `queueControlInLoop`, bounded `submitOperation`, `cancelOperation`, and legacy/new CQE dispatch.

- [ ] **Step 1: Write a failing guaranteed-control test**

Register `ucp_event_loop_operation_test`. In the test, start an `EventLoopThread` with `pendingQueueCapacity=1` and `registeredBuffersCount=0`, queue a blocking normal callback, fill the normal queue until `queueInLoop()` returns false, then call `queueControlInLoop()` with a callback that sets an atomic flag. Release the blocker and wait up to two seconds using `TestSupport::waitUntil`. Assert that the control flag becomes true and the queue-full count is nonzero.

Also submit an `IORING_OP_NOP` through the new operation path and assert that its continuation is selected once and the operation registry returns to zero.

- [ ] **Step 2: Verify the EventLoop test fails on missing APIs**

Run `cmake --build build-gateway --target ucp_event_loop_operation_test -j2`.

Expected: compile errors for the result-returning `queueInLoop`, `queueControlInLoop`, and `submitOperation`.

- [ ] **Step 3: Add the queue and submission interfaces**

Add to `EventLoop`:

```cpp
using OperationPreparer =
    std::function<void(std::span<io_uring_sqe*>, ucp::IoOperation&)>;

enum class SubmitDisposition { submitted, queued, rejected };
struct SubmitResult {
    SubmitDisposition disposition;
    ucp::Error error;
};

bool runInLoop(Functor cb);
bool queueInLoop(Functor cb);
void queueControlInLoop(Functor cb);
bool isInLoopThread() const noexcept;
std::uint64_t nextOperationId() noexcept;
SubmitResult submitOperation(std::shared_ptr<ucp::IoOperation> operation,
                             std::size_t sqeCount,
                             OperationPreparer prepare);
bool cancelOperation(const std::shared_ptr<ucp::IoOperation>& operation);
std::size_t inFlightOperationCount() const noexcept;
```

Add `pendingSubmissionCapacity` to `EventLoop::Options`. Define `registeredBuffersCount=0` as an explicit disabled state instead of normalizing it to one. Make `initRegisteredBuffers()` return `bool`, become idempotent, and clean up partial allocations when kernel registration fails so normal user-buffer I/O remains available. Store normal queue statistics in atomics. Add a mutex-protected `std::deque<Functor> controlFunctors_`, a loop-local bounded deque of pending submissions, an `unordered_map<uint64_t, shared_ptr<IoOperation>>` for in-flight ownership, and a loop-local monotonically increasing operation id counter.

Move worker buffer initialization from `EventLoopThread::startLoop()` into `threadFunc()` before the init callback, pointer publication, and `loop.loop()`. This keeps all ring registration on the owning thread. Make the destructor call `quit()` when a loop exists and join whenever `thread_.joinable()` so a worker that already cleared `loop_` cannot leave a joinable thread object.

- [ ] **Step 4: Implement deterministic dispatch and SQE reservation**

Before calling a preparer, require `io_uring_sq_space_left(&ring_) >= sqeCount`; obtain exactly that many SQEs into a small vector, call the preparer, arm the operation, insert it into the registry, and attach its completion tokens. The SQE convention is fixed: index 0 uses `ioToken()`, index 1, when present, uses `timeoutToken()`, and an explicit cancel request uses a separately allocated SQE with `cancelToken()` after calling `addExpectedCqe()`. If space is unavailable, queue the submission while below `pendingSubmissionCapacity`; otherwise call `operation->reject({resourceExhausted, EAGAIN, "pending submission queue full"})` and return `rejected`.

At each loop iteration:

1. drain control callbacks;
2. drain normal callbacks with the existing fairness cap;
3. flush pending submissions while SQ space permits;
4. submit ready SQEs;
5. process CQEs.

For CQEs, validate `CompletionData::magic`, dispatch `legacyContext` through the current handler, and dispatch `operationToken` through `IoOperation::onCompletion`. Hold a local `shared_ptr` while resuming its continuation or invoking its completion callback. Erase from the registry only when `decision.drained` is true.

- [ ] **Step 5: Move connection destruction to the guaranteed control lane**

Change `TcpServer::removeConnection()` so `connectDestroyed` uses `queueControlInLoop()` rather than the bounded normal queue. Leave ordinary user callbacks on `queueInLoop()` and handle a false result explicitly.

- [ ] **Step 6: Run operation, queue, and legacy smoke tests**

Run:

```bash
cmake --build build-gateway --target ucp_event_loop_operation_test proactor_test -j2
ctest --test-dir build-gateway -R "ucp_(io_operation|event_loop_operation)_test" --output-on-failure
```

Expected: operation tests pass, control callbacks are not lost, the NOP operation drains, and the existing server target still builds.

- [ ] **Step 7: Commit EventLoop ownership and queue semantics**

```bash
git add include/EventLoop.hpp src/EventLoop.cpp include/EventLoopThread.hpp src/EventLoopThread.cpp include/TcpServer.hpp src/TcpServer.cpp tests/CMakeLists.txt tests/event_loop_operation_test.cpp
git commit -m "feat: retain and drain event loop operations"
```

### Task 5: Safe Read, Write, Write-All, and Cancellation

**Files:**
- Create: `include/ucp/net/AsyncSocket.hpp`
- Create: `src/ucp/net/AsyncSocket.cpp`
- Modify: `include/TcpConnection.hpp`
- Modify: `src/TcpConnection.cpp`
- Create: `tests/async_socket_test.cpp`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `EventLoop::submitOperation`, `ucp::Task<T>`, `ucp::IoResult`.
- Produces: `asyncReadSome`, `asyncWriteSome`, `asyncWriteAll`, and `TcpConnection::cancelPendingOperations` for the proxy path.

- [ ] **Step 1: Write failing socketpair round-trip and write-all tests**

Register `ucp_async_socket_test`. Create a nonblocking `socketpair`, wrap one fd in `TcpConnection`, and run a coroutine that:

```cpp
std::array<std::byte, 16> input{};
auto read = co_await ucp::asyncReadSome(conn, input, std::nullopt);
CHECK(read);
CHECK_EQ(read.value(), 5U);

std::vector<std::byte> payload(256 * 1024, std::byte{'x'});
auto write = co_await ucp::asyncWriteAll(conn, payload, std::nullopt);
CHECK(write);
CHECK_EQ(write.value(), payload.size());
```

Use a peer thread to write `hello`, set a small receive buffer, and drain the large response in chunks. Assert the write-all result covers the full payload rather than the first partial write.

Construct this test loop without calling `initRegisteredBuffers()`. This proves the baseline proxy I/O path remains correct when fixed-buffer registration is unavailable.

- [ ] **Step 2: Verify compilation fails on missing safe socket API**

Run `cmake --build build-gateway --target ucp_async_socket_test -j2`.

Expected: missing `AsyncSocket.hpp` and functions.

- [ ] **Step 3: Add connection access and pending-operation tracking**

Add these methods without removing legacy APIs:

```cpp
int fd() const noexcept;
void trackOperation(const std::shared_ptr<ucp::IoOperation>& operation);
void untrackOperation(std::uint64_t operationId);
void cancelPendingOperations();
```

Store tracked operations in a loop-thread-only map keyed by operation id. `forceClose()` and `connectDestroyed()` request cancellation before closing the fd. Actual `IoOperation` memory remains owned by `EventLoop` until drained.

- [ ] **Step 4: Implement read-some and write-some awaitables**

Expose:

```cpp
using Deadline = std::optional<std::chrono::steady_clock::time_point>;

ReadSomeAwaitable asyncReadSome(
    std::shared_ptr<TcpConnection> connection,
    std::span<std::byte> buffer,
    Deadline deadline);

WriteSomeAwaitable asyncWriteSome(
    std::shared_ptr<TcpConnection> connection,
    std::span<const std::byte> buffer,
    Deadline deadline);

Task<IoResult> asyncWriteAll(
    std::shared_ptr<TcpConnection> connection,
    std::span<const std::byte> buffer,
    Deadline deadline);
```

Each awaitable creates one `IoOperation`, sets the coroutine continuation, and prepares an I/O SQE plus linked timeout SQE when a deadline exists. `await_suspend` returns `false` on rejected submission after storing the rejection in the operation. `await_resume` untracks and returns the typed result.

Convert an absolute deadline to a relative `__kernel_timespec` immediately before submission. If the deadline has already expired, return `timedOut` without obtaining an SQE or suspending. Store the timespec inside the awaitable/operation so its address remains valid through the timeout CQE.

Implement `asyncWriteAll` as a loop over `asyncWriteSome`, advancing the span by the exact successful byte count and returning the first terminal error. Treat a successful zero-byte write as `connectionReset` to prevent a busy loop.

- [ ] **Step 5: Run socket and legacy build checks**

Run:

```bash
cmake --build build-gateway --target ucp_async_socket_test proactor_test recommend_server -j2
ctest --test-dir build-gateway -R ucp_async_socket_test --output-on-failure
```

Expected: socketpair read/write passes, the large write completes fully, and old examples still compile.

- [ ] **Step 6: Commit the safe established-socket path**

```bash
git add include/ucp/net/AsyncSocket.hpp src/ucp/net/AsyncSocket.cpp include/TcpConnection.hpp src/TcpConnection.cpp tests/CMakeLists.txt tests/async_socket_test.cpp
git commit -m "feat: add safe coroutine socket io"
```

### Task 6: Asynchronous Outbound Connect

**Files:**
- Create: `include/ucp/net/AsyncConnect.hpp`
- Create: `src/ucp/net/AsyncConnect.cpp`
- Modify: `include/Socket.hpp`
- Modify: `src/Socket.cpp`
- Create: `tests/async_connect_test.cpp`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: operation submission, `TcpConnection`, `InetAddress`, typed tasks.
- Produces: `ucp::asyncConnect()` returning `Task<Result<shared_ptr<TcpConnection>>>`.

- [ ] **Step 1: Write failing connect success and refusal tests**

Register `ucp_async_connect_test`. Start a local nonblocking listener on port zero, obtain its assigned port, and accept in a helper thread. In the loop coroutine:

```cpp
auto connected = co_await ucp::asyncConnect(
    loop, InetAddress(port, "127.0.0.1"),
    std::chrono::steady_clock::now() + 500ms,
    "connect-test");
CHECK(connected);
CHECK(connected.value()->isConnected());
```

Then close the listener and connect to its released port. Assert the result is a failure with `system` or `connectionReset`, never a permanent suspension.

- [ ] **Step 2: Verify the connect test fails on the missing API**

Run `cmake --build build-gateway --target ucp_async_connect_test -j2`.

Expected: missing connect header and function.

- [ ] **Step 3: Add explicit fd transfer to `Socket`**

Add:

```cpp
int releaseFd() noexcept
{
    return std::exchange(sockfd_, -1);
}
```

Make socket-option setters report a boolean result and log `errno` on failure. Existing callers may ignore the return value.

- [ ] **Step 4: Implement outbound connect ownership**

Expose:

```cpp
Task<Result<std::shared_ptr<TcpConnection>>> asyncConnect(
    EventLoop& loop,
    const InetAddress& peer,
    std::chrono::steady_clock::time_point deadline,
    std::string connectionName);
```

Create `AF_INET | SOCK_NONBLOCK | SOCK_CLOEXEC`, set `TCP_NODELAY` and keepalive, and submit `IORING_OP_CONNECT` with a linked timeout. Keep the fd in an RAII `Socket` until the operation succeeds. On success, transfer it with `releaseFd()`, construct `TcpConnection`, and call `connectEstablished()` on the owning loop. On any failure, let `Socket` close it exactly once.

- [ ] **Step 5: Run connect tests and fd-leak repetition**

Run:

```bash
cmake --build build-gateway --target ucp_async_connect_test -j2
ctest --test-dir build-gateway -R ucp_async_connect_test --repeat until-fail:20 --output-on-failure
```

Expected: success/refusal cases complete and 20 repetitions do not hang.

- [ ] **Step 6: Commit outbound connection support**

```bash
git add include/ucp/net/AsyncConnect.hpp src/ucp/net/AsyncConnect.cpp include/Socket.hpp src/Socket.cpp tests/CMakeLists.txt tests/async_connect_test.cpp
git commit -m "feat: add asynchronous outbound connect"
```

### Task 7: Bounded Incremental HTTP Framing

**Files:**
- Create: `include/ucp/proxy/HttpFramer.hpp`
- Create: `src/ucp/proxy/HttpFramer.cpp`
- Create: `tests/http_framer_test.cpp`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: existing `Buffer`, `ucp::Result<T>`.
- Produces: `HttpRequestHead`, `HttpResponseHead`, `HttpLimits`, and header-only incremental parsing that consumes exactly the header block.

- [ ] **Step 1: Write table-driven failing framing tests**

Register `ucp_http_framer_test`. Build one valid POST head and append it one byte at a time. Before the final byte, assert `ParseStatus::needMore`; after the final byte, assert:

```cpp
CHECK_EQ(result.status, ucp::proxy::ParseStatus::complete);
CHECK_EQ(result.request.method, "POST");
CHECK_EQ(result.request.path, "/api/items");
CHECK_EQ(result.request.contentLength, 5U);
CHECK(result.request.keepAlive);
```

Add cases for lower-case header names, duplicate identical content length, conflicting content length, HTTP/1.0 rejection, 16 KiB boundary, 16 KiB plus one, transfer encoding, Upgrade, CONNECT, invalid response status, 204 with zero body, and retained body bytes after header consumption.

- [ ] **Step 2: Verify tests fail on missing framer types**

Run `cmake --build build-gateway --target ucp_http_framer_test -j2`.

Expected: missing header and parser types.

- [ ] **Step 3: Implement the explicit protocol surface**

Define:

```cpp
struct HttpLimits { std::size_t maxHeaderBytes{16 * 1024};
                    std::size_t maxBodyBytes{1024 * 1024}; };
enum class ParseStatus { needMore, complete, error };
enum class HttpParseError { none, badSyntax, headerTooLarge, bodyTooLarge,
                            unsupportedMethod, unsupportedFraming };
struct HttpRequestHead {
    std::string method, path, version, forwardHead;
    std::size_t contentLength{0};
    bool keepAlive{true};
};
struct HttpResponseHead {
    int statusCode{0};
    std::string version, forwardHead;
    std::size_t contentLength{0};
    bool keepAlive{true};
};
```

Provide `parseRequestHead(Buffer&, const HttpLimits&)` and `parseResponseHead(Buffer&, const HttpLimits&)`. Scan CRLF lines without copying the body. Parse header names case-insensitively, reject invalid decimal lengths and overflow, remove `Connection` and `Proxy-Connection` from `forwardHead`, and append one normalized `Connection` header. Consume only the header bytes; leave body and later bytes in the input buffer.

- [ ] **Step 4: Run the full framer table**

Run:

```bash
cmake --build build-gateway --target ucp_http_framer_test -j2
ctest --test-dir build-gateway -R ucp_http_framer_test --output-on-failure
```

Expected: all split points, limits, framing rejections, and body-retention cases pass.

- [ ] **Step 5: Commit the bounded HTTP subset**

```bash
git add include/ucp/proxy/HttpFramer.hpp src/ucp/proxy/HttpFramer.cpp tests/CMakeLists.txt tests/http_framer_test.cpp
git commit -m "feat: add bounded http framing"
```

### Task 8: Gateway Configuration, Routes, and Worker-Local Selection

**Files:**
- Create: `include/ucp/proxy/GatewayConfig.hpp`
- Create: `src/ucp/proxy/GatewayConfig.cpp`
- Create: `include/ucp/proxy/RouteTable.hpp`
- Create: `src/ucp/proxy/RouteTable.cpp`
- Create: `tests/route_table_test.cpp`
- Create: `config/gateway.conf`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: existing `Config::all()`, `ucp::Result<T>`, `InetAddress` construction.
- Produces: validated `GatewayConfig`, `Endpoint`, `Route`, `RouteTable::match`, and `RoundRobinBalancer::select`.

- [ ] **Step 1: Write failing configuration and matching tests**

Register `ucp_route_table_test`. Load this in-memory config:

```ini
[gateway]
listen_ip=127.0.0.1
listen_port=8080
workers=4
header_limit=16384
body_limit=1048576

[event_loop]
ring_entries=4096
sqpoll=false
registered_buffers_count=0
registered_buffer_size=4096
pending_queue_capacity=65536

[route.api]
prefix=/api/
upstreams=127.0.0.1:9001,127.0.0.1:9002
connect_timeout_ms=500
response_timeout_ms=3000
max_connections_per_worker=4
max_idle_per_worker=2
idle_timeout_ms=30000

[route.users]
prefix=/api/users/
upstreams=127.0.0.1:9100
```

Assert `/api/users/7` selects the longer users prefix, `/api/items` selects api, `/other` has no match, and four selections on api produce ports `9001, 9002, 9001, 9002`. Add invalid endpoint, zero port, empty prefix, no upstream, and `max_idle > max_connections` failure cases.

- [ ] **Step 2: Verify route tests fail before types exist**

Run `cmake --build build-gateway --target ucp_route_table_test -j2`.

Expected: missing gateway config and route table headers.

- [ ] **Step 3: Implement validated immutable configuration**

Define:

```cpp
struct Endpoint {
    std::string host;
    std::uint16_t port;
    std::string key() const;
    InetAddress address() const;
};

struct Route {
    std::string name;
    std::string prefix;
    std::vector<Endpoint> upstreams;
    std::chrono::milliseconds connectTimeout{500};
    std::chrono::milliseconds responseTimeout{3000};
    std::size_t maxConnectionsPerWorker{128};
    std::size_t maxIdlePerWorker{32};
    std::chrono::milliseconds idleTimeout{30000};
};

struct GatewayConfig {
    std::string listenIp;
    std::uint16_t listenPort;
    int workerCount{4};
    EventLoop::Options eventLoopOptions;
    HttpLimits httpLimits;
    std::chrono::milliseconds gracefulShutdown{5000};
    std::vector<Route> routes;
    static Result<GatewayConfig> from(const Config& config);
};
```

Discover route names by scanning `Config::all()` keys beginning with `route.` and ending in `.prefix`. Reject all invalid values with `ErrorCode::protocol` and a stable diagnostic string stored in the config parse result or logged by the caller.

- [ ] **Step 4: Implement longest-prefix and round robin**

Sort route pointers by descending prefix length once in the `RouteTable` constructor. `match()` returns the first prefix match. Store one `std::size_t` cursor per route in `RoundRobinBalancer`; the object is owned by one worker and uses no atomics.

- [ ] **Step 5: Add the concrete example config and run tests**

Create `config/gateway.conf` with the approved defaults, four workers, SQPOLL disabled, registered buffers disabled for the correctness baseline, and upstreams `127.0.0.1:9001,127.0.0.1:9002`. The benchmark report may add a second measured configuration with registered buffers enabled only after the baseline passes.

Run:

```bash
cmake --build build-gateway --target ucp_route_table_test -j2
ctest --test-dir build-gateway -R ucp_route_table_test --output-on-failure
```

Expected: valid routes parse and matching/round-robin order is deterministic; invalid configurations fail.

- [ ] **Step 6: Commit route configuration and selection**

```bash
git add include/ucp/proxy/GatewayConfig.hpp src/ucp/proxy/GatewayConfig.cpp include/ucp/proxy/RouteTable.hpp src/ucp/proxy/RouteTable.cpp tests/CMakeLists.txt tests/route_table_test.cpp config/gateway.conf
git commit -m "feat: add static gateway routing"
```

### Task 9: Worker-Local Upstream Pool and RAII Lease

**Files:**
- Create: `include/ucp/proxy/UpstreamPool.hpp`
- Create: `src/ucp/proxy/UpstreamPool.cpp`
- Create: `tests/upstream_pool_test.cpp`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `asyncConnect`, `Endpoint`, `Route`, owning `EventLoop`.
- Produces: move-only `UpstreamLease`, `UpstreamPool::acquire`, idle reuse, active/idle counts, and deterministic discard.

- [ ] **Step 1: Write failing reuse, capacity, and discard tests**

Register `ucp_upstream_pool_test`. Start a local listener that counts accepts and serves two fixed `Content-Length` responses on one keep-alive connection. In a loop coroutine:

1. acquire a lease and record its connection pointer;
2. mark it reusable and release it;
3. acquire again and assert pointer equality and one server accept;
4. hold leases up to `maxConnectionsPerWorker` and assert the next acquire returns `resourceExhausted`;
5. release one lease without marking it reusable and assert the next acquire creates a different connection.

- [ ] **Step 2: Verify pool tests fail on missing types**

Run `cmake --build build-gateway --target ucp_upstream_pool_test -j2`.

Expected: missing `UpstreamPool` and `UpstreamLease`.

- [ ] **Step 3: Implement the move-only lease contract**

Expose:

```cpp
class UpstreamLease {
public:
    UpstreamLease(UpstreamLease&&) noexcept;
    UpstreamLease& operator=(UpstreamLease&&) noexcept;
    ~UpstreamLease();
    std::shared_ptr<TcpConnection> connection() const;
    void markReusable() noexcept;
private:
    friend class UpstreamPool;
    UpstreamPool* pool_{nullptr};
    std::string bucketKey_;
    Endpoint endpoint_;
    std::shared_ptr<TcpConnection> connection_;
    bool reusable_{false};
};
```

The destructor calls a loop-thread-only pool release function. A lease not explicitly marked reusable discards its connection.

- [ ] **Step 4: Implement endpoint buckets and acquisition**

Expose:

```cpp
class UpstreamPool {
public:
    explicit UpstreamPool(EventLoop& loop);
    Task<Result<UpstreamLease>> acquire(
        const Route& route,
        const Endpoint& endpoint,
        std::chrono::steady_clock::time_point deadline);
    void closeIdle();
    std::size_t active(const Route&, const Endpoint&) const;
    std::size_t idle(const Route&, const Endpoint&) const;
};
```

Each bucket is keyed by `route.name + endpoint.key()` so routes with different capacity policies cannot accidentally share accounting. It stores `totalCount`, `leasedCount`, and a deque of `{connection, idleSince}`. Pop expired or disconnected entries before reuse and decrement `totalCount` for every discarded entry. Create a new connection only when `totalCount < maxConnectionsPerWorker`; increment both counts only after connect succeeds. Reusing idle increments `leasedCount`; releasing always decrements `leasedCount`. Retain only connected reusable entries below the idle limit; otherwise decrement `totalCount`, cancel, and close them. `active()` returns `leasedCount` and `idle()` returns deque size.

- [ ] **Step 5: Run pool tests repeatedly**

Run:

```bash
cmake --build build-gateway --target ucp_upstream_pool_test -j2
ctest --test-dir build-gateway -R ucp_upstream_pool_test --repeat until-fail:20 --output-on-failure
```

Expected: reuse, capacity, discard, and accounting remain deterministic for 20 runs.

- [ ] **Step 6: Commit worker-local connection reuse**

```bash
git add include/ucp/proxy/UpstreamPool.hpp src/ucp/proxy/UpstreamPool.cpp tests/CMakeLists.txt tests/upstream_pool_test.cpp
git commit -m "feat: add worker-local upstream pool"
```

### Task 10: Streaming ProxySession Vertical Slice

**Files:**
- Create: `include/ucp/proxy/ProxySession.hpp`
- Create: `src/ucp/proxy/ProxySession.cpp`
- Create: `tests/mock_http_upstream.hpp`
- Create: `tests/proxy_session_test.cpp`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: safe socket I/O, HTTP framer, route table, round robin, upstream pool.
- Produces: `ProxySession::run`, bounded header/body streaming, and pre-response HTTP error mapping.

- [ ] **Step 1: Write a failing end-to-end happy-path test**

Register `ucp_proxy_session_test`. `mock_http_upstream.hpp` must provide a local listener that records raw request heads/bodies and can fragment a response according to a supplied vector of chunk sizes.

Test through a downstream socketpair:

```text
POST /api/items HTTP/1.1\r\n
Host: example\r\n
Content-Length: 5\r\n
Connection: keep-alive\r\n
\r\n
hello
```

Fragment the request at every header byte boundary in separate test iterations. Assert the upstream receives exactly one normalized request and body `hello`; return a fragmented `200` response with `Content-Length: 5`; assert the client receives body `world` and the session remains available for a second request.

- [ ] **Step 2: Verify the session test fails on missing class**

Run `cmake --build build-gateway --target ucp_proxy_session_test -j2`.

Expected: missing `ProxySession`.

- [ ] **Step 3: Implement bounded read and stream helpers**

Use these private coroutine helpers:

```cpp
Task<Result<HttpRequestHead>> readRequestHead();
Task<Result<HttpResponseHead>> readResponseHead(
    const std::shared_ptr<TcpConnection>& upstream);
Task<IoResult> streamExact(
    const std::shared_ptr<TcpConnection>& source,
    Buffer& sourceBuffer,
    const std::shared_ptr<TcpConnection>& destination,
    std::size_t bytes,
    Deadline deadline);
Task<IoResult> sendError(int status, std::string_view reason);
```

`streamExact` first consumes already buffered bytes, writes each bounded chunk completely, and does not issue another source read until the destination write completes. Use a 16 KiB session scratch buffer. Return EOF before the promised content length as `protocol`.

- [ ] **Step 4: Implement the sequential request state machine**

Expose:

```cpp
class ProxySession : public std::enable_shared_from_this<ProxySession> {
public:
    using FinishCallback =
        std::function<void(const std::shared_ptr<ProxySession>&)>;
    ProxySession(std::shared_ptr<TcpConnection> downstream,
                 const RouteTable& routes,
                 RoundRobinBalancer& balancer,
                 UpstreamPool& pool,
                 const HttpLimits& limits,
                 FinishCallback onFinished);
    DetachedTask run();
    void cancel();
};
```

At the start of `run()`, retain `auto self = shared_from_this()` through coroutine exit. The finish callback erases the session from its worker's active-session map; `self` prevents destruction while the final callback is still executing.

For each request: parse and validate the head, find a route, select an endpoint, call `pool.acquire(route, endpoint, deadline)`, write the normalized request head, stream the request body, parse/write the response head, stream the response body, and mark the lease reusable only when both legs remain keep-alive and parsing completed cleanly. Read the next request only after the response completes.

Map parser errors to 400/413/431, missing route to 404, pool exhaustion to 503, connect/protocol failure to 502, and deadline to 504. Send an error only before response bytes have started; otherwise force-close downstream.

- [ ] **Step 5: Run the session happy path and legacy build**

Run:

```bash
cmake --build build-gateway --target ucp_proxy_session_test proactor_test -j2
ctest --test-dir build-gateway -R ucp_proxy_session_test --output-on-failure
```

Expected: fragmented GET/POST and two sequential keep-alive requests pass through one upstream connection.

- [ ] **Step 6: Commit the proxy vertical slice**

```bash
git add include/ucp/proxy/ProxySession.hpp src/ucp/proxy/ProxySession.cpp tests/CMakeLists.txt tests/mock_http_upstream.hpp tests/proxy_session_test.cpp
git commit -m "feat: stream http requests through proxy sessions"
```

### Task 11: GatewayServer, Metrics, and Graceful Stop

**Files:**
- Create: `include/ucp/proxy/GatewayMetrics.hpp`
- Create: `include/ucp/proxy/GatewayServer.hpp`
- Create: `src/ucp/proxy/GatewayServer.cpp`
- Create: `gateway/ucp_gateway.cpp`
- Modify: `include/Acceptor.hpp`
- Modify: `src/Acceptor.cpp`
- Modify: `include/TcpServer.hpp`
- Modify: `src/TcpServer.cpp`
- Modify: `CMakeLists.txt`
- Create: `tests/gateway_shutdown_test.cpp`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `ProxySession`, gateway config, per-worker pool.
- Produces: runnable `ucp_gateway`, worker metric shards, stop-accept/drain/cancel lifecycle, and stable snapshots.

- [ ] **Step 1: Write a failing shutdown-order test**

Register `ucp_gateway_shutdown_test`. Start a gateway with one worker and a mock upstream that delays its response. Send one active request, call `stop(500ms)`, and assert:

- new connections are refused after stop begins;
- the active request is allowed to complete before 500 ms;
- a second run with a response delayed beyond 500 ms closes the client and cancels the upstream;
- `activeSessionCount()==0` and every worker pool reports zero active/idle connections after stop;
- the process fd count returns to its pre-test value within a small allowance for the test harness.

- [ ] **Step 2: Verify shutdown test fails on missing server APIs**

Run `cmake --build build-gateway --target ucp_gateway_shutdown_test -j2`.

Expected: missing `GatewayServer`, `Acceptor::stop`, and server drain methods.

- [ ] **Step 3: Add stop-accepting and server lifecycle hooks**

Add `Acceptor::stop()` that changes listening state and cancels/closes the listen operation through the owning loop. Replace its embedded accept `IoContext` for the gateway path with an `IoOperation` completion callback submitted through `EventLoop::submitOperation`; SQ exhaustion must queue or reject with an explicit retry through the control lane rather than silently ending acceptance. Add to `TcpServer`:

```cpp
void setThreadInitCallback(EventLoopThreadPool::ThreadInitCallback cb);
void stopAccepting();
void forEachConnection(
    const std::function<void(const std::shared_ptr<TcpConnection>&)>& cb);
std::size_t connectionCount() const;
```

Store the thread-init callback before `start()` and pass it to `threadPool_.start()`. Keep connection-map mutations on the base loop.

- [ ] **Step 4: Implement worker contexts and metric shards**

Define a `GatewayWorker` owned by `GatewayServer` for each `EventLoop`, containing `RoundRobinBalancer`, one `UpstreamPool` whose buckets are route-scoped, active sessions, and an `alignas(64) GatewayMetricShard` with atomic counters. Counters include connections, requests, status classes, bytes, connect/protocol/timeout/cancel/overload errors, normal-queue rejection/high-water events, and pool acquisitions/reuses. Add fixed request-latency buckets at 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, and greater than 1000 milliseconds so snapshots can derive P50/P95/P99 without allocating on the hot path.

`GatewayMetricsSnapshot` is a plain value struct. Aggregate by reading each shard outside the hot path; never share pool or route cursor state across workers.

Have the application emit one text snapshot every five seconds and one final snapshot during shutdown. The benchmark script reads connection, error, timeout, byte, queue, latency-bucket, and pool-reuse values from these stable log keys; no Prometheus endpoint is added.

- [ ] **Step 5: Implement `GatewayServer` drain and runnable main**

Expose:

```cpp
class GatewayServer {
public:
    GatewayServer(EventLoop& baseLoop, GatewayConfig config);
    void start();
    void stop(std::chrono::milliseconds gracePeriod);
    GatewayMetricsSnapshot metrics() const;
    std::size_t activeSessionCount() const;
};
```

`stop()` is callable from the signal-wait thread but posts its state transition through the base loop's guaranteed control lane. It stops accepts, prevents new sessions/acquisitions, waits for sessions until the deadline, calls `ProxySession::cancel()` on the remainder, closes idle pools, and quits worker/base loops only after operation registries drain. A condition variable releases the signal-wait thread only after the base loop reports the stopped state; connection-map iteration never occurs directly from the signal thread.

In `gateway/ucp_gateway.cpp`, load and validate `config/gateway.conf`, construct the base `EventLoop` with `GatewayConfig::eventLoopOptions`, initialize logging, block `SIGINT` and `SIGTERM` before starting workers, and use one `sigwait` thread to call `GatewayServer::stop`. `GatewayServer` uses `GatewayConfig::workerCount` for its pool. Add a `ucp_gateway` executable linked to `proactor_static`.

- [ ] **Step 6: Run shutdown and executable smoke tests**

Run:

```bash
cmake --build build-gateway --target ucp_gateway ucp_gateway_shutdown_test -j2
ctest --test-dir build-gateway -R ucp_gateway_shutdown_test --output-on-failure
timeout 2s build-gateway/bin/ucp_gateway config/gateway.conf
```

Expected: shutdown tests pass; the smoke command starts the gateway and `timeout` terminates it through the configured signal path without a crash.

- [ ] **Step 7: Commit the runnable gateway lifecycle**

```bash
git add include/ucp/proxy/GatewayMetrics.hpp include/ucp/proxy/GatewayServer.hpp src/ucp/proxy/GatewayServer.cpp gateway/ucp_gateway.cpp include/Acceptor.hpp src/Acceptor.cpp include/TcpServer.hpp src/TcpServer.cpp CMakeLists.txt tests/CMakeLists.txt tests/gateway_shutdown_test.cpp
git commit -m "feat: run and drain the proxy gateway"
```

### Task 12: Failure Matrix, Sanitizers, and Full Verification

**Files:**
- Create: `cmake/Sanitizers.cmake`
- Create: `docs/tsan-notes.md`
- Create: `tests/gateway_failure_test.cpp`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`
- Modify: `tests/mock_http_upstream.hpp`

**Interfaces:**
- Consumes: complete gateway process and mock upstream controls.
- Produces: executable error-mapping/lifecycle regression matrix plus separate ASan/UBSan and TSan builds.

- [ ] **Step 1: Write failing error-mapping scenarios**

Register `ucp_gateway_failure_test`. Add one isolated test case for each result:

| Input/fault | Expected client result |
| --- | --- |
| malformed start line | 400 |
| 16 KiB plus one header | 431 |
| 1 MiB plus one content length | 413 |
| unmatched path | 404 |
| exhausted pool | 503 |
| refused upstream connect | 502 |
| malformed upstream status/header | 502 |
| upstream response beyond deadline | 504 |
| upstream reset after partial response | downstream EOF, no second status line |
| downstream close during upload | upstream operation cancelled and pool entry discarded |

Count response status lines to prove partial-response failures do not append a second HTTP response.

- [ ] **Step 2: Run the failure test and record the first failing scenario**

Run:

```bash
cmake --build build-gateway --target ucp_gateway_failure_test -j2
ctest --test-dir build-gateway -R ucp_gateway_failure_test --output-on-failure
```

Expected before fixes: at least one scenario fails with its expected/actual status diagnostic.

- [ ] **Step 3: Fix each failure at its owning boundary**

Apply these ownership rules while making the table pass:

- framing and limit errors stay in `HttpFramer`/`ProxySession`;
- connection and timeout errors stay typed in async network code;
- lease discard stays in `UpstreamPool`;
- second-response prevention stays in `ProxySession`'s `responseStarted_` state;
- CQE drain and cancellation stay in `EventLoop`/`IoOperation`.

After each row, run `ctest --test-dir build-gateway -R ucp_gateway_failure_test --output-on-failure` and do not proceed until that row passes.

- [ ] **Step 4: Add opt-in ASan/UBSan configuration**

Create `cmake/Sanitizers.cmake`:

```cmake
option(UCP_ENABLE_SANITIZERS "Enable AddressSanitizer and UBSan" OFF)
option(UCP_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
if(UCP_ENABLE_SANITIZERS AND UCP_ENABLE_TSAN)
    message(FATAL_ERROR "ASan/UBSan and TSan must use separate build trees")
endif()
if(UCP_ENABLE_SANITIZERS AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
endif()
if(UCP_ENABLE_TSAN AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
    add_link_options(-fsanitize=thread)
endif()
```

Include it after `project()` in the root CMake file.

- [ ] **Step 5: Run normal and sanitizer suites**

Run:

```bash
cmake --build build-gateway -j2
ctest --test-dir build-gateway --output-on-failure
cmake -S . -B build-gateway-asan -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DUCP_ENABLE_SANITIZERS=ON
cmake --build build-gateway-asan -j2
ctest --test-dir build-gateway-asan --output-on-failure
```

Expected: all registered tests pass in both builds with no ASan or UBSan report.

- [ ] **Step 6: Run TSan separately and preserve exact evidence**

Run:

```bash
cmake -S . -B build-gateway-tsan -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DUCP_ENABLE_TSAN=ON
cmake --build build-gateway-tsan -j2
ctest --test-dir build-gateway-tsan --output-on-failure
```

Expected: ordinary concurrency tests report no data race. Record the command, compiler, result, and any exact stack in `docs/tsan-notes.md`. If TSan reports a kernel/liburing interaction rather than a user-space race, document the evidence and suppression rationale there; do not describe that run as clean.

- [ ] **Step 7: Commit failure coverage and sanitizer support**

```bash
git add cmake/Sanitizers.cmake docs/tsan-notes.md CMakeLists.txt tests/CMakeLists.txt tests/mock_http_upstream.hpp tests/gateway_failure_test.cpp
git commit -m "test: cover gateway failures and sanitizers"
```

### Task 13: Reproducible Benchmark and Project Handoff

**Files:**
- Create: `bench/mock_upstream.cpp`
- Create: `bench/run_gateway_bench.sh`
- Create: `docs/gateway-benchmark.md`
- Modify: `CMakeLists.txt`
- Modify: `README.md`

**Interfaces:**
- Consumes: `ucp_gateway`, `gateway_mock_upstream`, installed `/usr/bin/wrk`.
- Produces: repeatable direct-versus-proxy load matrix, environment capture, honest result template, and resume-ready documentation.

- [ ] **Step 1: Add a deterministic benchmark upstream executable**

Create `bench/mock_upstream.cpp` as a small `TcpServer` application using the safe socket path. It accepts `--port`, parses only `GET /bytes/<N>`, validates `N` as one of `1024`, `4096`, or `16384`, and returns an `N`-byte body with `Content-Length` and keep-alive. Add `gateway_mock_upstream` to CMake. Two processes on ports 9001 and 9002 provide the configured upstream set; direct baseline requests use port 9001.

Build and smoke-test it:

```bash
cmake --build build-gateway --target gateway_mock_upstream -j2
timeout 2s build-gateway/bin/gateway_mock_upstream --port 9001
```

Expected: the process starts, listens on 9001, and exits through the timeout signal without a crash.

- [ ] **Step 2: Write the benchmark script with fixed inputs**

The script must use `set -euo pipefail`, accept `--gateway`, `--direct`, `--output`, `--threads`, and optional `--soak`, create a timestamped output directory, and record:

```bash
uname -a
cmake --version
c++ --version
lscpu
ulimit -n
```

For payload sizes `1024`, `4096`, and `16384`, use `/bytes/<size>` on the direct base URL and `/api/bytes/<size>` on the proxy base URL. For each URL, run:

```bash
wrk --latency -t "${threads}" -c 64   -d 10s "${url}"  # warm-up
wrk --latency -t "${threads}" -c 64   -d 30s "${url}"
wrk --latency -t "${threads}" -c 256  -d 30s "${url}"
wrk --latency -t "${threads}" -c 1024 -d 30s "${url}"
```

Store stdout and stderr for every run. Exit nonzero if wrk reports socket or non-2xx/3xx errors. Do not calculate a marketing percentage when either run contains errors.

When `--soak` is supplied, run `wrk --latency -t "${threads}" -c 256 -d 30m` against `/api/bytes/4096` and store gateway metrics, RSS, and fd count before and after the run.

- [ ] **Step 3: Dry-run and shell-check the script**

Run:

```bash
bash -n bench/run_gateway_bench.sh
bench/run_gateway_bench.sh --help
```

Expected: syntax check succeeds and help documents all four required options without starting a load test.

- [ ] **Step 4: Add the benchmark report template**

`docs/gateway-benchmark.md` must contain tables for 1 KiB, 4 KiB, and 16 KiB responses with direct/proxy rows and columns for RPS, P50, P95, P99, errors, CPU, RSS, fd peak, and pool reuse rate. Include exact build, startup, warm-up, measured-run, and 30-minute soak commands. Leave result cells as `Not measured` until a command has produced evidence; this is data state, not an implementation placeholder.

- [ ] **Step 5: Rewrite the README project entry points**

Document:

- the project positioning as a learning-oriented proxy data-plane core;
- the shared-nothing worker architecture and exactly-once operation lifetime;
- supported and rejected HTTP behavior;
- build, CTest, sanitizer, gateway startup, and wrk commands;
- a link to the design, this implementation plan, TSan notes, and benchmark report;
- known limits: no TLS, chunked transfer, discovery, retry, active health check, or complete production HTTP compliance.

- [ ] **Step 6: Run final documentation and repository checks**

Run:

```bash
git diff --check
cmake --build build-gateway -j2
ctest --test-dir build-gateway --output-on-failure
bash -n bench/run_gateway_bench.sh
git status --short
```

Expected: no whitespace errors, build succeeds, all tests pass, benchmark script parses, and status contains only the Task 13 documentation/script changes.

- [ ] **Step 7: Commit the benchmark and handoff documentation**

```bash
git add bench/mock_upstream.cpp bench/run_gateway_bench.sh docs/gateway-benchmark.md CMakeLists.txt README.md
git commit -m "docs: add proxy benchmark and handoff"
```

## Final Verification Gate

After Task 13, run these commands from a clean worktree:

```bash
cmake -S . -B build-gateway-release -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build-gateway-release -j2
ctest --test-dir build-gateway-release --output-on-failure
cmake -S . -B build-gateway-final-asan -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DUCP_ENABLE_SANITIZERS=ON
cmake --build build-gateway-final-asan -j2
ctest --test-dir build-gateway-final-asan --output-on-failure
cmake -S . -B build-gateway-final-tsan -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DUCP_ENABLE_TSAN=ON
cmake --build build-gateway-final-tsan -j2
ctest --test-dir build-gateway-final-tsan --output-on-failure
bash -n bench/run_gateway_bench.sh
git status --short
```

Do not claim completion unless the release and ASan/UBSan CTest runs have zero failures, ASan/UBSan output contains no report, the TSan result is reported exactly as observed, the script syntax check succeeds, and the worktree contains no unexpected files.
