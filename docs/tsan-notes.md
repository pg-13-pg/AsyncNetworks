# ThreadSanitizer Verification Notes

Date: 2026-09-02

## Environment

- Kernel: `Linux 6.18.33.2-microsoft-standard-WSL2 x86_64`
- Compiler: `g++ 13.3.0 (Ubuntu 13.3.0-6ubuntu2~24.04.1)`
- CMake: `3.28.3`
- liburing: `2.5`

## Commands

```bash
cmake -S . -B build-gateway-tsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTING=ON \
  -DUCP_ENABLE_TSAN=ON
cmake --build build-gateway-tsan -j2
ctest --test-dir build-gateway-tsan --output-on-failure
```

Configuration and compilation completed successfully. TSan results on this
WSL2 host are nondeterministic because many processes fail while reserving the
TSan shadow address space. The final handoff run completed 2 of 12 tests
without a TSan report (`ucp_io_operation_test` and
`ucp_event_loop_operation_test`). Of the other 10 tests, eight failed directly
with `unexpected memory mapping`, `ucp_proxy_session_test` had a startup
segmentation fault, and the shutdown test's signal-path child failed shadow
memory initialization so its parent assertion failed. None produced an
application race report.

Representative output from the smallest test was:

```text
Start 1: ucp_result_test
FATAL: ThreadSanitizer: unexpected memory mapping 0x5d8cc2256000-0x5d8cc2258000
```

`ucp_result_test` does not create an EventLoop or submit io_uring operations.
Its startup failure therefore shows that the mapping error is not a
liburing-specific report.

## Races Found And Fixed

An earlier run did initialize far enough to report a real race between
`EventLoop::~EventLoop()` closing the wakeup eventfd and another thread in
`EventLoop::quit() -> EventLoop::wakeup()` writing that fd. The root cause was
that `EventLoopThread` returned a pointer to a loop living on the worker stack;
the worker could destroy it before an external lifecycle call returned.

The fix gives `EventLoopThread` ownership through join, linearizes normal and
control ingress against a one-way stopping state, drains accepted callbacks
and in-flight operations, then releases kernel resources while retaining the
stopped object until its owner is destroyed. The regression covers concurrent
producers, four simultaneous quitters, quit-before-loop, final-drain I/O
rejection, and cancellation of an in-flight read.

A later initialized run found a second race between
`GatewayServer::Impl::~Impl()` destroying `shutdownCv_` and a base-loop callback
still executing `notify_all()`. Local phase-completion predicates and their
notifications now execute under the same `shutdownMutex_`, so a waiter cannot
return and destroy the condition variable before notification completes.

Post-fix targeted evidence:

```bash
ctest --test-dir build-gateway-tsan --output-on-failure \
  --repeat until-pass:30 -R ucp_event_loop_operation_test
ctest --test-dir build-gateway-tsan --output-on-failure \
  --repeat until-pass:30 -R ucp_upstream_pool_test
ctest --test-dir build-gateway-tsan --output-on-failure \
  --repeat until-pass:30 -R ucp_gateway_shutdown_test
```

The EventLoop and upstream-pool tests each obtained a successful TSan run with
no race report. The shutdown test did not obtain an end-to-end pass: one parent
process completed both in-process shutdown cases without a report, but its
signal-path child then failed TSan shadow-memory initialization. Other attempts
failed at initialization before application code.

## Interpretation

The final full TSan suite is **not clean** because 10 processes did not
initialize. The successful runs are useful evidence for the exercised paths,
but they do not justify a project-wide race-free claim. A suppression is not
appropriate: the remaining failure is a fatal shadow-memory mapping conflict,
not a suppressible application stack.

The normal and ASan/UBSan suites independently pass all 12 tests, but they do
not replace TSan. Before making a race-free claim, rerun the full command on a
native Linux host where GCC ThreadSanitizer initializes reliably, and preserve
any reported user-space stack without broad suppressions.
