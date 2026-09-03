# Gateway Cancellation Regression Notes

Date: 2026-09-03

## Symptom

`ucp_gateway_failure_test` intermittently timed out while waiting for
`GatewayMetricsSnapshot::cancellations` to become nonzero in
`downstreamCloseCancelsUpload()`.

## Root Cause

The test wrote the full 1 MiB request body before closing the client socket.
Those bytes could remain in the gateway's kernel receive buffer while the
mock upstream intentionally stopped reading. A normal TCP FIN is not a
reliable immediate cancellation signal while unread request bytes remain, so
the downstream-close monitor could complete after the test's two-second
assertion window.

There was also a metric gap when the upload read itself observed downstream
EOF before the close monitor: the session classified the truncated HTTP body
as a protocol error without first entering its cancellation path.

## Resolution

`ProxySession::streamExact()` now calls `cancelInLoop()` when its downstream
source reaches EOF before the declared request body is complete. This cancels
the upstream operation promptly.

Cancellation accounting is now session-scoped and idempotent:

- `cancelInLoop()` records the cancellation when the session first transitions
  to cancelled.
- A later `ErrorCode::cancelled` completion uses the same helper, preventing a
  second increment when the cancellation monitor and an I/O completion race.

The regression test now sends only the HTTP request head, waits until the
upstream has accepted the request, and then closes the client. This represents
a client abandoning an upload that the gateway is actively waiting to read,
without pre-buffering the entire declared body. It asserts exactly one
cancellation for the session.

## Verification

The focused ASan test was repeated 50 times:

```bash
ctest --test-dir build-gateway-asan -R ucp_gateway_failure_test \
  --output-on-failure --repeat until-fail:50
```

All 50 runs passed. The full ASan suite then passed all 12 tests:

```bash
cmake --build build-gateway-asan -j2
ctest --test-dir build-gateway-asan --output-on-failure
```
