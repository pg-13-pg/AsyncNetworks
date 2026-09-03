#include "ucp/proxy/ProxySession.hpp"

#include "EventLoop.hpp"
#include "TcpConnection.hpp"
#include "ucp/proxy/GatewayMetrics.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace ucp::proxy {
namespace {

std::span<const std::byte> bytesOf(std::string_view value)
{
    return std::as_bytes(std::span(value.data(), value.size()));
}

std::span<const std::byte> bytesOf(const char* data, std::size_t size)
{
    return std::as_bytes(std::span(data, size));
}

Error parserError(HttpParseError error, std::string message)
{
    return {
        ErrorCode::protocol,
        static_cast<int>(error),
        std::move(message)};
}

int requestErrorStatus(const Error& error)
{
    if (error.code != ErrorCode::protocol) {
        return 400;
    }
    const auto parseError = static_cast<HttpParseError>(error.systemError);
    if (parseError == HttpParseError::headerTooLarge) {
        return 431;
    }
    if (parseError == HttpParseError::bodyTooLarge) {
        return 413;
    }
    return 400;
}

std::string_view reasonForStatus(int status)
{
    switch (status) {
    case 400:
        return "Bad Request";
    case 404:
        return "Not Found";
    case 413:
        return "Payload Too Large";
    case 431:
        return "Request Header Fields Too Large";
    case 502:
        return "Bad Gateway";
    case 503:
        return "Service Unavailable";
    case 504:
        return "Gateway Timeout";
    default:
        return "Proxy Error";
    }
}

std::string normalizeConnection(std::string head, bool keepAlive)
{
    const auto start = head.rfind("Connection: ");
    if (start == std::string::npos) {
        return head;
    }
    const auto end = head.find("\r\n", start);
    if (end == std::string::npos) {
        return head;
    }
    head.replace(
        start, end - start,
        keepAlive ? "Connection: keep-alive" : "Connection: close");
    return head;
}

} // namespace

ProxySession::ProxySession(
    std::shared_ptr<TcpConnection> downstream,
    const RouteTable& routes,
    RoundRobinBalancer& balancer,
    UpstreamPool& pool,
    const HttpLimits& limits,
    FinishCallback onFinished,
    GatewayMetricShard* metrics)
    : downstream_(std::move(downstream))
    , routes_(routes)
    , balancer_(balancer)
    , pool_(pool)
    , limits_(limits)
    , onFinished_(std::move(onFinished))
    , metrics_(metrics)
{
}

DetachedTask ProxySession::run()
{
    auto self = shared_from_this();
    if (started_) {
        co_return;
    }
    started_ = true;
    monitorDownstreamClose(self, downstream_);
    try {
        co_await runLoop();
    } catch (...) {
        std::fputs("ProxySession: unhandled session exception\n", stderr);
        cancelInLoop();
    }
    finish(self);
}

DetachedTask ProxySession::monitorDownstreamClose(
    std::weak_ptr<ProxySession> session,
    std::shared_ptr<TcpConnection> downstream)
{
    auto closed = co_await asyncWaitPeerClose(std::move(downstream));
    if (closed) {
        if (auto owner = session.lock(); owner && !owner->finished_) {
            owner->cancelInLoop();
        }
    }
}

Task<void> ProxySession::runLoop()
{
    while (!cancelled_) {
        responseStarted_ = false;
        requestStartedAt_ = std::chrono::steady_clock::now();
        requestInProgress_ = false;
        auto requestResult = co_await readRequestHead();
        if (!requestResult) {
            const auto error = requestResult.error();
            recordError(error);
            if (error.code != ErrorCode::eof
                && error.code != ErrorCode::cancelled) {
                const int status = requestErrorStatus(error);
                (void)co_await sendError(status, reasonForStatus(status));
            }
            break;
        }
        auto request = std::move(requestResult).takeValue();
        requestInProgress_ = true;
        if (metrics_) {
            metrics_->requests.fetch_add(1, std::memory_order_relaxed);
        }

        const Route* route = routes_.match(request.path);
        if (!route) {
            (void)co_await sendError(404, reasonForStatus(404));
            break;
        }
        const Endpoint* endpoint = balancer_.select(*route);
        if (!endpoint) {
            (void)co_await sendError(503, reasonForStatus(503));
            break;
        }

        auto acquired = co_await pool_.acquire(
            *route, *endpoint,
            std::chrono::steady_clock::now() + route->connectTimeout);
        if (!acquired) {
            const int status = acquired.error().code == ErrorCode::timedOut
                ? 504
                : (acquired.error().code == ErrorCode::resourceExhausted
                       ? 503
                       : 502);
            (void)co_await sendError(status, reasonForStatus(status));
            break;
        }

        auto lease = std::move(acquired).takeValue();
        currentUpstream_ = lease.connection();
        responseDeadline_ =
            std::chrono::steady_clock::now() + route->responseTimeout;

        auto writeHead = co_await asyncWriteAll(
            currentUpstream_, bytesOf(request.forwardHead),
            responseDeadline_);
        if (!writeHead) {
            recordError(writeHead.error());
            const int status = writeHead.error().code == ErrorCode::timedOut
                ? 504
                : 502;
            (void)co_await sendError(status, reasonForStatus(status));
            currentUpstream_.reset();
            break;
        }
        if (metrics_) {
            metrics_->bytesFromClients.fetch_add(
                writeHead.value(), std::memory_order_relaxed);
        }

        auto upload = co_await streamExact(
            downstream_, downstreamInput_, currentUpstream_,
            request.contentLength, responseDeadline_,
            metrics_ ? &metrics_->bytesFromClients : nullptr);
        if (!upload) {
            recordError(upload.error());
            const int status = upload.error().code == ErrorCode::timedOut
                ? 504
                : (upload.error().code == ErrorCode::protocol ? 400 : 502);
            (void)co_await sendError(status, reasonForStatus(status));
            currentUpstream_.reset();
            break;
        }

        upstreamInput_.reset();
        auto responseResult = co_await readResponseHead(currentUpstream_);
        if (!responseResult) {
            recordError(responseResult.error());
            const int status =
                responseResult.error().code == ErrorCode::timedOut
                ? 504
                : 502;
            (void)co_await sendError(status, reasonForStatus(status));
            currentUpstream_.reset();
            break;
        }
        auto response = std::move(responseResult).takeValue();
        const bool keepAlive = request.keepAlive && response.keepAlive;
        response.forwardHead = normalizeConnection(
            std::move(response.forwardHead), keepAlive);

        responseStarted_ = true;
        auto downstreamHead = co_await asyncWriteAll(
            downstream_, bytesOf(response.forwardHead), responseDeadline_);
        if (!downstreamHead) {
            recordError(downstreamHead.error());
            currentUpstream_.reset();
            break;
        }
        if (metrics_) {
            metrics_->bytesToClients.fetch_add(
                downstreamHead.value(), std::memory_order_relaxed);
        }
        auto download = co_await streamExact(
            currentUpstream_, upstreamInput_, downstream_,
            response.contentLength, responseDeadline_,
            metrics_ ? &metrics_->bytesToClients : nullptr);
        if (!download) {
            recordError(download.error());
            currentUpstream_.reset();
            break;
        }

        recordStatus(response.statusCode);
        recordLatency();

        if (keepAlive) {
            lease.markReusable();
        }
        currentUpstream_.reset();
        responseDeadline_.reset();
        if (!keepAlive) {
            break;
        }
    }

    if (downstream_ && downstream_->isConnected()) {
        downstream_->forceClose();
    }
    co_return;
}

Task<Result<HttpRequestHead>> ProxySession::readRequestHead()
{
    while (true) {
        auto parsed = parseRequestHead(downstreamInput_, limits_);
        if (parsed.status == ParseStatus::complete) {
            co_return Result<HttpRequestHead>::success(
                std::move(parsed.request));
        }
        if (parsed.status == ParseStatus::error) {
            co_return Result<HttpRequestHead>::failure(
                parserError(parsed.error, "invalid downstream request head"));
        }

        const auto buffered = downstreamInput_.readableBytes();
        if (buffered >= limits_.maxHeaderBytes) {
            co_return Result<HttpRequestHead>::failure(
                parserError(
                    HttpParseError::headerTooLarge,
                    "downstream request head is too large"));
        }
        const auto available = std::min(
            scratch_.size(), limits_.maxHeaderBytes - buffered);
        auto read = co_await asyncReadSome(
            downstream_, std::span(scratch_).first(available), std::nullopt);
        if (!read) {
            co_return Result<HttpRequestHead>::failure(read.error());
        }
        downstreamInput_.append(
            reinterpret_cast<const char*>(scratch_.data()), read.value());
    }
}

Task<Result<HttpResponseHead>> ProxySession::readResponseHead(
    const std::shared_ptr<TcpConnection>& upstream)
{
    while (true) {
        auto parsed = parseResponseHead(upstreamInput_, limits_);
        if (parsed.status == ParseStatus::complete) {
            co_return Result<HttpResponseHead>::success(
                std::move(parsed.response));
        }
        if (parsed.status == ParseStatus::error) {
            co_return Result<HttpResponseHead>::failure(
                parserError(parsed.error, "invalid upstream response head"));
        }

        const auto buffered = upstreamInput_.readableBytes();
        if (buffered >= limits_.maxHeaderBytes) {
            co_return Result<HttpResponseHead>::failure(
                parserError(
                    HttpParseError::headerTooLarge,
                    "upstream response head is too large"));
        }
        const auto available = std::min(
            scratch_.size(), limits_.maxHeaderBytes - buffered);
        auto read = co_await asyncReadSome(
            upstream, std::span(scratch_).first(available),
            responseDeadline_);
        if (!read) {
            co_return Result<HttpResponseHead>::failure(read.error());
        }
        upstreamInput_.append(
            reinterpret_cast<const char*>(scratch_.data()), read.value());
    }
}

Task<IoResult> ProxySession::streamExact(
    const std::shared_ptr<TcpConnection>& source,
    Buffer& sourceBuffer,
    const std::shared_ptr<TcpConnection>& destination,
    std::size_t bytes,
    Deadline deadline,
    std::atomic_uint64_t* transferredBytes)
{
    std::size_t transferred = 0;
    while (transferred < bytes) {
        if (sourceBuffer.readableBytes() > 0) {
            const auto count = std::min(
                sourceBuffer.readableBytes(), bytes - transferred);
            auto write = co_await asyncWriteAll(
                destination,
                bytesOf(sourceBuffer.readBeginAddr(), count), deadline);
            if (!write) {
                co_return IoResult::failure(write.error());
            }
            if (transferredBytes) {
                transferredBytes->fetch_add(
                    write.value(), std::memory_order_relaxed);
            }
            sourceBuffer.retrieve(count);
            transferred += count;
            continue;
        }

        const auto requested = std::min(
            scratch_.size(), bytes - transferred);
        auto read = co_await asyncReadSome(
            source, std::span(scratch_).first(requested), deadline);
        if (!read) {
            if (read.error().code == ErrorCode::eof) {
                if (source == downstream_) {
                    cancelInLoop();
                }
                co_return IoResult::failure(
                    {ErrorCode::protocol, 0,
                     "stream ended before Content-Length"});
            }
            co_return IoResult::failure(read.error());
        }
        auto write = co_await asyncWriteAll(
            destination,
            std::span<const std::byte>(scratch_.data(), read.value()),
            deadline);
        if (!write) {
            co_return IoResult::failure(write.error());
        }
        if (transferredBytes) {
            transferredBytes->fetch_add(
                write.value(), std::memory_order_relaxed);
        }
        transferred += read.value();
    }
    co_return IoResult::success(transferred);
}

Task<IoResult> ProxySession::sendError(
    int status, std::string_view reason)
{
    if (responseStarted_) {
        co_return IoResult::failure(
            {ErrorCode::protocol, 0,
             "response already started"});
    }
    const std::string response =
        "HTTP/1.1 " + std::to_string(status) + ' '
        + std::string(reason)
        + "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    responseStarted_ = true;
    auto written = co_await asyncWriteAll(
        downstream_, bytesOf(response),
        std::chrono::steady_clock::now() + std::chrono::seconds(1));
    if (written) {
        if (metrics_) {
            metrics_->bytesToClients.fetch_add(
                written.value(), std::memory_order_relaxed);
        }
        recordStatus(status);
        recordLatency();
    }
    co_return written;
}

void ProxySession::cancel()
{
    if (!downstream_) {
        return;
    }
    EventLoop* loop = downstream_->getLoop();
    if (loop->isInLoopThread()) {
        cancelInLoop();
        return;
    }
    auto self = shared_from_this();
    if (!loop->queueControlInLoop([self] { self->cancelInLoop(); })) {
        LOG_WARN("ProxySession cancellation rejected by stopping EventLoop");
    }
}

void ProxySession::cancelInLoop()
{
    if (cancelled_) {
        return;
    }
    cancelled_ = true;
    recordCancellation();
    if (currentUpstream_) {
        currentUpstream_->cancelPendingOperations();
    }
    if (downstream_) {
        downstream_->cancelPendingOperations();
        if (downstream_->isConnected()) {
            downstream_->forceClose();
        }
    }
}

void ProxySession::finish(const std::shared_ptr<ProxySession>& self)
{
    if (finished_) {
        return;
    }
    finished_ = true;
    if (downstream_) {
        downstream_->cancelPendingOperations();
    }
    if (onFinished_) {
        onFinished_(self);
    }
}

void ProxySession::recordStatus(int status) noexcept
{
    if (!metrics_) {
        return;
    }
    if (status >= 200 && status < 300) {
        metrics_->status2xx.fetch_add(1, std::memory_order_relaxed);
    } else if (status >= 400 && status < 500) {
        metrics_->status4xx.fetch_add(1, std::memory_order_relaxed);
    } else if (status >= 500 && status < 600) {
        metrics_->status5xx.fetch_add(1, std::memory_order_relaxed);
    }
}

void ProxySession::recordError(const Error& error) noexcept
{
    if (!metrics_) {
        return;
    }
    switch (error.code) {
    case ErrorCode::protocol:
        metrics_->protocolErrors.fetch_add(1, std::memory_order_relaxed);
        break;
    case ErrorCode::timedOut:
        metrics_->timeoutErrors.fetch_add(1, std::memory_order_relaxed);
        break;
    case ErrorCode::cancelled:
        recordCancellation();
        break;
    case ErrorCode::resourceExhausted:
        metrics_->overloadErrors.fetch_add(1, std::memory_order_relaxed);
        break;
    default:
        break;
    }
}

void ProxySession::recordCancellation() noexcept
{
    if (metrics_ && !cancellationRecorded_) {
        metrics_->cancellations.fetch_add(1, std::memory_order_relaxed);
        cancellationRecorded_ = true;
    }
}

void ProxySession::recordLatency() noexcept
{
    if (!metrics_ || !requestInProgress_) {
        return;
    }
    const auto elapsed = std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - requestStartedAt_);
    metrics_->recordLatency(elapsed.count());
    requestInProgress_ = false;
}

} // namespace ucp::proxy
