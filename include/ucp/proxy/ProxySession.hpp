#pragma once

#include "Buffer.hpp"
#include "ucp/net/AsyncSocket.hpp"
#include "ucp/proxy/HttpFramer.hpp"
#include "ucp/proxy/RouteTable.hpp"
#include "ucp/proxy/UpstreamPool.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <string_view>

class TcpConnection;

namespace ucp::proxy {

struct GatewayMetricShard;

class ProxySession : public std::enable_shared_from_this<ProxySession> {
public:
    using FinishCallback =
        std::function<void(const std::shared_ptr<ProxySession>&)>;

    ProxySession(
        std::shared_ptr<TcpConnection> downstream,
        const RouteTable& routes,
        RoundRobinBalancer& balancer,
        UpstreamPool& pool,
        const HttpLimits& limits,
        FinishCallback onFinished,
        GatewayMetricShard* metrics = nullptr);

    DetachedTask run();
    void cancel();

private:
    static DetachedTask monitorDownstreamClose(
        std::weak_ptr<ProxySession> session,
        std::shared_ptr<TcpConnection> downstream);
    Task<void> runLoop();
    Task<Result<HttpRequestHead>> readRequestHead();
    Task<Result<HttpResponseHead>> readResponseHead(
        const std::shared_ptr<TcpConnection>& upstream);
    Task<IoResult> streamExact(
        const std::shared_ptr<TcpConnection>& source,
        Buffer& sourceBuffer,
        const std::shared_ptr<TcpConnection>& destination,
        std::size_t bytes,
        Deadline deadline,
        std::atomic_uint64_t* transferredBytes = nullptr);
    Task<IoResult> sendError(int status, std::string_view reason);
    void cancelInLoop();
    void finish(const std::shared_ptr<ProxySession>& self);
    void recordStatus(int status) noexcept;
    void recordError(const Error& error) noexcept;
    void recordCancellation() noexcept;
    void recordLatency() noexcept;

    std::shared_ptr<TcpConnection> downstream_;
    const RouteTable& routes_;
    RoundRobinBalancer& balancer_;
    UpstreamPool& pool_;
    HttpLimits limits_;
    FinishCallback onFinished_;
    GatewayMetricShard* metrics_{nullptr};
    Buffer downstreamInput_;
    Buffer upstreamInput_;
    std::array<std::byte, 16 * 1024> scratch_{};
    std::shared_ptr<TcpConnection> currentUpstream_;
    Deadline responseDeadline_;
    bool started_{false};
    bool cancelled_{false};
    bool cancellationRecorded_{false};
    bool responseStarted_{false};
    bool finished_{false};
    bool requestInProgress_{false};
    std::chrono::steady_clock::time_point requestStartedAt_{};
};

} // namespace ucp::proxy
