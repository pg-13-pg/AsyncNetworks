#pragma once

#include "Buffer.hpp"
#include "ucp/net/AsyncSocket.hpp"
#include "ucp/proxy/HttpFramer.hpp"
#include "ucp/proxy/RouteTable.hpp"
#include "ucp/proxy/UpstreamPool.hpp"

#include <array>
#include <cstddef>
#include <functional>
#include <memory>
#include <string_view>

class TcpConnection;

namespace ucp::proxy {

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
        FinishCallback onFinished);

    DetachedTask run();
    void cancel();

private:
    Task<void> runLoop();
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
    void cancelInLoop();
    void finish(const std::shared_ptr<ProxySession>& self);

    std::shared_ptr<TcpConnection> downstream_;
    const RouteTable& routes_;
    RoundRobinBalancer& balancer_;
    UpstreamPool& pool_;
    HttpLimits limits_;
    FinishCallback onFinished_;
    Buffer downstreamInput_;
    Buffer upstreamInput_;
    std::array<std::byte, 16 * 1024> scratch_{};
    std::shared_ptr<TcpConnection> currentUpstream_;
    Deadline responseDeadline_;
    bool started_{false};
    bool cancelled_{false};
    bool responseStarted_{false};
    bool finished_{false};
};

} // namespace ucp::proxy
