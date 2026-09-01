#pragma once

#include "ucp/proxy/GatewayConfig.hpp"
#include "ucp/proxy/GatewayMetrics.hpp"

#include <chrono>
#include <cstddef>
#include <memory>

class EventLoop;

namespace ucp::proxy {

class GatewayServer {
public:
    GatewayServer(EventLoop& baseLoop, GatewayConfig config);
    ~GatewayServer();

    GatewayServer(const GatewayServer&) = delete;
    GatewayServer& operator=(const GatewayServer&) = delete;

    void start();
    // Synchronous drain entry point; call from a signal/coordinator thread,
    // never from the base EventLoop thread.
    void stop(std::chrono::milliseconds gracePeriod);
    GatewayMetricsSnapshot metrics() const;
    std::size_t activeSessionCount() const noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace ucp::proxy
