#pragma once

#include "EventLoop.hpp"
#include "InetAddress.hpp"
#include "ucp/proxy/HttpFramer.hpp"
#include "ucp/runtime/Result.hpp"

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

class Config;

namespace ucp::proxy {

struct Endpoint {
    std::string host;
    std::uint16_t port{0};

    std::string key() const;
    InetAddress address() const;
    bool operator==(const Endpoint&) const = default;
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
    std::uint16_t listenPort{0};
    int workerCount{4};
    EventLoop::Options eventLoopOptions;
    HttpLimits httpLimits;
    std::chrono::milliseconds gracefulShutdown{5000};
    std::vector<Route> routes;

    static Result<GatewayConfig> from(const Config& config);
};

} // namespace ucp::proxy
