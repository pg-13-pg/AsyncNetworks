#pragma once

#include "ucp/proxy/GatewayConfig.hpp"

#include <cstddef>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace ucp::proxy {

class RouteTable {
public:
    explicit RouteTable(const std::vector<Route>& routes);

    const Route* match(std::string_view path) const noexcept;

private:
    std::vector<const Route*> orderedRoutes_;
};

class RoundRobinBalancer {
public:
    const Endpoint* select(const Route& route);

private:
    std::unordered_map<const Route*, std::size_t> cursors_;
};

} // namespace ucp::proxy
