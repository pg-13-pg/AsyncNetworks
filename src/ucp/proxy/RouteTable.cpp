#include "ucp/proxy/RouteTable.hpp"

#include <algorithm>

namespace ucp::proxy {

RouteTable::RouteTable(const std::vector<Route>& routes)
{
    orderedRoutes_.reserve(routes.size());
    for (const auto& route : routes) {
        orderedRoutes_.push_back(&route);
    }
    std::sort(
        orderedRoutes_.begin(), orderedRoutes_.end(),
        [](const Route* lhs, const Route* rhs) {
            if (lhs->prefix.size() != rhs->prefix.size()) {
                return lhs->prefix.size() > rhs->prefix.size();
            }
            return lhs->name < rhs->name;
        });
}

const Route* RouteTable::match(std::string_view path) const noexcept
{
    for (const Route* route : orderedRoutes_) {
        if (path.starts_with(route->prefix)) {
            return route;
        }
    }
    return nullptr;
}

const Endpoint* RoundRobinBalancer::select(const Route& route)
{
    if (route.upstreams.empty()) {
        return nullptr;
    }
    auto& cursor = cursors_[&route];
    const auto index = cursor % route.upstreams.size();
    cursor = (index + 1) % route.upstreams.size();
    return &route.upstreams[index];
}

} // namespace ucp::proxy
