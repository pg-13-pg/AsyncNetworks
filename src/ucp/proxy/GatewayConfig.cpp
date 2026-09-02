#include "ucp/proxy/GatewayConfig.hpp"

#include "Config.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <charconv>
#include <limits>
#include <optional>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace ucp::proxy {
namespace {

using Values = std::unordered_map<std::string, std::string>;

std::string_view trim(std::string_view value)
{
    while (!value.empty()
           && (value.front() == ' ' || value.front() == '\t')) {
        value.remove_prefix(1);
    }
    while (!value.empty()
           && (value.back() == ' ' || value.back() == '\t')) {
        value.remove_suffix(1);
    }
    return value;
}

std::optional<std::string_view> findRaw(
    const Values& values, std::string_view key)
{
    const auto found = values.find(std::string(key));
    if (found == values.end()) {
        return std::nullopt;
    }
    return trim(found->second);
}

template <typename Integer>
bool parseUnsigned(std::string_view raw, Integer& output)
{
    static_assert(std::is_integral_v<Integer>);
    static_assert(std::is_unsigned_v<Integer>);
    if (raw.empty()) {
        return false;
    }
    unsigned long long value = 0;
    const auto [end, error] = std::from_chars(
        raw.data(), raw.data() + raw.size(), value);
    if (error != std::errc{} || end != raw.data() + raw.size()
        || value > std::numeric_limits<Integer>::max()) {
        return false;
    }
    output = static_cast<Integer>(value);
    return true;
}

bool parsePositiveInt(std::string_view raw, int& output)
{
    unsigned int value = 0;
    if (!parseUnsigned(raw, value)
        || value == 0
        || value > static_cast<unsigned int>(
            std::numeric_limits<int>::max())) {
        return false;
    }
    output = static_cast<int>(value);
    return true;
}

bool parseBool(std::string_view raw, bool& output)
{
    std::string normalized(raw);
    std::transform(
        normalized.begin(), normalized.end(), normalized.begin(),
        [](unsigned char ch) {
            return ch >= 'A' && ch <= 'Z'
                ? static_cast<char>(ch + ('a' - 'A'))
                : static_cast<char>(ch);
        });
    if (normalized == "true" || normalized == "1"
        || normalized == "yes" || normalized == "on") {
        output = true;
        return true;
    }
    if (normalized == "false" || normalized == "0"
        || normalized == "no" || normalized == "off") {
        output = false;
        return true;
    }
    return false;
}

Result<GatewayConfig> failure(std::string message)
{
    return Result<GatewayConfig>::failure(
        {ErrorCode::protocol, 0, std::move(message)});
}

template <typename Integer>
bool readUnsigned(
    const Values& values,
    std::string_view key,
    Integer defaultValue,
    Integer& output,
    bool allowZero = false)
{
    const auto raw = findRaw(values, key);
    if (!raw) {
        output = defaultValue;
        return allowZero || output != 0;
    }
    return parseUnsigned(*raw, output) && (allowZero || output != 0);
}

bool readDuration(
    const Values& values,
    std::string_view key,
    std::chrono::milliseconds defaultValue,
    std::chrono::milliseconds& output)
{
    std::size_t milliseconds = 0;
    if (!readUnsigned(
            values, key,
            static_cast<std::size_t>(defaultValue.count()),
            milliseconds)) {
        return false;
    }
    if (milliseconds > static_cast<std::size_t>(
            std::chrono::milliseconds::max().count())) {
        return false;
    }
    output = std::chrono::milliseconds(milliseconds);
    return true;
}

bool validIpv4(std::string_view host)
{
    in_addr address{};
    const std::string text(host);
    return ::inet_pton(AF_INET, text.c_str(), &address) == 1;
}

bool parseEndpoint(std::string_view raw, Endpoint& endpoint)
{
    raw = trim(raw);
    const auto colon = raw.find(':');
    if (colon == std::string_view::npos
        || colon == 0
        || colon != raw.rfind(':')
        || colon + 1 == raw.size()) {
        return false;
    }

    const auto host = trim(raw.substr(0, colon));
    const auto portText = trim(raw.substr(colon + 1));
    std::uint16_t port = 0;
    if (!validIpv4(host) || !parseUnsigned(portText, port) || port == 0) {
        return false;
    }
    endpoint.host = host;
    endpoint.port = port;
    return true;
}

bool parseUpstreams(
    std::string_view raw, std::vector<Endpoint>& endpoints)
{
    raw = trim(raw);
    if (raw.empty()) {
        return false;
    }
    while (true) {
        const auto comma = raw.find(',');
        Endpoint endpoint;
        if (!parseEndpoint(raw.substr(0, comma), endpoint)) {
            return false;
        }
        endpoints.push_back(std::move(endpoint));
        if (comma == std::string_view::npos) {
            return true;
        }
        raw.remove_prefix(comma + 1);
        if (raw.empty()) {
            return false;
        }
    }
}

} // namespace

std::string Endpoint::key() const
{
    return host + ':' + std::to_string(port);
}

InetAddress Endpoint::address() const
{
    return InetAddress(port, host);
}

Result<GatewayConfig> GatewayConfig::from(const Config& config)
{
    const auto& values = config.all();
    GatewayConfig result;

    const auto listenIp = findRaw(values, "gateway.listen_ip");
    if (!listenIp || !validIpv4(*listenIp)) {
        return failure("invalid gateway.listen_ip");
    }
    result.listenIp = *listenIp;

    if (!readUnsigned(
            values, "gateway.listen_port", std::uint16_t{0},
            result.listenPort)) {
        return failure("invalid gateway.listen_port");
    }

    const auto workers = findRaw(values, "gateway.workers");
    if (workers && !parsePositiveInt(*workers, result.workerCount)) {
        return failure("invalid gateway.workers");
    }
    if (!workers && result.workerCount <= 0) {
        return failure("invalid gateway.workers");
    }

    if (!readUnsigned(
            values, "gateway.header_limit",
            result.httpLimits.maxHeaderBytes,
            result.httpLimits.maxHeaderBytes)) {
        return failure("invalid gateway.header_limit");
    }
    if (!readUnsigned(
            values, "gateway.body_limit",
            result.httpLimits.maxBodyBytes,
            result.httpLimits.maxBodyBytes)) {
        return failure("invalid gateway.body_limit");
    }
    if (!readDuration(
            values, "gateway.graceful_shutdown_ms",
            result.gracefulShutdown, result.gracefulShutdown)) {
        return failure("invalid gateway.graceful_shutdown_ms");
    }

    auto& loop = result.eventLoopOptions;
    if (!readUnsigned(
            values, "event_loop.ring_entries",
            loop.ringEntries, loop.ringEntries)) {
        return failure("invalid event_loop.ring_entries");
    }
    if (loop.ringEntries > std::numeric_limits<unsigned int>::max()) {
        return failure("invalid event_loop.ring_entries");
    }
    if (const auto raw = findRaw(values, "event_loop.sqpoll")) {
        if (!parseBool(*raw, loop.sqpoll)) {
            return failure("invalid event_loop.sqpoll");
        }
    }
    if (!readUnsigned(
            values, "event_loop.sqpoll_idle_ms",
            loop.sqpollIdleMs, loop.sqpollIdleMs, true)) {
        return failure("invalid event_loop.sqpoll_idle_ms");
    }
    if (!readUnsigned(
            values, "event_loop.registered_buffers_count",
            loop.registeredBuffersCount,
            loop.registeredBuffersCount, true)) {
        return failure("invalid event_loop.registered_buffers_count");
    }
    if (!readUnsigned(
            values, "event_loop.registered_buffer_size",
            loop.registeredBuffersSize,
            loop.registeredBuffersSize)) {
        return failure("invalid event_loop.registered_buffer_size");
    }
    if (!readUnsigned(
            values, "event_loop.pending_queue_capacity",
            loop.pendingQueueCapacity,
            loop.pendingQueueCapacity)) {
        return failure("invalid event_loop.pending_queue_capacity");
    }
    if (!readUnsigned(
            values, "event_loop.pending_submission_capacity",
            loop.pendingSubmissionCapacity,
            loop.pendingSubmissionCapacity)) {
        return failure("invalid event_loop.pending_submission_capacity");
    }

    std::vector<std::string> routeNames;
    constexpr std::string_view routePrefix{"route."};
    constexpr std::string_view prefixSuffix{".prefix"};
    for (const auto& [key, value] : values) {
        (void)value;
        if (key.starts_with(routePrefix)
            && key.ends_with(prefixSuffix)
            && key.size() > routePrefix.size() + prefixSuffix.size()) {
            routeNames.push_back(key.substr(
                routePrefix.size(),
                key.size() - routePrefix.size() - prefixSuffix.size()));
        }
    }
    std::sort(routeNames.begin(), routeNames.end());
    routeNames.erase(
        std::unique(routeNames.begin(), routeNames.end()), routeNames.end());
    if (routeNames.empty()) {
        return failure("gateway requires at least one route");
    }

    std::unordered_set<std::string> prefixes;
    result.routes.reserve(routeNames.size());
    for (const auto& name : routeNames) {
        const std::string base = "route." + name + '.';
        Route route;
        route.name = name;

        const auto prefix = findRaw(values, base + "prefix");
        if (!prefix || prefix->empty() || prefix->front() != '/') {
            return failure("invalid " + base + "prefix");
        }
        route.prefix = *prefix;
        if (!prefixes.insert(route.prefix).second) {
            return failure("duplicate route prefix: " + route.prefix);
        }

        const auto upstreams = findRaw(values, base + "upstreams");
        if (!upstreams || !parseUpstreams(*upstreams, route.upstreams)) {
            return failure("invalid " + base + "upstreams");
        }
        if (!readDuration(
                values, base + "connect_timeout_ms",
                route.connectTimeout, route.connectTimeout)) {
            return failure("invalid " + base + "connect_timeout_ms");
        }
        if (!readDuration(
                values, base + "response_timeout_ms",
                route.responseTimeout, route.responseTimeout)) {
            return failure("invalid " + base + "response_timeout_ms");
        }
        if (!readUnsigned(
                values, base + "max_connections_per_worker",
                route.maxConnectionsPerWorker,
                route.maxConnectionsPerWorker)) {
            return failure(
                "invalid " + base + "max_connections_per_worker");
        }
        if (!readUnsigned(
                values, base + "max_idle_per_worker",
                route.maxIdlePerWorker,
                route.maxIdlePerWorker, true)) {
            return failure("invalid " + base + "max_idle_per_worker");
        }
        if (route.maxIdlePerWorker > route.maxConnectionsPerWorker) {
            return failure(
                base + "max_idle_per_worker exceeds max_connections_per_worker");
        }
        if (!readDuration(
                values, base + "idle_timeout_ms",
                route.idleTimeout, route.idleTimeout)) {
            return failure("invalid " + base + "idle_timeout_ms");
        }
        result.routes.push_back(std::move(route));
    }

    return Result<GatewayConfig>::success(std::move(result));
}

} // namespace ucp::proxy
