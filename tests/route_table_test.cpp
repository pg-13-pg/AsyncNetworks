#include "Config.hpp"
#include "TestSupport.hpp"
#include "ucp/proxy/GatewayConfig.hpp"
#include "ucp/proxy/RouteTable.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

using namespace std::chrono_literals;
namespace proxy = ucp::proxy;

namespace {

const std::string validConfig = R"ini(
[gateway]
listen_ip=127.0.0.1
listen_port=8080
workers=4
header_limit=16384
body_limit=1048576

[event_loop]
ring_entries=4096
sqpoll=false
registered_buffers_count=0
registered_buffer_size=4096
pending_queue_capacity=65536

[route.api]
prefix=/api/
upstreams=127.0.0.1:9001,127.0.0.1:9002
connect_timeout_ms=500
response_timeout_ms=3000
max_connections_per_worker=4
max_idle_per_worker=2
idle_timeout_ms=30000

[route.users]
prefix=/api/users/
upstreams=127.0.0.1:9100
)ini";

std::string replaced(
    std::string value,
    std::string_view needle,
    std::string_view replacement)
{
    const auto position = value.find(needle);
    CHECK(position != std::string::npos);
    value.replace(position, needle.size(), replacement);
    return value;
}

ucp::Result<proxy::GatewayConfig> parse(std::string_view text)
{
    Config source;
    std::string error;
    CHECK(source.loadFromString(std::string(text), &error));
    return proxy::GatewayConfig::from(source);
}

void checkProtocolFailure(std::string text)
{
    const auto result = parse(text);
    CHECK(!result);
    CHECK_EQ(result.error().code, ucp::ErrorCode::protocol);
    CHECK(!result.error().message.empty());
}

} // namespace

int main()
{
    auto parsed = parse(validConfig);
    CHECK(parsed);
    auto gateway = std::move(parsed).takeValue();

    CHECK_EQ(gateway.listenIp, "127.0.0.1");
    CHECK_EQ(gateway.listenPort, 8080);
    CHECK_EQ(gateway.workerCount, 4);
    CHECK_EQ(gateway.httpLimits.maxHeaderBytes, 16U * 1024U);
    CHECK_EQ(gateway.httpLimits.maxBodyBytes, 1024U * 1024U);
    CHECK_EQ(gateway.eventLoopOptions.ringEntries, 4096U);
    CHECK(!gateway.eventLoopOptions.sqpoll);
    CHECK_EQ(gateway.eventLoopOptions.registeredBuffersCount, 0U);
    CHECK_EQ(gateway.eventLoopOptions.registeredBuffersSize, 4096U);
    CHECK_EQ(gateway.eventLoopOptions.pendingQueueCapacity, 65536U);
    CHECK_EQ(gateway.routes.size(), 2U);

    proxy::RouteTable routes(gateway.routes);
    const auto* users = routes.match("/api/users/7");
    CHECK(users != nullptr);
    CHECK_EQ(users->name, "users");
    CHECK_EQ(users->prefix, "/api/users/");

    const auto* api = routes.match("/api/items");
    CHECK(api != nullptr);
    CHECK_EQ(api->name, "api");
    CHECK(routes.match("/other") == nullptr);

    proxy::RoundRobinBalancer balancer;
    const std::uint16_t expected[] = {9001, 9002, 9001, 9002};
    for (const auto port : expected) {
        const auto* endpoint = balancer.select(*api);
        CHECK(endpoint != nullptr);
        CHECK_EQ(endpoint->port, port);
    }
    CHECK_EQ(api->upstreams[0].key(), "127.0.0.1:9001");
    CHECK_EQ(api->upstreams[0].address().toIpPort(), "127.0.0.1:9001");

    checkProtocolFailure(replaced(
        validConfig,
        "upstreams=127.0.0.1:9001,127.0.0.1:9002",
        "upstreams=bad-endpoint"));
    checkProtocolFailure(replaced(
        validConfig, "127.0.0.1:9001", "127.0.0.1:0"));
    checkProtocolFailure(replaced(
        validConfig, "prefix=/api/", "prefix="));
    checkProtocolFailure(replaced(
        validConfig,
        "upstreams=127.0.0.1:9001,127.0.0.1:9002",
        "upstreams="));
    checkProtocolFailure(replaced(
        validConfig,
        "max_idle_per_worker=2",
        "max_idle_per_worker=5"));
    checkProtocolFailure(replaced(
        validConfig, "workers=4", "workers=not-a-number"));
    checkProtocolFailure(replaced(
        validConfig, "listen_port=8080", ""));
    checkProtocolFailure(replaced(
        validConfig, "sqpoll=false", "sqpoll=maybe"));
    checkProtocolFailure(replaced(
        validConfig,
        "ring_entries=4096",
        "ring_entries=18446744073709551615"));

    proxy::Route emptyRoute;
    emptyRoute.name = "empty";
    CHECK(balancer.select(emptyRoute) == nullptr);
    return 0;
}
