#include "EventLoopThread.hpp"
#include "TestSupport.hpp"
#include "TcpConnection.hpp"
#include "mock_http_upstream.hpp"
#include "ucp/proxy/ProxySession.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
namespace proxy = ucp::proxy;

namespace {

void sendAll(int fd, std::string_view bytes, bool oneByteAtATime)
{
    while (!bytes.empty()) {
        const auto requested = oneByteAtATime ? 1U : bytes.size();
        const auto count = ::send(
            fd, bytes.data(), requested, MSG_NOSIGNAL);
        if (count > 0) {
            bytes.remove_prefix(static_cast<std::size_t>(count));
            if (oneByteAtATime) {
                std::this_thread::sleep_for(1ms);
            }
        } else if (count < 0
                   && (errno == EAGAIN || errno == EWOULDBLOCK
                       || errno == EINTR)) {
            std::this_thread::sleep_for(1ms);
        } else {
            CHECK(false);
        }
    }
}

std::string readResponseBody(int fd, std::string& pending)
{
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline) {
        const auto headerEnd = pending.find("\r\n\r\n");
        if (headerEnd != std::string::npos) {
            const auto lengthHeader = pending.find("Content-Length:");
            CHECK(lengthHeader != std::string::npos);
            auto cursor = lengthHeader + std::strlen("Content-Length:");
            while (pending[cursor] == ' ') {
                ++cursor;
            }
            std::size_t contentLength = 0;
            while (cursor < pending.size()
                   && pending[cursor] >= '0' && pending[cursor] <= '9') {
                contentLength = contentLength * 10
                    + static_cast<std::size_t>(pending[cursor] - '0');
                ++cursor;
            }
            const auto bodyStart = headerEnd + 4;
            if (pending.size() >= bodyStart + contentLength) {
                auto body = pending.substr(bodyStart, contentLength);
                pending.erase(0, bodyStart + contentLength);
                return body;
            }
        }

        char buffer[4096];
        const auto count = ::recv(fd, buffer, sizeof(buffer), 0);
        if (count > 0) {
            pending.append(buffer, static_cast<std::size_t>(count));
        } else if (count < 0
                   && (errno == EAGAIN || errno == EWOULDBLOCK
                       || errno == EINTR)) {
            std::this_thread::sleep_for(1ms);
        } else {
            CHECK(false);
        }
    }
    CHECK(false);
    return {};
}

std::size_t countOccurrences(
    std::string_view value, std::string_view needle)
{
    std::size_t count = 0;
    std::size_t position = 0;
    while ((position = value.find(needle, position))
           != std::string_view::npos) {
        ++count;
        position += needle.size();
    }
    return count;
}

} // namespace

int main()
{
    MockHttpUpstream upstream({"world", "again"});

    int downstreamSockets[2];
    CHECK_EQ(::socketpair(
        AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
        0, downstreamSockets), 0);

    EventLoop::Options options;
    options.ringEntries = 128;
    options.sqpoll = false;
    options.registeredBuffersCount = 0;
    EventLoopThread thread(options);
    EventLoop* loop = thread.startLoop();

    proxy::Route route;
    route.name = "api";
    route.prefix = "/api/";
    route.upstreams.push_back({"127.0.0.1", upstream.port()});
    route.connectTimeout = 500ms;
    route.responseTimeout = 2s;
    route.maxConnectionsPerWorker = 2;
    route.maxIdlePerWorker = 1;
    route.idleTimeout = 30s;
    std::vector<proxy::Route> routes{route};
    proxy::RouteTable routeTable(routes);
    proxy::RoundRobinBalancer balancer;
    proxy::UpstreamPool pool(*loop);

    auto downstream = std::make_shared<TcpConnection>(
        "proxy-downstream", loop, downstreamSockets[0], InetAddress{});
    std::atomic_bool finished{false};
    auto session = std::make_shared<proxy::ProxySession>(
        downstream, routeTable, balancer, pool, proxy::HttpLimits{},
        [&](const std::shared_ptr<proxy::ProxySession>& completed) {
            CHECK(completed != nullptr);
            finished.store(true, std::memory_order_release);
        });

    loop->queueControlInLoop([downstream, session] {
        downstream->connectEstablished();
        session->run();
    });

    std::thread client([&] {
        const std::string firstHead =
            "POST /api/items HTTP/1.1\r\n"
            "Host: example\r\n"
            "Content-Length: 5\r\n"
            "Connection: keep-alive\r\n\r\n";
        sendAll(downstreamSockets[1], firstHead, true);
        sendAll(downstreamSockets[1], "hello", false);

        std::string pending;
        CHECK_EQ(readResponseBody(downstreamSockets[1], pending), "world");

        const std::string second =
            "GET /api/again HTTP/1.1\r\n"
            "Host: example\r\n"
            "Content-Length: 0\r\n"
            "Connection: keep-alive\r\n\r\n";
        sendAll(downstreamSockets[1], second, false);
        CHECK_EQ(readResponseBody(downstreamSockets[1], pending), "again");
        CHECK_EQ(::close(downstreamSockets[1]), 0);
    });

    CHECK(TestSupport::waitUntil(
        [&] { return finished.load(std::memory_order_acquire); }, 10s));
    client.join();
    CHECK(TestSupport::waitUntil(
        [&] { return upstream.requests().size() == 2; }, 2s));
    CHECK_EQ(upstream.accepts(), 1U);

    const auto requests = upstream.requests();
    CHECK_EQ(requests[0].body, "hello");
    CHECK_EQ(requests[1].body, "");
    CHECK(requests[0].head.starts_with(
        "POST /api/items HTTP/1.1\r\n"));
    CHECK(requests[1].head.starts_with(
        "GET /api/again HTTP/1.1\r\n"));
    CHECK_EQ(countOccurrences(requests[0].head, "Connection:"), 1U);
    CHECK(requests[0].head.find("Connection: keep-alive\r\n")
          != std::string::npos);

    std::atomic_bool cleaned{false};
    loop->queueControlInLoop([&] {
        pool.closeIdle();
        downstream->connectDestroyed();
        cleaned.store(true, std::memory_order_release);
    });
    CHECK(TestSupport::waitUntil(
        [&] { return cleaned.load(std::memory_order_acquire); }, 2s));
    loop->quit();
    return 0;
}
