#include "EventLoopThread.hpp"
#include "TestSupport.hpp"
#include "mock_http_upstream.hpp"
#include "ucp/proxy/GatewayServer.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <poll.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;
namespace proxy = ucp::proxy;

namespace {

std::uint16_t reservePort()
{
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    CHECK(fd >= 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    CHECK_EQ(::bind(
        fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
    socklen_t length = sizeof(address);
    CHECK_EQ(::getsockname(
        fd, reinterpret_cast<sockaddr*>(&address), &length), 0);
    const auto port = ntohs(address.sin_port);
    CHECK_EQ(::close(fd), 0);
    return port;
}

class BoundRefusedPort {
public:
    BoundRefusedPort()
        : fd_(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0))
    {
        CHECK(fd_ >= 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        CHECK_EQ(::bind(
            fd_, reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)), 0);
        socklen_t length = sizeof(address);
        CHECK_EQ(::getsockname(
            fd_, reinterpret_cast<sockaddr*>(&address), &length), 0);
        port_ = ntohs(address.sin_port);
    }

    ~BoundRefusedPort()
    {
        CHECK_EQ(::close(fd_), 0);
    }

    std::uint16_t port() const noexcept { return port_; }

private:
    int fd_;
    std::uint16_t port_{0};
};

proxy::GatewayConfig makeConfig(
    std::uint16_t upstreamPort,
    std::chrono::milliseconds responseTimeout = 1s,
    std::size_t maxConnections = 4)
{
    proxy::GatewayConfig config;
    config.listenIp = "127.0.0.1";
    config.listenPort = reservePort();
    config.workerCount = 1;
    config.eventLoopOptions.ringEntries = 128;
    config.eventLoopOptions.sqpoll = false;
    config.eventLoopOptions.registeredBuffersCount = 0;
    config.eventLoopOptions.pendingQueueCapacity = 128;
    config.eventLoopOptions.pendingSubmissionCapacity = 128;
    config.httpLimits.maxHeaderBytes = 16 * 1024;
    config.httpLimits.maxBodyBytes = 1024 * 1024;
    proxy::Route route;
    route.name = "api";
    route.prefix = "/api/";
    route.upstreams.push_back({"127.0.0.1", upstreamPort});
    route.connectTimeout = 250ms;
    route.responseTimeout = responseTimeout;
    route.maxConnectionsPerWorker = maxConnections;
    route.maxIdlePerWorker = maxConnections;
    config.routes.push_back(std::move(route));
    return config;
}

int tryConnect(std::uint16_t port)
{
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    CHECK(fd >= 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    if (::connect(
            fd, reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)) != 0) {
        CHECK_EQ(::close(fd), 0);
        return -1;
    }
    return fd;
}

void sendAll(int fd, std::string_view bytes)
{
    while (!bytes.empty()) {
        const auto count = ::send(
            fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
        if (count > 0) {
            bytes.remove_prefix(static_cast<std::size_t>(count));
        } else if (count < 0 && errno == EINTR) {
            continue;
        } else {
            CHECK(false);
        }
    }
}

struct ClientResult {
    std::string bytes;
    bool closed{false};
    bool eof{false};
    int terminalError{0};
};

ClientResult readUntilClose(int fd, std::chrono::milliseconds timeout = 2s)
{
    ClientResult result;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        pollfd descriptor{fd, POLLIN | POLLHUP, 0};
        const int ready = ::poll(&descriptor, 1, 10);
        if (ready < 0 && errno == EINTR) {
            continue;
        }
        CHECK(ready >= 0);
        if (ready == 0) {
            continue;
        }
        char buffer[4096];
        const auto count = ::recv(fd, buffer, sizeof(buffer), 0);
        if (count > 0) {
            result.bytes.append(buffer, static_cast<std::size_t>(count));
        } else if (count == 0) {
            result.closed = true;
            result.eof = true;
            return result;
        } else if (errno != EINTR && errno != EAGAIN
                   && errno != EWOULDBLOCK) {
            result.closed = true;
            result.terminalError = errno;
            return result;
        }
    }
    return result;
}

std::size_t statusLineCount(std::string_view response)
{
    std::size_t count = 0;
    std::size_t cursor = 0;
    while ((cursor = response.find("HTTP/1.1 ", cursor))
           != std::string_view::npos) {
        ++count;
        cursor += std::strlen("HTTP/1.1 ");
    }
    return count;
}

class RunningGateway {
public:
    explicit RunningGateway(proxy::GatewayConfig config)
        : config_(std::move(config))
        , baseThread_(config_.eventLoopOptions)
        , baseLoop_(baseThread_.startLoop())
        , server_(*baseLoop_, config_)
    {
        server_.start();
    }

    ~RunningGateway()
    {
        stop();
    }

    int connectClient()
    {
        int client = -1;
        CHECK(TestSupport::waitUntil([&] {
            client = tryConnect(config_.listenPort);
            return client >= 0;
        }, 2s));
        return client;
    }

    void stop(std::chrono::milliseconds grace = 0ms)
    {
        if (!stopped_) {
            server_.stop(grace);
            stopped_ = true;
        }
    }

    std::size_t activeSessionCount() const noexcept
    {
        return server_.activeSessionCount();
    }

    proxy::GatewayMetricsSnapshot metrics() const
    {
        return server_.metrics();
    }

private:
    proxy::GatewayConfig config_;
    EventLoopThread baseThread_;
    EventLoop* baseLoop_;
    proxy::GatewayServer server_;
    bool stopped_{false};
};

void expectStatus(
    std::uint16_t upstreamPort,
    std::string request,
    int expectedStatus,
    std::chrono::milliseconds responseTimeout = 1s)
{
    RunningGateway gateway(makeConfig(upstreamPort, responseTimeout));
    const int client = gateway.connectClient();
    sendAll(client, request);
    const auto response = readUntilClose(client);
    CHECK(response.closed);
    CHECK_EQ(statusLineCount(response.bytes), 1U);
    CHECK(response.bytes.starts_with(
        "HTTP/1.1 " + std::to_string(expectedStatus) + ' '));
    CHECK_EQ(::close(client), 0);
    gateway.stop(500ms);
}

void requestFailureScenarios()
{
    FaultHttpUpstream upstream(FaultHttpUpstream::Mode::normal);
    expectStatus(
        upstream.port(),
        "BROKEN\r\nHost: x\r\nContent-Length: 0\r\n\r\n", 400);

    std::string oversizedHeader =
        "GET /api/x HTTP/1.1\r\nX-Fill: ";
    constexpr std::size_t oversizedHeaderBytes = 16 * 1024 + 1;
    oversizedHeader.append(
        oversizedHeaderBytes - oversizedHeader.size() - 4, 'x');
    oversizedHeader += "\r\n\r\n";
    CHECK_EQ(oversizedHeader.size(), oversizedHeaderBytes);
    expectStatus(upstream.port(), std::move(oversizedHeader), 431);

    expectStatus(
        upstream.port(),
        "POST /api/x HTTP/1.1\r\nHost: x\r\n"
        "Content-Length: 1048577\r\n\r\n",
        413);
    expectStatus(
        upstream.port(),
        "GET /missing HTTP/1.1\r\nHost: x\r\n"
        "Content-Length: 0\r\n\r\n",
        404);
    CHECK_EQ(upstream.accepts(), 0U);
}

void upstreamFailureScenarios()
{
    BoundRefusedPort refused;
    expectStatus(
        refused.port(),
        "GET /api/x HTTP/1.1\r\nHost: x\r\n"
        "Content-Length: 0\r\n\r\n",
        502);

    {
        FaultHttpUpstream malformed(
            FaultHttpUpstream::Mode::malformedResponse);
        expectStatus(
            malformed.port(),
            "GET /api/x HTTP/1.1\r\nHost: x\r\n"
            "Content-Length: 0\r\n\r\n",
            502);
    }
    {
        FaultHttpUpstream delayed(
            FaultHttpUpstream::Mode::delayedResponse, 500ms);
        expectStatus(
            delayed.port(),
            "GET /api/x HTTP/1.1\r\nHost: x\r\n"
            "Content-Length: 0\r\n\r\n",
            504, 100ms);
    }
}

void partialResponseDoesNotAppendError()
{
    FaultHttpUpstream upstream(FaultHttpUpstream::Mode::partialResponse);
    RunningGateway gateway(makeConfig(upstream.port()));
    const int client = gateway.connectClient();
    sendAll(
        client,
        "GET /api/x HTTP/1.1\r\nHost: x\r\n"
        "Content-Length: 0\r\n\r\n");
    const auto response = readUntilClose(client);
    CHECK(response.closed);
    CHECK(response.eof);
    CHECK(response.bytes.starts_with("HTTP/1.1 200 OK\r\n"));
    CHECK_EQ(statusLineCount(response.bytes), 1U);
    CHECK_EQ(::close(client), 0);
    gateway.stop(500ms);
}

void exhaustedPoolReturns503()
{
    FaultHttpUpstream upstream(FaultHttpUpstream::Mode::holdResponse);
    RunningGateway gateway(makeConfig(upstream.port(), 5s, 1));
    const std::string request =
        "GET /api/x HTTP/1.1\r\nHost: x\r\n"
        "Content-Length: 0\r\n\r\n";
    const int first = gateway.connectClient();
    sendAll(first, request);
    CHECK(TestSupport::waitUntil(
        [&] { return upstream.requests() == 1; }, 2s));

    const int second = gateway.connectClient();
    sendAll(second, request);
    const auto response = readUntilClose(second);
    CHECK(response.closed);
    CHECK(response.bytes.starts_with("HTTP/1.1 503 "));
    CHECK_EQ(statusLineCount(response.bytes), 1U);
    CHECK_EQ(::close(second), 0);
    CHECK_EQ(::close(first), 0);
    gateway.stop(0ms);
    CHECK_EQ(upstream.accepts(), 1U);
}

void downstreamCloseCancelsUpload()
{
    FaultHttpUpstream upstream(FaultHttpUpstream::Mode::stalledUpload);
    RunningGateway gateway(makeConfig(upstream.port(), 5s, 1));
    const int client = gateway.connectClient();
    std::string upload =
        "POST /api/upload HTTP/1.1\r\nHost: x\r\n"
        "Content-Length: 1048576\r\n\r\n";
    sendAll(client, upload);
    CHECK(TestSupport::waitUntil(
        [&] { return upstream.requests() == 1; }, 2s));
    CHECK(upstream.uploadedBodyBytes() < 1024U * 1024U);
    CHECK_EQ(::close(client), 0);
    CHECK(TestSupport::waitUntil(
        [&] { return gateway.metrics().cancellations >= 1; }, 2s));
    upstream.resumeUploadReads();
    CHECK(TestSupport::waitUntil(
        [&] { return upstream.peerClosed(); }, 2s));
    CHECK(TestSupport::waitUntil(
        [&] { return gateway.activeSessionCount() == 0; }, 2s));
    const auto snapshot = gateway.metrics();
    CHECK_EQ(snapshot.poolAcquisitions, 1U);
    CHECK_EQ(snapshot.poolActiveConnections, 0U);
    CHECK_EQ(snapshot.poolIdleConnections, 0U);
    CHECK_EQ(snapshot.cancellations, 1U);
    gateway.stop(500ms);
}

} // namespace

int main()
{
    requestFailureScenarios();
    upstreamFailureScenarios();
    partialResponseDoesNotAppendError();
    exhaustedPoolReturns503();
    downstreamCloseCancelsUpload();
    return 0;
}
