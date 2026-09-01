#include "EventLoopThread.hpp"
#include "TestSupport.hpp"
#include "ucp/proxy/GatewayServer.hpp"

#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <poll.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;
namespace proxy = ucp::proxy;

namespace {

std::size_t openFdCount()
{
    std::size_t count = 0;
    for ([[maybe_unused]] const auto& entry
         : std::filesystem::directory_iterator("/proc/self/fd")) {
        ++count;
    }
    return count;
}

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

int connectTo(std::uint16_t port)
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

void sendRequest(int fd)
{
    std::string_view request =
        "GET /api/value HTTP/1.1\r\n"
        "Host: gateway.test\r\n"
        "Content-Length: 0\r\n"
        "Connection: close\r\n\r\n";
    while (!request.empty()) {
        const auto count = ::send(
            fd, request.data(), request.size(), MSG_NOSIGNAL);
        CHECK(count > 0);
        request.remove_prefix(static_cast<std::size_t>(count));
    }
}

enum class ReadOutcome {
    complete,
    closed,
    timedOut
};

ReadOutcome readResponse(int fd, std::chrono::milliseconds timeout)
{
    std::string response;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        pollfd descriptor{fd, POLLIN, 0};
        const int ready = ::poll(&descriptor, 1, 10);
        if (ready < 0 && errno == EINTR) {
            continue;
        }
        CHECK(ready >= 0);
        if (ready == 0) {
            continue;
        }
        char buffer[1024];
        const auto count = ::recv(fd, buffer, sizeof(buffer), 0);
        if (count > 0) {
            response.append(buffer, static_cast<std::size_t>(count));
            if (response.find("\r\n\r\nok") != std::string::npos) {
                return ReadOutcome::complete;
            }
        } else if (count == 0) {
            return response.find("\r\n\r\nok") != std::string::npos
                ? ReadOutcome::complete : ReadOutcome::closed;
        } else if (errno != EINTR && errno != EAGAIN
                   && errno != EWOULDBLOCK) {
            return ReadOutcome::closed;
        }
    }
    return ReadOutcome::timedOut;
}

class DelayedUpstream {
public:
    explicit DelayedUpstream(std::chrono::milliseconds delay)
        : delay_(delay)
    {
        listener_ = ::socket(
            AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        CHECK(listener_ >= 0);
        int reuse = 1;
        CHECK_EQ(::setsockopt(
            listener_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)), 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        CHECK_EQ(::bind(
            listener_, reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)), 0);
        CHECK_EQ(::listen(listener_, 8), 0);
        socklen_t length = sizeof(address);
        CHECK_EQ(::getsockname(
            listener_, reinterpret_cast<sockaddr*>(&address), &length), 0);
        port_ = ntohs(address.sin_port);
        thread_ = std::thread([this] { run(); });
    }

    ~DelayedUpstream()
    {
        stop_.store(true, std::memory_order_release);
        thread_.join();
        CHECK_EQ(::close(listener_), 0);
    }

    std::uint16_t port() const noexcept { return port_; }
    std::size_t requests() const noexcept
    {
        return requests_.load(std::memory_order_acquire);
    }
    bool peerClosed() const noexcept
    {
        return peerClosed_.load(std::memory_order_acquire);
    }

private:
    void run()
    {
        int connection = -1;
        while (!stop_.load(std::memory_order_acquire)) {
            connection = ::accept4(
                listener_, nullptr, nullptr,
                SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (connection >= 0) {
                break;
            }
            CHECK(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR);
            std::this_thread::sleep_for(1ms);
        }
        if (connection < 0) {
            return;
        }

        std::string request;
        while (!stop_.load(std::memory_order_acquire)
               && request.find("\r\n\r\n") == std::string::npos) {
            char buffer[1024];
            const auto count = ::recv(connection, buffer, sizeof(buffer), 0);
            if (count > 0) {
                request.append(buffer, static_cast<std::size_t>(count));
            } else if (count == 0) {
                peerClosed_.store(true, std::memory_order_release);
                break;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK
                       || errno == EINTR) {
                std::this_thread::sleep_for(1ms);
            } else {
                break;
            }
        }
        if (request.find("\r\n\r\n") != std::string::npos) {
            requests_.fetch_add(1, std::memory_order_release);
        }

        const auto sendAt = std::chrono::steady_clock::now() + delay_;
        while (!stop_.load(std::memory_order_acquire)
               && std::chrono::steady_clock::now() < sendAt) {
            char byte;
            const auto count = ::recv(
                connection, &byte, sizeof(byte), MSG_PEEK | MSG_DONTWAIT);
            if (count == 0) {
                peerClosed_.store(true, std::memory_order_release);
                break;
            }
            std::this_thread::sleep_for(1ms);
        }
        if (!peerClosed_.load(std::memory_order_acquire)) {
            std::string_view response =
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n"
                "Connection: close\r\n\r\nok";
            while (!response.empty()) {
                const auto count = ::send(
                    connection, response.data(), response.size(), MSG_NOSIGNAL);
                if (count > 0) {
                    response.remove_prefix(static_cast<std::size_t>(count));
                } else if (count < 0 && (errno == EAGAIN
                           || errno == EWOULDBLOCK || errno == EINTR)) {
                    std::this_thread::sleep_for(1ms);
                } else {
                    peerClosed_.store(true, std::memory_order_release);
                    break;
                }
            }
        }
        CHECK_EQ(::close(connection), 0);
    }

    int listener_{-1};
    std::uint16_t port_{0};
    std::chrono::milliseconds delay_;
    std::atomic_bool stop_{false};
    std::atomic_bool peerClosed_{false};
    std::atomic_size_t requests_{0};
    std::thread thread_;
};

proxy::GatewayConfig gatewayConfig(
    std::uint16_t listenPort, std::uint16_t upstreamPort)
{
    proxy::GatewayConfig config;
    config.listenIp = "127.0.0.1";
    config.listenPort = listenPort;
    config.workerCount = 1;
    config.eventLoopOptions.ringEntries = 128;
    config.eventLoopOptions.sqpoll = false;
    config.eventLoopOptions.registeredBuffersCount = 0;
    config.eventLoopOptions.pendingQueueCapacity = 128;
    config.eventLoopOptions.pendingSubmissionCapacity = 128;
    proxy::Route route;
    route.name = "api";
    route.prefix = "/api/";
    route.upstreams.push_back({"127.0.0.1", upstreamPort});
    route.connectTimeout = 250ms;
    route.responseTimeout = 3s;
    route.maxConnectionsPerWorker = 4;
    route.maxIdlePerWorker = 2;
    config.routes.push_back(std::move(route));
    return config;
}

void runShutdownCase(
    std::chrono::milliseconds responseDelay, bool expectResponse)
{
    DelayedUpstream upstream(responseDelay);
    const auto caseFdBaseline = openFdCount();
    const auto gatewayPort = reservePort();
    auto config = gatewayConfig(gatewayPort, upstream.port());
    EventLoopThread baseThread(config.eventLoopOptions);
    EventLoop* baseLoop = baseThread.startLoop();
    proxy::GatewayServer gateway(*baseLoop, std::move(config));
    gateway.start();

    int client = -1;
    CHECK(TestSupport::waitUntil([&] {
        client = connectTo(gatewayPort);
        return client >= 0;
    }, 2s));
    sendRequest(client);
    CHECK(TestSupport::waitUntil(
        [&] { return upstream.requests() == 1; }, 2s));

    std::atomic_int64_t stopMilliseconds{-1};
    std::thread stopper([&] {
        const auto started = std::chrono::steady_clock::now();
        gateway.stop(500ms);
        stopMilliseconds.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - started).count(),
            std::memory_order_release);
    });
    CHECK(TestSupport::waitUntil(
        [&] {
            const int rejected = connectTo(gatewayPort);
            if (rejected >= 0) {
                CHECK_EQ(::close(rejected), 0);
                return false;
            }
            return true;
        }, 1s));

    const auto outcome = readResponse(client, 2s);
    CHECK(outcome != ReadOutcome::timedOut);
    CHECK_EQ(outcome == ReadOutcome::complete, expectResponse);
    CHECK_EQ(::close(client), 0);
    stopper.join();
    gateway.stop(1ms);
    const auto elapsed = stopMilliseconds.load(std::memory_order_acquire);
    CHECK(elapsed >= 0);
    CHECK(elapsed < 1500);

    CHECK_EQ(gateway.activeSessionCount(), 0U);
    const auto snapshot = gateway.metrics();
    CHECK_EQ(snapshot.connectionsAccepted, snapshot.connectionsClosed);
    CHECK_EQ(snapshot.activeConnections, 0U);
    CHECK_EQ(snapshot.poolActiveConnections, 0U);
    CHECK_EQ(snapshot.poolIdleConnections, 0U);
    CHECK_EQ(snapshot.requests, 1U);
    CHECK(snapshot.bytesFromClients > 0);
    if (!expectResponse) {
        CHECK(TestSupport::waitUntil([&] { return upstream.peerClosed(); }, 1s));
        CHECK(snapshot.cancellations >= 1U);
    } else {
        CHECK_EQ(snapshot.status2xx, 1U);
        CHECK(snapshot.bytesToClients > 0);
        std::uint64_t latencySamples = 0;
        for (const auto samples : snapshot.latencyBuckets) {
            latencySamples += samples;
        }
        CHECK_EQ(latencySamples, 1U);
        CHECK_EQ(snapshot.poolAcquisitions, 1U);
    }
    CHECK(TestSupport::waitUntil(
        [&] { return openFdCount() <= caseFdBaseline + 1; }, 1s));
}

void verifySignalShutdown()
{
    char configPath[] = "/tmp/ucp-gateway-signal-XXXXXX";
    const int configFd = ::mkstemp(configPath);
    CHECK(configFd >= 0);
    const auto gatewayPort = reservePort();
    const auto upstreamPort = reservePort();
    const std::string contents =
        "[gateway]\n"
        "listen_ip = 127.0.0.1\n"
        "listen_port = " + std::to_string(gatewayPort) + "\n"
        "workers = 1\n"
        "header_limit = 16384\n"
        "body_limit = 1048576\n"
        "graceful_shutdown_ms = 500\n"
        "[event_loop]\n"
        "ring_entries = 128\n"
        "sqpoll = false\n"
        "registered_buffers_count = 0\n"
        "pending_queue_capacity = 128\n"
        "pending_submission_capacity = 128\n"
        "[route.api]\n"
        "prefix = /api/\n"
        "upstreams = 127.0.0.1:" + std::to_string(upstreamPort) + "\n"
        "connect_timeout_ms = 250\n"
        "response_timeout_ms = 1000\n"
        "max_connections_per_worker = 2\n"
        "max_idle_per_worker = 1\n"
        "idle_timeout_ms = 1000\n";
    std::string_view remaining = contents;
    while (!remaining.empty()) {
        const auto written = ::write(
            configFd, remaining.data(), remaining.size());
        CHECK(written > 0);
        remaining.remove_prefix(static_cast<std::size_t>(written));
    }
    CHECK_EQ(::close(configFd), 0);

    const auto executable = std::filesystem::read_symlink("/proc/self/exe")
        .parent_path() / "ucp_gateway";
    const pid_t child = ::fork();
    CHECK(child >= 0);
    if (child == 0) {
        ::execl(
            executable.c_str(), executable.c_str(), configPath,
            static_cast<char*>(nullptr));
        ::_exit(127);
    }

    std::this_thread::sleep_for(150ms);
    CHECK_EQ(::kill(child, SIGTERM), 0);
    int status = 0;
    const bool exited = TestSupport::waitUntil([&] {
        const auto result = ::waitpid(child, &status, WNOHANG);
        CHECK(result >= 0);
        return result == child;
    }, 3s, 10ms);
    if (!exited) {
        (void)::kill(child, SIGKILL);
        (void)::waitpid(child, &status, 0);
    }
    CHECK_EQ(::unlink(configPath), 0);
    CHECK(exited);
    CHECK(WIFEXITED(status));
    CHECK_EQ(WEXITSTATUS(status), 0);
}

} // namespace

int main()
{
    const auto before = openFdCount();
    runShutdownCase(100ms, true);
    runShutdownCase(1200ms, false);
    verifySignalShutdown();
    CHECK(TestSupport::waitUntil(
        [&] { return openFdCount() <= before + 2; }, 2s));
    return 0;
}
