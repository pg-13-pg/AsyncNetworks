#include "Buffer.hpp"
#include "Config.hpp"
#include "EventLoop.hpp"
#include "InetAddress.hpp"
#include "TcpConnection.hpp"
#include "TcpServer.hpp"
#include "ucp/net/AsyncSocket.hpp"
#include "ucp/proxy/HttpFramer.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

using namespace std::chrono_literals;
namespace proxy = ucp::proxy;

namespace {

constexpr std::size_t maxHeaderBytes = 16 * 1024;
constexpr std::array<std::size_t, 3> payloadSizes{1024, 4096, 16384};

std::span<const std::byte> bytesOf(const std::string& value)
{
    return {
        reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

std::string makeResponse(std::size_t payloadSize, bool keepAlive)
{
    std::string response =
        "HTTP/1.1 200 OK\r\nContent-Length: "
        + std::to_string(payloadSize)
        + "\r\nConnection: "
        + (keepAlive ? "keep-alive" : "close")
        + "\r\nContent-Type: application/octet-stream\r\n\r\n";
    response.append(payloadSize, 'x');
    return response;
}

const std::string& responseFor(std::size_t index, bool keepAlive)
{
    static const std::array<std::string, 3> keepAliveResponses{
        makeResponse(payloadSizes[0], true),
        makeResponse(payloadSizes[1], true),
        makeResponse(payloadSizes[2], true)};
    static const std::array<std::string, 3> closeResponses{
        makeResponse(payloadSizes[0], false),
        makeResponse(payloadSizes[1], false),
        makeResponse(payloadSizes[2], false)};
    return keepAlive ? keepAliveResponses[index] : closeResponses[index];
}

std::optional<std::size_t> benchmarkPathIndex(std::string_view path)
{
    if (path == "/bytes/1024" || path == "/api/bytes/1024") {
        return 0;
    }
    if (path == "/bytes/4096" || path == "/api/bytes/4096") {
        return 1;
    }
    if (path == "/bytes/16384" || path == "/api/bytes/16384") {
        return 2;
    }
    return std::nullopt;
}

ucp::Task<proxy::RequestParseResult> readRequestHead(
    const std::shared_ptr<TcpConnection>& connection,
    Buffer& input,
    std::array<std::byte, 4096>& scratch)
{
    const proxy::HttpLimits limits{maxHeaderBytes, 0};
    while (true) {
        auto parsed = proxy::parseRequestHead(input, limits);
        if (parsed.status != proxy::ParseStatus::needMore) {
            co_return parsed;
        }

        const auto buffered = input.readableBytes();
        if (buffered >= maxHeaderBytes) {
            co_return proxy::RequestParseResult{
                proxy::ParseStatus::error,
                proxy::HttpParseError::headerTooLarge,
                {}};
        }
        const auto available = std::min(
            scratch.size(), maxHeaderBytes - buffered);
        auto read = co_await ucp::asyncReadSome(
            connection, std::span(scratch).first(available));
        if (!read) {
            co_return proxy::RequestParseResult{
                proxy::ParseStatus::error,
                proxy::HttpParseError::badSyntax,
                {}};
        }
        input.append(
            reinterpret_cast<const char*>(scratch.data()), read.value());
    }
}

ucp::Task<void> writeAndClose(
    const std::shared_ptr<TcpConnection>& connection,
    const std::string& response)
{
    (void)co_await ucp::asyncWriteAll(
        connection, bytesOf(response),
        std::chrono::steady_clock::now() + 1s);
    co_return;
}

ucp::DetachedTask serveConnection(
    std::shared_ptr<TcpConnection> connection)
{
    static const std::string badRequest =
        "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n"
        "Connection: close\r\n\r\n";
    static const std::string methodNotAllowed =
        "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n"
        "Connection: close\r\n\r\n";
    static const std::string notFound =
        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n"
        "Connection: close\r\n\r\n";

    Buffer input(maxHeaderBytes);
    std::array<std::byte, 4096> scratch{};
    while (connection->isConnected()) {
        auto parsed = co_await readRequestHead(connection, input, scratch);
        if (parsed.status != proxy::ParseStatus::complete) {
            co_await writeAndClose(connection, badRequest);
            break;
        }

        auto request = std::move(parsed.request);
        if (request.method != "GET" || request.contentLength != 0) {
            co_await writeAndClose(connection, methodNotAllowed);
            break;
        }
        const auto index = benchmarkPathIndex(request.path);
        if (!index) {
            co_await writeAndClose(connection, notFound);
            break;
        }

        auto written = co_await ucp::asyncWriteAll(
            connection, bytesOf(responseFor(*index, request.keepAlive)),
            std::chrono::steady_clock::now() + 5s);
        if (!written || !request.keepAlive) {
            break;
        }
    }

    if (connection->isConnected()) {
        connection->forceClose();
    }
}

bool parseArguments(
    int argc,
    char** argv,
    std::string& host,
    std::uint16_t& port,
    int& workers,
    std::string& configPath,
    bool& hostOverride,
    bool& portOverride,
    bool& workersOverride)
{
    if (argc < 2 || (argc - 1) % 2 != 0) {
        return false;
    }
    host = "127.0.0.1";
    port = 0;
    workers = 0;
    configPath.clear();
    hostOverride = false;
    portOverride = false;
    workersOverride = false;
    int index = 1;
    while (index < argc) {
        if (std::string_view(argv[index]) == "--config" && index + 1 < argc) {
            configPath = argv[index + 1];
            if (configPath.empty()) {
                return false;
            }
            index += 2;
            continue;
        }
        if (std::string_view(argv[index]) == "--host" && index + 1 < argc) {
            host = argv[index + 1];
            if (host.empty()) {
                return false;
            }
            hostOverride = true;
            index += 2;
            continue;
        }
        if (std::string_view(argv[index]) == "--port" && index + 1 < argc) {
            unsigned value = 0;
            const std::string_view argument(argv[index + 1]);
            const auto [end, error] = std::from_chars(
                argument.data(), argument.data() + argument.size(), value);
            if (error != std::errc{} || end != argument.data() + argument.size()
                || value == 0 || value > 65535 || portOverride) {
                return false;
            }
            port = static_cast<std::uint16_t>(value);
            portOverride = true;
            index += 2;
            continue;
        }
        if (std::string_view(argv[index]) == "--workers" && index + 1 < argc) {
            unsigned value = 0;
            const std::string_view argument(argv[index + 1]);
            const auto [end, error] = std::from_chars(
                argument.data(), argument.data() + argument.size(), value);
            if (error != std::errc{} || end != argument.data() + argument.size()
                || value > 1024) {
                return false;
            }
            workers = static_cast<int>(value);
            workersOverride = true;
            index += 2;
            continue;
        }
        return false;
    }
    return !configPath.empty() || portOverride;
}

void printUsage(const char* program)
{
    std::fprintf(
        stderr,
        "Usage: %s --config FILE [--host ADDRESS] [--port PORT] [--workers COUNT]\n"
        "       %s [--host ADDRESS] [--workers COUNT] --port PORT\n",
        program, program);
}

} // namespace

int main(int argc, char** argv)
{
    std::string host;
    std::uint16_t port = 0;
    int workers = 0;
    std::string configPath;
    bool hostOverride = false;
    bool portOverride = false;
    bool workersOverride = false;
    if (!parseArguments(argc, argv, host, port, workers, configPath,
                        hostOverride, portOverride, workersOverride)) {
        printUsage(argv[0]);
        return 2;
    }

    if (!configPath.empty()) {
        Config config;
        std::string configError;
        if (!config.loadFromFile(configPath, &configError)) {
            std::fprintf(stderr, "gateway_mock_upstream: %s\n",
                         configError.c_str());
            return 1;
        }
        if (!hostOverride) {
            host = config.getString("server.host", host);
        }
        if (!portOverride) {
            const int configuredPort = config.getInt("server.port", 0);
            if (configuredPort <= 0 || configuredPort > 65535) {
                std::fprintf(stderr,
                             "gateway_mock_upstream: invalid server.port\n");
                return 1;
            }
            port = static_cast<std::uint16_t>(configuredPort);
        }
        if (!workersOverride) {
            workers = config.getInt("server.workers", workers);
        }

        EventLoop::Options options;
        options.ringEntries = config.getSizeT(
            "event_loop.ring_entries", options.ringEntries);
        options.sqpoll = config.getBool("event_loop.sqpoll", options.sqpoll);
        options.sqpollIdleMs = static_cast<unsigned int>(config.getSizeT(
            "event_loop.sqpoll_idle_ms", options.sqpollIdleMs));
        options.registeredBuffersCount = config.getSizeT(
            "event_loop.registered_buffers_count", options.registeredBuffersCount);
        options.registeredBuffersSize = config.getSizeT(
            "event_loop.registered_buffer_size", options.registeredBuffersSize);
        options.pendingQueueCapacity = config.getSizeT(
            "event_loop.pending_queue_capacity", options.pendingQueueCapacity);
        options.pendingSubmissionCapacity = config.getSizeT(
            "event_loop.pending_submission_capacity", options.pendingSubmissionCapacity);
        options.pendingQueueHighWaterMark = config.getSizeT(
            "event_loop.pending_queue_high_water_mark", options.pendingQueueHighWaterMark);
        options.pendingQueueLowWaterMark = config.getSizeT(
            "event_loop.pending_queue_low_water_mark", options.pendingQueueLowWaterMark);
        options.enableQueueFullStats = config.getBool(
            "event_loop.enable_queue_full_stats", options.enableQueueFullStats);

        // Keep the accept loop and every worker on the same configured path.
        EventLoop::Options configuredOptions = options;
        sigset_t signals;
        ::sigemptyset(&signals);
        ::sigaddset(&signals, SIGINT);
        ::sigaddset(&signals, SIGTERM);
        if (::pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0) {
            std::fputs("gateway_mock_upstream: failed to block signals\n", stderr);
            return 1;
        }
        EventLoop loop(configuredOptions);
        if (!loop.initRegisteredBuffers()) {
            std::fputs("gateway_mock_upstream: registered buffer initialization failed\n", stderr);
            return 1;
        }
        TcpServer server(&loop, InetAddress(port, host), "gateway-mock-upstream");
        server.setEventLoopOptions(configuredOptions);
        server.setThreadNum(workers);
        std::atomic_bool stopping{false};
        server.setConnectionCallback([](const std::shared_ptr<TcpConnection>& connection) {
            serveConnection(connection);
        });
        server.setConnectionDestroyedCallback([&] {
            if (stopping.load(std::memory_order_acquire) && server.connectionCount() == 0) {
                loop.quit();
            }
        });
        server.start();
        std::thread signalThread([&] {
            int signal = 0;
            if (::sigwait(&signals, &signal) != 0) return;
            stopping.store(true, std::memory_order_release);
            loop.queueControlInLoop([&] {
                server.stopAccepting();
                server.forEachConnection([](const std::shared_ptr<TcpConnection>& connection) {
                    connection->forceClose();
                });
                if (server.connectionCount() == 0) loop.quit();
            });
        });
        std::printf("gateway_mock_upstream listening on %s:%u\n", host.c_str(), port);
        std::fflush(stdout);
        loop.loop();
        signalThread.join();
        return 0;
    }

    sigset_t signals;
    ::sigemptyset(&signals);
    ::sigaddset(&signals, SIGINT);
    ::sigaddset(&signals, SIGTERM);
    if (::pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0) {
        std::fputs("gateway_mock_upstream: failed to block signals\n", stderr);
        return 1;
    }

    EventLoop::Options options;
    options.ringEntries = 128;
    options.sqpoll = false;
    options.registeredBuffersCount = 0;
    EventLoop loop(options);
    if (!loop.initRegisteredBuffers()) {
        std::fputs(
            "gateway_mock_upstream: registered buffer initialization failed\n",
            stderr);
        return 1;
    }

    TcpServer server(
        &loop, InetAddress(port, host), "gateway-mock-upstream");
    server.setThreadNum(workers);
    std::atomic_bool stopping{false};
    server.setConnectionCallback([](
        const std::shared_ptr<TcpConnection>& connection) {
        serveConnection(connection);
    });
    server.setConnectionDestroyedCallback([&] {
        if (stopping.load(std::memory_order_acquire)
            && server.connectionCount() == 0) {
            loop.quit();
        }
    });
    server.start();

    std::thread signalThread([&] {
        int signal = 0;
        if (::sigwait(&signals, &signal) != 0) {
            return;
        }
        stopping.store(true, std::memory_order_release);
        loop.queueControlInLoop([&] {
            server.stopAccepting();
            server.forEachConnection([](
                const std::shared_ptr<TcpConnection>& connection) {
                connection->forceClose();
            });
            if (server.connectionCount() == 0) {
                loop.quit();
            }
        });
    });

    std::printf("gateway_mock_upstream listening on %s:%u\n", host.c_str(), port);
    std::fflush(stdout);
    loop.loop();
    signalThread.join();
    return 0;
}
