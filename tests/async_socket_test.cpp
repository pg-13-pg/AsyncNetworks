#include "EventLoopThread.hpp"
#include "TestSupport.hpp"
#include "TcpConnection.hpp"
#include "ucp/net/AsyncSocket.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <memory>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;

ucp::DetachedTask exerciseSocket(
    const std::shared_ptr<TcpConnection>& connection,
    std::atomic_bool& timeoutObserved,
    std::atomic_bool& cancellationPending,
    std::atomic_bool& completed)
{
    std::array<std::byte, 16> input{};

    auto expired = co_await ucp::asyncReadSome(
        connection, input, std::chrono::steady_clock::now() - 1ms);
    CHECK(!expired);
    CHECK_EQ(expired.error().code, ucp::ErrorCode::timedOut);

    auto timedOut = co_await ucp::asyncReadSome(
        connection, input, std::chrono::steady_clock::now() + 50ms);
    CHECK(!timedOut);
    CHECK_EQ(timedOut.error().code, ucp::ErrorCode::timedOut);
    timeoutObserved.store(true, std::memory_order_release);

    auto read = co_await ucp::asyncReadSome(
        connection, input, std::nullopt);
    CHECK(read);
    CHECK_EQ(read.value(), 5U);
    CHECK(std::memcmp(input.data(), "hello", 5) == 0);

    std::vector<std::byte> payload(256 * 1024, std::byte{'x'});
    auto write = co_await ucp::asyncWriteAll(
        connection, payload, std::nullopt);
    CHECK(write);
    CHECK_EQ(write.value(), payload.size());

    cancellationPending.store(true, std::memory_order_release);
    auto cancelled = co_await ucp::asyncReadSome(
        connection, input, std::nullopt);
    CHECK(!cancelled);
    CHECK_EQ(cancelled.error().code, ucp::ErrorCode::cancelled);
    completed.store(true, std::memory_order_release);
}

int main()
{
    int sockets[2];
    CHECK_EQ(::socketpair(
        AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);

    int smallBuffer = 4096;
    CHECK_EQ(::setsockopt(
        sockets[0], SOL_SOCKET, SO_SNDBUF,
        &smallBuffer, sizeof(smallBuffer)), 0);
    CHECK_EQ(::setsockopt(
        sockets[1], SOL_SOCKET, SO_RCVBUF,
        &smallBuffer, sizeof(smallBuffer)), 0);

    EventLoop::Options options;
    options.ringEntries = 64;
    options.sqpoll = false;
    options.registeredBuffersCount = 0;
    EventLoopThread thread(options);
    EventLoop* loop = thread.startLoop();

    auto connection = std::make_shared<TcpConnection>(
        "async-socket-test", loop, sockets[0], InetAddress{});
    std::atomic_bool timeoutObserved{false};
    std::atomic_bool cancellationPending{false};
    std::atomic_bool completed{false};
    std::atomic_size_t peerBytes{0};

    std::thread peer([&] {
        CHECK(TestSupport::waitUntil(
            [&] { return timeoutObserved.load(std::memory_order_acquire); },
            2s));
        const char hello[] = "hello";
        std::size_t sent = 0;
        while (sent < 5) {
            const auto count = ::write(sockets[1], hello + sent, 5 - sent);
            if (count > 0) {
                sent += static_cast<std::size_t>(count);
            } else if (count < 0 && errno != EAGAIN && errno != EINTR) {
                CHECK(false);
            } else {
                std::this_thread::sleep_for(1ms);
            }
        }

        std::array<std::byte, 2048> chunk{};
        while (peerBytes.load(std::memory_order_relaxed) < 256 * 1024) {
            const auto count = ::read(sockets[1], chunk.data(), chunk.size());
            if (count > 0) {
                peerBytes.fetch_add(
                    static_cast<std::size_t>(count),
                    std::memory_order_relaxed);
            } else if (count < 0 && errno != EAGAIN && errno != EINTR) {
                CHECK(false);
            } else {
                std::this_thread::sleep_for(1ms);
            }
        }
    });

    loop->queueControlInLoop([&] {
        connection->connectEstablished();
        exerciseSocket(
            connection, timeoutObserved, cancellationPending, completed);
    });

    CHECK(TestSupport::waitUntil(
        [&] { return cancellationPending.load(std::memory_order_acquire); },
        5s));
    loop->queueControlInLoop([connection] {
        connection->cancelPendingOperations();
    });
    CHECK(TestSupport::waitUntil(
        [&] { return completed.load(std::memory_order_acquire); }, 5s));
    peer.join();
    CHECK_EQ(peerBytes.load(std::memory_order_relaxed), 256U * 1024U);

    std::atomic_bool destroyed{false};
    loop->queueControlInLoop([connection, &destroyed] {
        connection->connectDestroyed();
        destroyed.store(true, std::memory_order_release);
    });
    CHECK(TestSupport::waitUntil(
        [&] { return destroyed.load(std::memory_order_acquire); }, 2s));
    loop->quit();
    ::close(sockets[1]);
    return 0;
}
