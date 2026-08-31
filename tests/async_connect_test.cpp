#include "EventLoopThread.hpp"
#include "InetAddress.hpp"
#include "TestSupport.hpp"
#include "TcpConnection.hpp"
#include "ucp/net/AsyncConnect.hpp"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <memory>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;

ucp::DetachedTask connectSuccessfully(
    EventLoop& loop,
    std::uint16_t port,
    std::atomic_bool& completed)
{
    auto connected = co_await ucp::asyncConnect(
        loop, InetAddress(port, "127.0.0.1"),
        std::chrono::steady_clock::now() + 500ms,
        "connect-test");
    CHECK(connected);
    CHECK(connected.value()->isConnected());
    connected.value()->connectDestroyed();
    completed.store(true, std::memory_order_release);
}

ucp::DetachedTask connectToReleasedPort(
    EventLoop& loop,
    std::uint16_t port,
    std::atomic_bool& completed)
{
    auto refused = co_await ucp::asyncConnect(
        loop, InetAddress(port, "127.0.0.1"),
        std::chrono::steady_clock::now() + 500ms,
        "refused-connect-test");
    CHECK(!refused);
    CHECK(refused.error().code == ucp::ErrorCode::system
          || refused.error().code == ucp::ErrorCode::connectionReset);
    completed.store(true, std::memory_order_release);
}

int main()
{
    const int listener = ::socket(
        AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    CHECK(listener >= 0);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    CHECK_EQ(::bind(
        listener, reinterpret_cast<const sockaddr*>(&address),
        sizeof(address)), 0);
    CHECK_EQ(::listen(listener, 1), 0);

    socklen_t addressLength = sizeof(address);
    CHECK_EQ(::getsockname(
        listener, reinterpret_cast<sockaddr*>(&address),
        &addressLength), 0);
    const auto port = ntohs(address.sin_port);

    std::thread accepter([listener] {
        while (true) {
            const int accepted = ::accept4(
                listener, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (accepted >= 0) {
                CHECK_EQ(::close(accepted), 0);
                return;
            }
            CHECK(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR);
            std::this_thread::sleep_for(1ms);
        }
    });

    EventLoop::Options options;
    options.ringEntries = 64;
    options.sqpoll = false;
    options.registeredBuffersCount = 0;
    EventLoopThread thread(options);
    EventLoop* loop = thread.startLoop();

    std::atomic_bool connected{false};
    loop->queueControlInLoop([&] {
        connectSuccessfully(*loop, port, connected);
    });
    CHECK(TestSupport::waitUntil(
        [&] { return connected.load(std::memory_order_acquire); }, 2s));
    accepter.join();
    CHECK_EQ(::close(listener), 0);

    std::atomic_bool refused{false};
    loop->queueControlInLoop([&] {
        connectToReleasedPort(*loop, port, refused);
    });
    CHECK(TestSupport::waitUntil(
        [&] { return refused.load(std::memory_order_acquire); }, 2s));

    loop->quit();
    return 0;
}
