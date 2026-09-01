#include "EventLoopThread.hpp"
#include "TestSupport.hpp"
#include "TcpConnection.hpp"
#include "ucp/proxy/UpstreamPool.hpp"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <memory>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
namespace proxy = ucp::proxy;

namespace {

class HoldingListener {
public:
    HoldingListener()
    {
        fd_ = ::socket(
            AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        CHECK(fd_ >= 0);

        int reuse = 1;
        CHECK_EQ(::setsockopt(
            fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)), 0);

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        CHECK_EQ(::bind(
            fd_, reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)), 0);
        CHECK_EQ(::listen(fd_, 16), 0);

        socklen_t length = sizeof(address);
        CHECK_EQ(::getsockname(
            fd_, reinterpret_cast<sockaddr*>(&address), &length), 0);
        port_ = ntohs(address.sin_port);

        thread_ = std::thread([this] { run(); });
    }

    ~HoldingListener()
    {
        stop_.store(true, std::memory_order_release);
        thread_.join();
        CHECK_EQ(::close(fd_), 0);
    }

    HoldingListener(const HoldingListener&) = delete;
    HoldingListener& operator=(const HoldingListener&) = delete;

    std::uint16_t port() const noexcept
    {
        return port_;
    }

    std::size_t accepts() const noexcept
    {
        return accepts_.load(std::memory_order_acquire);
    }

private:
    void run()
    {
        std::vector<int> accepted;
        while (!stop_.load(std::memory_order_acquire)) {
            const int connection = ::accept4(
                fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (connection >= 0) {
                accepted.push_back(connection);
                accepts_.fetch_add(1, std::memory_order_release);
                continue;
            }
            CHECK(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR);
            std::this_thread::sleep_for(1ms);
        }
        for (const int connection : accepted) {
            CHECK_EQ(::close(connection), 0);
        }
    }

    int fd_{-1};
    std::uint16_t port_{0};
    std::atomic_bool stop_{false};
    std::atomic_size_t accepts_{0};
    std::thread thread_;
};

ucp::DetachedTask acquireConcurrently(
    proxy::UpstreamPool& pool,
    const proxy::Route& route,
    const proxy::Endpoint& endpoint,
    std::atomic_size_t& succeeded,
    std::atomic_size_t& exhausted,
    std::atomic_size_t& finished)
{
    auto result = co_await pool.acquire(
        route, endpoint, std::chrono::steady_clock::now() + 500ms);
    if (result) {
        auto lease = std::move(result).takeValue();
        CHECK(lease.connection() != nullptr);
        succeeded.fetch_add(1, std::memory_order_release);
    } else {
        CHECK_EQ(result.error().code, ucp::ErrorCode::resourceExhausted);
        exhausted.fetch_add(1, std::memory_order_release);
    }
    finished.fetch_add(1, std::memory_order_release);
}

ucp::DetachedTask exercisePool(
    proxy::UpstreamPool& pool,
    const proxy::Route& route,
    const proxy::Endpoint& endpoint,
    std::atomic_bool& completed)
{
    std::shared_ptr<TcpConnection> firstConnection;
    std::shared_ptr<TcpConnection> discardedConnection;

    {
        auto firstResult = co_await pool.acquire(
            route, endpoint, std::chrono::steady_clock::now() + 500ms);
        CHECK(firstResult);
        auto first = std::move(firstResult).takeValue();
        firstConnection = first.connection();
        CHECK(firstConnection != nullptr);
        CHECK_EQ(pool.active(route, endpoint), 1U);
        CHECK_EQ(pool.idle(route, endpoint), 0U);
        first.markReusable();
    }
    CHECK_EQ(pool.active(route, endpoint), 0U);
    CHECK_EQ(pool.idle(route, endpoint), 1U);

    {
        auto reusedResult = co_await pool.acquire(
            route, endpoint, std::chrono::steady_clock::now() + 500ms);
        CHECK(reusedResult);
        auto reused = std::move(reusedResult).takeValue();
        CHECK(reused.connection() == firstConnection);
        CHECK_EQ(pool.active(route, endpoint), 1U);
        CHECK_EQ(pool.idle(route, endpoint), 0U);

        {
            auto heldResult = co_await pool.acquire(
                route, endpoint,
                std::chrono::steady_clock::now() + 500ms);
            CHECK(heldResult);
            auto held = std::move(heldResult).takeValue();
            discardedConnection = held.connection();
            CHECK_EQ(pool.active(route, endpoint), 2U);

            auto exhausted = co_await pool.acquire(
                route, endpoint,
                std::chrono::steady_clock::now() + 500ms);
            CHECK(!exhausted);
            CHECK_EQ(
                exhausted.error().code,
                ucp::ErrorCode::resourceExhausted);
        }

        CHECK_EQ(pool.active(route, endpoint), 1U);
        auto replacementResult = co_await pool.acquire(
            route, endpoint, std::chrono::steady_clock::now() + 500ms);
        CHECK(replacementResult);
        auto replacement = std::move(replacementResult).takeValue();
        CHECK(replacement.connection() != discardedConnection);
        CHECK_EQ(pool.active(route, endpoint), 2U);

        reused.markReusable();
        replacement.markReusable();
    }

    CHECK_EQ(pool.active(route, endpoint), 0U);
    CHECK_EQ(pool.idle(route, endpoint), 2U);
    pool.closeIdle();
    CHECK_EQ(pool.active(route, endpoint), 0U);
    CHECK_EQ(pool.idle(route, endpoint), 0U);
    CHECK(!firstConnection->isConnected());
    completed.store(true, std::memory_order_release);
}

ucp::DetachedTask verifyStoppedPoolRejectsAcquisition(
    proxy::UpstreamPool& pool,
    const proxy::Route& route,
    const proxy::Endpoint& endpoint,
    std::atomic_bool& completed)
{
    pool.stopAcquiring();
    auto rejected = co_await pool.acquire(
        route, endpoint, std::chrono::steady_clock::now() + 500ms);
    CHECK(!rejected);
    CHECK_EQ(rejected.error().code, ucp::ErrorCode::cancelled);
    completed.store(true, std::memory_order_release);
}

} // namespace

int main()
{
    HoldingListener listener;

    EventLoop::Options options;
    options.ringEntries = 64;
    options.sqpoll = false;
    options.registeredBuffersCount = 0;
    EventLoopThread thread(options);
    EventLoop* loop = thread.startLoop();

    proxy::Route route;
    route.name = "pool-test";
    route.prefix = "/";
    route.connectTimeout = 500ms;
    route.maxConnectionsPerWorker = 2;
    route.maxIdlePerWorker = 2;
    route.idleTimeout = 30s;
    route.upstreams.push_back({"127.0.0.1", listener.port()});
    const auto& endpoint = route.upstreams.front();

    proxy::Route concurrentRoute = route;
    concurrentRoute.name = "concurrent-pool-test";
    concurrentRoute.maxConnectionsPerWorker = 1;
    concurrentRoute.maxIdlePerWorker = 0;

    proxy::UpstreamPool pool(*loop);
    std::atomic_size_t concurrentSucceeded{0};
    std::atomic_size_t concurrentExhausted{0};
    std::atomic_size_t concurrentFinished{0};
    loop->queueControlInLoop([&] {
        acquireConcurrently(
            pool, concurrentRoute, endpoint,
            concurrentSucceeded, concurrentExhausted,
            concurrentFinished);
        acquireConcurrently(
            pool, concurrentRoute, endpoint,
            concurrentSucceeded, concurrentExhausted,
            concurrentFinished);
    });
    CHECK(TestSupport::waitUntil(
        [&] {
            return concurrentFinished.load(std::memory_order_acquire) == 2;
        },
        2s));
    CHECK_EQ(concurrentSucceeded.load(std::memory_order_acquire), 1U);
    CHECK_EQ(concurrentExhausted.load(std::memory_order_acquire), 1U);

    std::atomic_bool completed{false};
    loop->queueControlInLoop([&] {
        exercisePool(pool, route, endpoint, completed);
    });

    CHECK(TestSupport::waitUntil(
        [&] { return completed.load(std::memory_order_acquire); }, 5s));
    CHECK(TestSupport::waitUntil(
        [&] { return listener.accepts() == 4; }, 2s));

    std::atomic_bool stoppedPoolChecked{false};
    loop->queueControlInLoop([&] {
        verifyStoppedPoolRejectsAcquisition(
            pool, route, endpoint, stoppedPoolChecked);
    });
    CHECK(TestSupport::waitUntil(
        [&] {
            return stoppedPoolChecked.load(std::memory_order_acquire);
        }, 2s));
    loop->quit();
    return 0;
}
