#include "ucp/proxy/GatewayServer.hpp"

#include "EventLoop.hpp"
#include "InetAddress.hpp"
#include "Logger.hpp"
#include "TcpConnection.hpp"
#include "TcpServer.hpp"
#include "ucp/proxy/ProxySession.hpp"
#include "ucp/proxy/RouteTable.hpp"
#include "ucp/proxy/UpstreamPool.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ucp::proxy {
namespace {

struct GatewayWorker {
    explicit GatewayWorker(EventLoop& owner)
        : loop(owner), pool(owner, &metrics)
    {
    }

    EventLoop& loop;
    RoundRobinBalancer balancer;
    GatewayMetricShard metrics;
    UpstreamPool pool;
    std::unordered_map<ProxySession*, std::shared_ptr<ProxySession>> sessions;
};

void updateMaximum(std::atomic_uint64_t& target, std::uint64_t value)
{
    auto observed = target.load(std::memory_order_relaxed);
    while (observed < value
           && !target.compare_exchange_weak(
               observed, value, std::memory_order_relaxed)) {
    }
}

} // namespace

class GatewayServer::Impl {
public:
    Impl(EventLoop& baseLoop, GatewayConfig config)
        : baseLoop_(baseLoop)
        , config_(std::move(config))
        , routes_(config_.routes)
        , server_(std::make_unique<TcpServer>(
              &baseLoop_,
              InetAddress(config_.listenPort, config_.listenIp),
              "ucp-gateway"))
    {
    }

    ~Impl()
    {
        if (started_.load(std::memory_order_acquire)
            && !stopped_.load(std::memory_order_acquire)) {
            stop(config_.gracefulShutdown);
        }
    }

    void start()
    {
        bool expected = false;
        if (!started_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            return;
        }

        acceptingSessions_.store(true, std::memory_order_release);
        server_->setThreadNum(config_.workerCount);
        server_->setEventLoopOptions(config_.eventLoopOptions);
        server_->setThreadInitCallback([this](EventLoop* loop) {
            auto worker = std::make_unique<GatewayWorker>(*loop);
            GatewayWorker* workerPtr = worker.get();
            loop->setBackpressureCallback([workerPtr](bool high) {
                if (high) {
                    workerPtr->metrics.queueHighWaterEvents.fetch_add(
                        1, std::memory_order_relaxed);
                }
            });
            std::lock_guard<std::mutex> lock(workersMutex_);
            workers_.emplace(loop, std::move(worker));
        });
        server_->setConnectionCallback(
            [this](const std::shared_ptr<TcpConnection>& connection) {
                startSession(connection);
            });
        server_->start();
    }

    void stop(std::chrono::milliseconds gracePeriod)
    {
        if (!started_.load(std::memory_order_acquire)) {
            return;
        }
        {
            std::unique_lock<std::mutex> lock(shutdownMutex_);
            if (stopped_.load(std::memory_order_acquire)) {
                return;
            }
            if (stopping_) {
                shutdownCv_.wait(lock, [this] {
                    return stopped_.load(std::memory_order_acquire);
                });
                return;
            }
            if (baseLoop_.isInLoopThread()) {
                throw std::logic_error(
                    "GatewayServer::stop must be called off the base EventLoop");
            }
            stopping_ = true;
        }

        acceptingSessions_.store(false, std::memory_order_release);
        const auto deadline = std::chrono::steady_clock::now()
            + std::max(gracePeriod, std::chrono::milliseconds::zero());
        const auto coordinationDeadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);

        postBaseAndWait(
            [this] { server_->stopAccepting(); }, coordinationDeadline,
            "stop accepting");

        auto workers = workerSnapshot();
        runOnWorkersAndWait(
            workers,
            [](GatewayWorker& worker) { worker.pool.stopAcquiring(); },
            coordinationDeadline, "stop upstream acquisition");

        {
            std::unique_lock<std::mutex> lock(shutdownMutex_);
            shutdownCv_.wait_until(lock, deadline, [this] {
                return activeSessions_.load(std::memory_order_acquire) == 0;
            });
        }

        if (activeSessions_.load(std::memory_order_acquire) != 0) {
            const auto forcedDeadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(5);
            cancelSessions(workers, forcedDeadline);
            std::unique_lock<std::mutex> lock(shutdownMutex_);
            if (!shutdownCv_.wait_until(lock, forcedDeadline, [this] {
                    return activeSessions_.load(std::memory_order_acquire) == 0;
                })) {
                failStop("cancel active sessions");
            }
        }

        const auto forcedDeadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        postBaseAndWait(
            [this] {
                server_->forEachConnection(
                    [](const std::shared_ptr<TcpConnection>& connection) {
                        connection->forceClose();
                    });
            },
            forcedDeadline, "force downstream close");
        waitForConnectionsToClose(forcedDeadline);

        runOnWorkersAndWait(
            workers,
            [](GatewayWorker& worker) {
                worker.pool.closeIdle();
                const auto stats = worker.loop.getBackpressureStats();
                worker.metrics.queueRejections.store(
                    stats.queueFullCount, std::memory_order_relaxed);
                worker.metrics.queueHighWaterEvents.store(
                    stats.highWaterMarkEvents, std::memory_order_relaxed);
                updateMaximum(
                    worker.metrics.queueMaxDepth, stats.maxPendingQueueSize);
                worker.metrics.poolActiveConnections.store(
                    0, std::memory_order_relaxed);
                worker.metrics.poolIdleConnections.store(
                    0, std::memory_order_relaxed);
            },
            forcedDeadline, "close idle upstream connections");

        waitForOperationDrain(workers, forcedDeadline);
        {
            std::lock_guard<std::mutex> lock(workersMutex_);
            loopsStopped_ = true;
            for (GatewayWorker* worker : workers) {
                worker->loop.quit();
            }
        }

        waitForBaseOperationDrain(forcedDeadline);
        baseLoop_.quit();

        {
            std::lock_guard<std::mutex> lock(shutdownMutex_);
            stopped_.store(true, std::memory_order_release);
        }
        shutdownCv_.notify_all();
    }

    GatewayMetricsSnapshot metrics() const
    {
        GatewayMetricsSnapshot result;
        std::lock_guard<std::mutex> lock(workersMutex_);
        for (const auto& [loop, worker] : workers_) {
            (void)loop;
            auto workerSnapshot = worker->metrics.snapshot();
            if (!loopsStopped_) {
                const auto queue = worker->loop.getBackpressureStats();
                workerSnapshot.queueRejections = queue.queueFullCount;
                workerSnapshot.queueHighWaterEvents =
                    queue.highWaterMarkEvents;
                workerSnapshot.queueMaxDepth = queue.maxPendingQueueSize;
            }
            result += workerSnapshot;
        }
        return result;
    }

    std::size_t activeSessionCount() const noexcept
    {
        return activeSessions_.load(std::memory_order_acquire);
    }

private:
    void startSession(const std::shared_ptr<TcpConnection>& connection)
    {
        if (!acceptingSessions_.load(std::memory_order_acquire)) {
            connection->forceClose();
            return;
        }

        GatewayWorker* worker = nullptr;
        {
            std::lock_guard<std::mutex> lock(workersMutex_);
            const auto position = workers_.find(connection->getLoop());
            if (position != workers_.end()) {
                worker = position->second.get();
            }
        }
        if (!worker) {
            connection->forceClose();
            return;
        }

        worker->metrics.connectionsAccepted.fetch_add(
            1, std::memory_order_relaxed);
        worker->metrics.activeConnections.fetch_add(
            1, std::memory_order_relaxed);
        activeSessions_.fetch_add(1, std::memory_order_release);

        auto session = std::make_shared<ProxySession>(
            connection, routes_, worker->balancer, worker->pool,
            config_.httpLimits,
            [this, worker](const std::shared_ptr<ProxySession>& finished) {
                worker->sessions.erase(finished.get());
                worker->metrics.connectionsClosed.fetch_add(
                    1, std::memory_order_relaxed);
                worker->metrics.activeConnections.fetch_sub(
                    1, std::memory_order_relaxed);
                activeSessions_.fetch_sub(1, std::memory_order_release);
                shutdownCv_.notify_all();
            },
            &worker->metrics);
        worker->sessions.emplace(session.get(), session);
        session->run();
    }

    std::vector<GatewayWorker*> workerSnapshot() const
    {
        std::vector<GatewayWorker*> result;
        std::lock_guard<std::mutex> lock(workersMutex_);
        result.reserve(workers_.size());
        for (const auto& [loop, worker] : workers_) {
            (void)loop;
            result.push_back(worker.get());
        }
        return result;
    }

    [[noreturn]] static void failStop(const char* phase)
    {
        std::fprintf(stderr, "gateway forced shutdown stalled during %s\n",
                     phase);
        std::abort();
    }

    void postBaseAndWait(
        std::function<void()> action,
        std::chrono::steady_clock::time_point deadline,
        const char* phase)
    {
        bool complete = false;
        baseLoop_.queueControlInLoop([&, action = std::move(action)] {
            action();
            {
                std::lock_guard<std::mutex> lock(shutdownMutex_);
                complete = true;
            }
            shutdownCv_.notify_all();
        });
        std::unique_lock<std::mutex> lock(shutdownMutex_);
        if (!shutdownCv_.wait_until(lock, deadline, [&] { return complete; })) {
            failStop(phase);
        }
    }

    void cancelSessions(
        const std::vector<GatewayWorker*>& workers,
        std::chrono::steady_clock::time_point deadline)
    {
        runOnWorkersAndWait(
            workers,
            [](GatewayWorker& worker) {
                worker.pool.cancelPendingAcquisitions();
                std::vector<std::shared_ptr<ProxySession>> sessions;
                sessions.reserve(worker.sessions.size());
                for (const auto& [session, owned] : worker.sessions) {
                    (void)session;
                    sessions.push_back(owned);
                }
                for (const auto& session : sessions) {
                    session->cancel();
                }
            },
            deadline, "dispatch session cancellation");
    }

    void waitForConnectionsToClose(
        std::chrono::steady_clock::time_point deadline)
    {
        while (server_->connectionCount() != 0) {
            std::unique_lock<std::mutex> lock(shutdownMutex_);
            if (shutdownCv_.wait_until(lock, deadline)
                == std::cv_status::timeout
                && server_->connectionCount() != 0) {
                failStop("publish connection destruction");
            }
        }
    }

    template <typename Action>
    void runOnWorkersAndWait(
        const std::vector<GatewayWorker*>& workers, Action action,
        std::chrono::steady_clock::time_point deadline,
        const char* phase)
    {
        std::size_t complete = 0;
        for (GatewayWorker* worker : workers) {
            worker->loop.queueControlInLoop([&, worker, action] {
                action(*worker);
                {
                    std::lock_guard<std::mutex> lock(shutdownMutex_);
                    ++complete;
                }
                shutdownCv_.notify_all();
            });
        }
        std::unique_lock<std::mutex> lock(shutdownMutex_);
        if (!shutdownCv_.wait_until(lock, deadline, [&] {
                return complete == workers.size();
            })) {
            failStop(phase);
        }
    }

    void waitForOperationDrain(
        const std::vector<GatewayWorker*>& workers,
        std::chrono::steady_clock::time_point deadline)
    {
        std::size_t ready = 0;
        std::vector<std::shared_ptr<std::function<void()>>> polls;
        polls.reserve(workers.size());
        for (GatewayWorker* worker : workers) {
            auto poll = std::make_shared<std::function<void()>>();
            std::weak_ptr<std::function<void()>> weakPoll = poll;
            *poll = [&, worker, weakPoll] {
                if (worker->loop.inFlightOperationCount() == 0) {
                    {
                        std::lock_guard<std::mutex> lock(shutdownMutex_);
                        ++ready;
                    }
                    shutdownCv_.notify_all();
                    return;
                }
                worker->loop.queueControlInLoop([weakPoll] {
                    if (auto retry = weakPoll.lock()) {
                        (*retry)();
                    }
                });
            };
            worker->loop.queueControlInLoop(*poll);
            polls.push_back(std::move(poll));
        }
        std::unique_lock<std::mutex> lock(shutdownMutex_);
        if (!shutdownCv_.wait_until(lock, deadline, [&] {
                return ready == workers.size();
            })) {
            failStop("drain worker operations");
        }
    }

    void waitForBaseOperationDrain(
        std::chrono::steady_clock::time_point deadline)
    {
        bool ready = false;
        auto poll = std::make_shared<std::function<void()>>();
        std::weak_ptr<std::function<void()>> weakPoll = poll;
        *poll = [&, weakPoll] {
            if (baseLoop_.inFlightOperationCount() == 0) {
                {
                    std::lock_guard<std::mutex> lock(shutdownMutex_);
                    ready = true;
                }
                shutdownCv_.notify_all();
                return;
            }
            baseLoop_.queueControlInLoop([weakPoll] {
                if (auto retry = weakPoll.lock()) {
                    (*retry)();
                }
            });
        };
        baseLoop_.queueControlInLoop(*poll);
        std::unique_lock<std::mutex> lock(shutdownMutex_);
        if (!shutdownCv_.wait_until(lock, deadline, [&] { return ready; })) {
            failStop("drain base operations");
        }
    }

    EventLoop& baseLoop_;
    GatewayConfig config_;
    RouteTable routes_;
    mutable std::mutex workersMutex_;
    std::unordered_map<EventLoop*, std::unique_ptr<GatewayWorker>> workers_;
    bool loopsStopped_{false};
    std::unique_ptr<TcpServer> server_;
    std::atomic_bool started_{false};
    std::atomic_bool acceptingSessions_{false};
    std::atomic_size_t activeSessions_{0};
    mutable std::mutex shutdownMutex_;
    std::condition_variable shutdownCv_;
    bool stopping_{false};
    std::atomic_bool stopped_{false};
};

GatewayServer::GatewayServer(EventLoop& baseLoop, GatewayConfig config)
    : impl_(std::make_unique<Impl>(baseLoop, std::move(config)))
{
}

GatewayServer::~GatewayServer() = default;

void GatewayServer::start()
{
    impl_->start();
}

void GatewayServer::stop(std::chrono::milliseconds gracePeriod)
{
    impl_->stop(gracePeriod);
}

GatewayMetricsSnapshot GatewayServer::metrics() const
{
    return impl_->metrics();
}

std::size_t GatewayServer::activeSessionCount() const noexcept
{
    return impl_->activeSessionCount();
}

} // namespace ucp::proxy
