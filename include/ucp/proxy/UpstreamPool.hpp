#pragma once

#include "ucp/proxy/GatewayConfig.hpp"
#include "ucp/runtime/Result.hpp"
#include "ucp/runtime/Task.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>

class EventLoop;
class TcpConnection;

namespace ucp::proxy {

class UpstreamPool;

class UpstreamLease {
public:
    UpstreamLease(const UpstreamLease&) = delete;
    UpstreamLease& operator=(const UpstreamLease&) = delete;
    UpstreamLease(UpstreamLease&& other) noexcept;
    UpstreamLease& operator=(UpstreamLease&& other) noexcept;
    ~UpstreamLease();

    std::shared_ptr<TcpConnection> connection() const;
    void markReusable() noexcept;

private:
    friend class UpstreamPool;

    UpstreamLease(
        UpstreamPool* pool,
        std::string bucketKey,
        Endpoint endpoint,
        std::shared_ptr<TcpConnection> connection);
    void release() noexcept;

    UpstreamPool* pool_{nullptr};
    std::string bucketKey_;
    Endpoint endpoint_;
    std::shared_ptr<TcpConnection> connection_;
    bool reusable_{false};
};

class UpstreamPool {
public:
    explicit UpstreamPool(EventLoop& loop);
    UpstreamPool(const UpstreamPool&) = delete;
    UpstreamPool& operator=(const UpstreamPool&) = delete;

    Task<Result<UpstreamLease>> acquire(
        const Route& route,
        const Endpoint& endpoint,
        std::chrono::steady_clock::time_point deadline);

    void closeIdle();
    std::size_t active(
        const Route& route, const Endpoint& endpoint) const;
    std::size_t idle(
        const Route& route, const Endpoint& endpoint) const;

private:
    friend class UpstreamLease;

    struct IdleEntry {
        std::shared_ptr<TcpConnection> connection;
        std::chrono::steady_clock::time_point idleSince;
    };

    struct Bucket {
        std::size_t totalCount{0};
        std::size_t leasedCount{0};
        std::size_t connectingCount{0};
        std::size_t maxConnections{0};
        std::size_t maxIdle{0};
        std::chrono::milliseconds idleTimeout{0};
        std::deque<IdleEntry> idleConnections;
    };

    static std::string makeBucketKey(
        const Route& route, const Endpoint& endpoint);
    Bucket& bucketFor(const Route& route, const Endpoint& endpoint);
    void pruneIdle(Bucket& bucket);
    void release(UpstreamLease& lease) noexcept;
    void closeConnection(
        const std::shared_ptr<TcpConnection>& connection) noexcept;

    EventLoop& loop_;
    std::unordered_map<std::string, Bucket> buckets_;
    std::uint64_t nextConnectionId_{1};
};

} // namespace ucp::proxy
