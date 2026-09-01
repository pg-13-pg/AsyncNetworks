#include "ucp/proxy/UpstreamPool.hpp"

#include "EventLoop.hpp"
#include "TcpConnection.hpp"
#include "ucp/net/AsyncConnect.hpp"

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <utility>

namespace ucp::proxy {
namespace {

Result<UpstreamLease> poolFailure(
    ErrorCode code, int systemError, std::string message)
{
    return Result<UpstreamLease>::failure(
        {code, systemError, std::move(message)});
}

} // namespace

UpstreamLease::UpstreamLease(
    UpstreamPool* pool,
    std::string bucketKey,
    Endpoint endpoint,
    std::shared_ptr<TcpConnection> connection)
    : pool_(pool)
    , bucketKey_(std::move(bucketKey))
    , endpoint_(std::move(endpoint))
    , connection_(std::move(connection))
{
}

UpstreamLease::UpstreamLease(UpstreamLease&& other) noexcept
    : pool_(std::exchange(other.pool_, nullptr))
    , bucketKey_(std::move(other.bucketKey_))
    , endpoint_(std::move(other.endpoint_))
    , connection_(std::move(other.connection_))
    , reusable_(std::exchange(other.reusable_, false))
{
}

UpstreamLease& UpstreamLease::operator=(UpstreamLease&& other) noexcept
{
    if (this != &other) {
        release();
        pool_ = std::exchange(other.pool_, nullptr);
        bucketKey_ = std::move(other.bucketKey_);
        endpoint_ = std::move(other.endpoint_);
        connection_ = std::move(other.connection_);
        reusable_ = std::exchange(other.reusable_, false);
    }
    return *this;
}

UpstreamLease::~UpstreamLease()
{
    release();
}

std::shared_ptr<TcpConnection> UpstreamLease::connection() const
{
    return connection_;
}

void UpstreamLease::markReusable() noexcept
{
    if (pool_ && connection_) {
        reusable_ = true;
    }
}

void UpstreamLease::release() noexcept
{
    if (pool_) {
        pool_->release(*this);
    }
}

UpstreamPool::UpstreamPool(EventLoop& loop)
    : loop_(loop)
{
}

std::string UpstreamPool::makeBucketKey(
    const Route& route, const Endpoint& endpoint)
{
    return std::to_string(route.name.size()) + ':' + route.name
        + endpoint.key();
}

UpstreamPool::Bucket& UpstreamPool::bucketFor(
    const Route& route, const Endpoint& endpoint)
{
    const auto key = makeBucketKey(route, endpoint);
    auto [position, inserted] = buckets_.try_emplace(key);
    if (inserted) {
        position->second.maxConnections = route.maxConnectionsPerWorker;
        position->second.maxIdle = route.maxIdlePerWorker;
        position->second.idleTimeout = route.idleTimeout;
    }
    return position->second;
}

void UpstreamPool::pruneIdle(Bucket& bucket)
{
    const auto now = std::chrono::steady_clock::now();
    auto position = bucket.idleConnections.begin();
    while (position != bucket.idleConnections.end()) {
        const bool expired = bucket.idleTimeout <=
                std::chrono::milliseconds::zero()
            || now - position->idleSince >= bucket.idleTimeout;
        if (!position->connection
            || !position->connection->isConnected()
            || expired) {
            auto connection = std::move(position->connection);
            position = bucket.idleConnections.erase(position);
            assert(bucket.totalCount > 0);
            --bucket.totalCount;
            closeConnection(connection);
        } else {
            ++position;
        }
    }
}

Task<Result<UpstreamLease>> UpstreamPool::acquire(
    const Route& route,
    const Endpoint& endpoint,
    std::chrono::steady_clock::time_point deadline)
{
    if (!loop_.isInLoopThread()) {
        co_return poolFailure(
            ErrorCode::system, EPERM,
            "upstream pool must run on its owning EventLoop");
    }
    if (deadline <= std::chrono::steady_clock::now()) {
        co_return poolFailure(
            ErrorCode::timedOut, ETIMEDOUT,
            "upstream acquisition deadline has expired");
    }

    const auto key = makeBucketKey(route, endpoint);
    Bucket& bucket = bucketFor(route, endpoint);
    pruneIdle(bucket);
    if (!bucket.idleConnections.empty()) {
        auto connection = std::move(
            bucket.idleConnections.front().connection);
        bucket.idleConnections.pop_front();
        ++bucket.leasedCount;
        co_return Result<UpstreamLease>::success(
            UpstreamLease(this, key, endpoint, std::move(connection)));
    }

    if (bucket.totalCount + bucket.connectingCount
        >= bucket.maxConnections) {
        co_return poolFailure(
            ErrorCode::resourceExhausted, EAGAIN,
            "upstream connection capacity exhausted");
    }

    ++bucket.connectingCount;
    struct ConnectingReservation {
        Bucket& bucket;
        ~ConnectingReservation()
        {
            assert(bucket.connectingCount > 0);
            --bucket.connectingCount;
        }
    } reservation{bucket};

    const auto connectDeadline = std::min(
        deadline,
        std::chrono::steady_clock::now() + route.connectTimeout);
    auto connected = co_await asyncConnect(
        loop_, endpoint.address(), connectDeadline,
        "upstream-" + endpoint.key() + '-'
            + std::to_string(nextConnectionId_++));
    if (!connected) {
        co_return Result<UpstreamLease>::failure(connected.error());
    }

    ++bucket.totalCount;
    ++bucket.leasedCount;
    co_return Result<UpstreamLease>::success(
        UpstreamLease(
            this, key, endpoint,
            std::move(connected).takeValue()));
}

void UpstreamPool::release(UpstreamLease& lease) noexcept
{
    assert(loop_.isInLoopThread());
    assert(lease.pool_ == this);
    const auto position = buckets_.find(lease.bucketKey_);
    assert(position != buckets_.end());
    Bucket& bucket = position->second;
    assert(bucket.leasedCount > 0);
    assert(bucket.totalCount > 0);
    --bucket.leasedCount;

    auto connection = std::move(lease.connection_);
    if (lease.reusable_ && connection && connection->isConnected()
        && bucket.idleConnections.size() < bucket.maxIdle) {
        try {
            bucket.idleConnections.push_back(
                {connection, std::chrono::steady_clock::now()});
            connection.reset();
        } catch (...) {
            // A noexcept lease destructor must degrade to discard on
            // bookkeeping allocation failure.
        }
    }
    if (connection) {
        --bucket.totalCount;
        closeConnection(connection);
    }

    lease.pool_ = nullptr;
    lease.bucketKey_.clear();
    lease.reusable_ = false;
}

void UpstreamPool::closeConnection(
    const std::shared_ptr<TcpConnection>& connection) noexcept
{
    if (connection) {
        connection->connectDestroyed();
    }
}

void UpstreamPool::closeIdle()
{
    assert(loop_.isInLoopThread());
    for (auto& [key, bucket] : buckets_) {
        (void)key;
        while (!bucket.idleConnections.empty()) {
            auto connection = std::move(
                bucket.idleConnections.front().connection);
            bucket.idleConnections.pop_front();
            assert(bucket.totalCount > 0);
            --bucket.totalCount;
            closeConnection(connection);
        }
    }
}

std::size_t UpstreamPool::active(
    const Route& route, const Endpoint& endpoint) const
{
    assert(loop_.isInLoopThread());
    const auto position = buckets_.find(makeBucketKey(route, endpoint));
    return position == buckets_.end() ? 0 : position->second.leasedCount;
}

std::size_t UpstreamPool::idle(
    const Route& route, const Endpoint& endpoint) const
{
    assert(loop_.isInLoopThread());
    const auto position = buckets_.find(makeBucketKey(route, endpoint));
    return position == buckets_.end()
        ? 0
        : position->second.idleConnections.size();
}

} // namespace ucp::proxy
