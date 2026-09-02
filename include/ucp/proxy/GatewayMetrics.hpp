#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace ucp::proxy {

inline constexpr std::size_t gatewayLatencyBucketCount = 14;

struct GatewayMetricsSnapshot {
    std::uint64_t connectionsAccepted{0};
    std::uint64_t connectionsClosed{0};
    std::uint64_t activeConnections{0};
    std::uint64_t requests{0};
    std::uint64_t status2xx{0};
    std::uint64_t status4xx{0};
    std::uint64_t status5xx{0};
    std::uint64_t bytesFromClients{0};
    std::uint64_t bytesToClients{0};
    std::uint64_t connectErrors{0};
    std::uint64_t protocolErrors{0};
    std::uint64_t timeoutErrors{0};
    std::uint64_t cancellations{0};
    std::uint64_t overloadErrors{0};
    std::uint64_t queueRejections{0};
    std::uint64_t queueHighWaterEvents{0};
    std::uint64_t queueMaxDepth{0};
    std::uint64_t poolAcquisitions{0};
    std::uint64_t poolReuses{0};
    std::uint64_t poolActiveConnections{0};
    std::uint64_t poolIdleConnections{0};
    std::array<std::uint64_t, gatewayLatencyBucketCount> latencyBuckets{};

    GatewayMetricsSnapshot& operator+=(const GatewayMetricsSnapshot& other)
    {
        connectionsAccepted += other.connectionsAccepted;
        connectionsClosed += other.connectionsClosed;
        activeConnections += other.activeConnections;
        requests += other.requests;
        status2xx += other.status2xx;
        status4xx += other.status4xx;
        status5xx += other.status5xx;
        bytesFromClients += other.bytesFromClients;
        bytesToClients += other.bytesToClients;
        connectErrors += other.connectErrors;
        protocolErrors += other.protocolErrors;
        timeoutErrors += other.timeoutErrors;
        cancellations += other.cancellations;
        overloadErrors += other.overloadErrors;
        queueRejections += other.queueRejections;
        queueHighWaterEvents += other.queueHighWaterEvents;
        queueMaxDepth = queueMaxDepth > other.queueMaxDepth
            ? queueMaxDepth : other.queueMaxDepth;
        poolAcquisitions += other.poolAcquisitions;
        poolReuses += other.poolReuses;
        poolActiveConnections += other.poolActiveConnections;
        poolIdleConnections += other.poolIdleConnections;
        for (std::size_t index = 0; index < latencyBuckets.size(); ++index) {
            latencyBuckets[index] += other.latencyBuckets[index];
        }
        return *this;
    }
};

struct alignas(64) GatewayMetricShard {
    std::atomic_uint64_t connectionsAccepted{0};
    std::atomic_uint64_t connectionsClosed{0};
    std::atomic_uint64_t activeConnections{0};
    std::atomic_uint64_t requests{0};
    std::atomic_uint64_t status2xx{0};
    std::atomic_uint64_t status4xx{0};
    std::atomic_uint64_t status5xx{0};
    std::atomic_uint64_t bytesFromClients{0};
    std::atomic_uint64_t bytesToClients{0};
    std::atomic_uint64_t connectErrors{0};
    std::atomic_uint64_t protocolErrors{0};
    std::atomic_uint64_t timeoutErrors{0};
    std::atomic_uint64_t cancellations{0};
    std::atomic_uint64_t overloadErrors{0};
    std::atomic_uint64_t queueRejections{0};
    std::atomic_uint64_t queueHighWaterEvents{0};
    std::atomic_uint64_t queueMaxDepth{0};
    std::atomic_uint64_t poolAcquisitions{0};
    std::atomic_uint64_t poolReuses{0};
    std::atomic_uint64_t poolActiveConnections{0};
    std::atomic_uint64_t poolIdleConnections{0};
    std::array<std::atomic_uint64_t, gatewayLatencyBucketCount>
        latencyBuckets{};

    void recordLatency(double milliseconds) noexcept
    {
        static constexpr std::array<double, gatewayLatencyBucketCount - 1>
            upperBounds{0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                        25.0, 50.0, 100.0, 250.0, 500.0, 1000.0};
        std::size_t index = 0;
        while (index < upperBounds.size()
               && milliseconds > upperBounds[index]) {
            ++index;
        }
        latencyBuckets[index].fetch_add(1, std::memory_order_relaxed);
    }

    GatewayMetricsSnapshot snapshot() const noexcept
    {
        GatewayMetricsSnapshot result;
#define UCP_LOAD_METRIC(name) \
        result.name = name.load(std::memory_order_relaxed)
        UCP_LOAD_METRIC(connectionsAccepted);
        UCP_LOAD_METRIC(connectionsClosed);
        UCP_LOAD_METRIC(activeConnections);
        UCP_LOAD_METRIC(requests);
        UCP_LOAD_METRIC(status2xx);
        UCP_LOAD_METRIC(status4xx);
        UCP_LOAD_METRIC(status5xx);
        UCP_LOAD_METRIC(bytesFromClients);
        UCP_LOAD_METRIC(bytesToClients);
        UCP_LOAD_METRIC(connectErrors);
        UCP_LOAD_METRIC(protocolErrors);
        UCP_LOAD_METRIC(timeoutErrors);
        UCP_LOAD_METRIC(cancellations);
        UCP_LOAD_METRIC(overloadErrors);
        UCP_LOAD_METRIC(queueRejections);
        UCP_LOAD_METRIC(queueHighWaterEvents);
        UCP_LOAD_METRIC(queueMaxDepth);
        UCP_LOAD_METRIC(poolAcquisitions);
        UCP_LOAD_METRIC(poolReuses);
        UCP_LOAD_METRIC(poolActiveConnections);
        UCP_LOAD_METRIC(poolIdleConnections);
#undef UCP_LOAD_METRIC
        for (std::size_t index = 0; index < latencyBuckets.size(); ++index) {
            result.latencyBuckets[index] =
                latencyBuckets[index].load(std::memory_order_relaxed);
        }
        return result;
    }
};

} // namespace ucp::proxy
