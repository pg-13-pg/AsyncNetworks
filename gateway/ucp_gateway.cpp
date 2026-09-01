#include "Config.hpp"
#include "EventLoop.hpp"
#include "Logger.hpp"
#include "ucp/proxy/GatewayServer.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <string>
#include <thread>

using namespace std::chrono_literals;

namespace {

void logMetrics(const ucp::proxy::GatewayMetricsSnapshot& metrics)
{
    auto cumulativeLatency = metrics.latencyBuckets;
    for (std::size_t index = 1; index + 1 < cumulativeLatency.size();
         ++index) {
        cumulativeLatency[index] += cumulativeLatency[index - 1];
    }
    LOG_INFO(
        "gateway_metrics connections={} closed={} active={} requests={} status_2xx={} "
        "status_4xx={} status_5xx={} bytes_in={} bytes_out={} "
        "connect_errors={} protocol_errors={} timeouts={} cancellations={} "
        "overloads={} queue_rejections={} queue_high_water={} queue_max={} "
        "pool_acquisitions={} pool_reuses={} pool_active={} pool_idle={}",
        metrics.connectionsAccepted, metrics.connectionsClosed,
        metrics.activeConnections, metrics.requests, metrics.status2xx,
        metrics.status4xx, metrics.status5xx, metrics.bytesFromClients,
        metrics.bytesToClients, metrics.connectErrors, metrics.protocolErrors,
        metrics.timeoutErrors, metrics.cancellations, metrics.overloadErrors,
        metrics.queueRejections, metrics.queueHighWaterEvents,
        metrics.queueMaxDepth, metrics.poolAcquisitions, metrics.poolReuses,
        metrics.poolActiveConnections, metrics.poolIdleConnections);
    LOG_INFO(
        "gateway_latency "
        "latency_le_0_1={} latency_le_0_25={} latency_le_0_5={} latency_le_1={} "
        "latency_le_2_5={} latency_le_5={} latency_le_10={} latency_le_25={} "
        "latency_le_50={} latency_le_100={} latency_le_250={} latency_le_500={} "
        "latency_le_1000={} latency_gt_1000={}",
        cumulativeLatency[0], cumulativeLatency[1], cumulativeLatency[2],
        cumulativeLatency[3], cumulativeLatency[4], cumulativeLatency[5],
        cumulativeLatency[6], cumulativeLatency[7], cumulativeLatency[8],
        cumulativeLatency[9], cumulativeLatency[10], cumulativeLatency[11],
        cumulativeLatency[12], cumulativeLatency[13]);
}

} // namespace

int main(int argc, char** argv)
{
    const std::string configPath = argc > 1 && argv[1]
        ? argv[1] : "config/gateway.conf";
    Config rawConfig;
    std::string configError;
    if (!rawConfig.loadFromFile(configPath, &configError)) {
        std::fprintf(stderr, "gateway config load failed: %s\n",
                     configError.c_str());
        return 1;
    }
    auto parsed = ucp::proxy::GatewayConfig::from(rawConfig);
    if (!parsed) {
        std::fprintf(stderr, "gateway config invalid: %s\n",
                     parsed.error().message.c_str());
        return 1;
    }
    auto config = std::move(parsed).takeValue();

    sigset_t signals;
    ::sigemptyset(&signals);
    ::sigaddset(&signals, SIGINT);
    ::sigaddset(&signals, SIGTERM);
    if (::pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0) {
        std::fputs("gateway failed to block termination signals\n", stderr);
        Logger::shutdown();
        return 1;
    }

    // Every subsequently created thread must inherit the blocked mask so
    // process-directed termination is consumed only by sigwait().
    Logger::Options logOptions;
    logOptions.logFile = "logs/ucp_gateway.log";
    Logger::init(logOptions);

    EventLoop baseLoop(config.eventLoopOptions);
    if (!baseLoop.initRegisteredBuffers()) {
        LOG_WARN("gateway registered buffers unavailable; using ordinary buffers");
    }
    ucp::proxy::GatewayServer gateway(baseLoop, config);
    gateway.start();

    std::atomic_bool stopping{false};
    std::thread metricThread([&] {
        while (!stopping.load(std::memory_order_acquire)) {
            for (int tick = 0; tick < 50
                 && !stopping.load(std::memory_order_acquire); ++tick) {
                std::this_thread::sleep_for(100ms);
            }
            if (!stopping.load(std::memory_order_acquire)) {
                logMetrics(gateway.metrics());
            }
        }
    });
    std::thread signalThread([&] {
        int signal = 0;
        if (::sigwait(&signals, &signal) == 0) {
            LOG_INFO("gateway received signal {}", signal);
            stopping.store(true, std::memory_order_release);
            gateway.stop(config.gracefulShutdown);
        }
    });

    LOG_INFO("gateway listening on {}:{} with {} workers",
             config.listenIp, config.listenPort, config.workerCount);
    baseLoop.loop();
    signalThread.join();
    stopping.store(true, std::memory_order_release);
    metricThread.join();
    logMetrics(gateway.metrics());
    Logger::shutdown();
    return 0;
}
