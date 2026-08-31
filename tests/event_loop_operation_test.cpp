#include "EventLoopThread.hpp"
#include "TestSupport.hpp"
#include "ucp/runtime/IoOperation.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <span>
#include <thread>

using namespace std::chrono_literals;

int main()
{
    EventLoop::Options options;
    options.ringEntries = 32;
    options.sqpoll = false;
    options.registeredBuffersCount = 0;
    options.pendingQueueCapacity = 1;
    options.pendingSubmissionCapacity = 4;

    EventLoopThread thread(options);
    EventLoop* loop = thread.startLoop();

    std::atomic_bool blockerStarted{false};
    std::atomic_bool releaseBlocker{false};
    CHECK(loop->queueInLoop([&] {
        blockerStarted.store(true, std::memory_order_release);
        while (!releaseBlocker.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(1ms);
        }
    }));
    CHECK(TestSupport::waitUntil(
        [&] { return blockerStarted.load(std::memory_order_acquire); }, 2s));

    CHECK(loop->queueInLoop([] {}));
    CHECK(!loop->queueInLoop([] {}));

    std::atomic_bool controlRan{false};
    loop->queueControlInLoop([&] {
        controlRan.store(true, std::memory_order_release);
    });
    releaseBlocker.store(true, std::memory_order_release);

    CHECK(TestSupport::waitUntil(
        [&] { return controlRan.load(std::memory_order_acquire); }, 2s));
    CHECK(loop->getBackpressureStats().queueFullCount > 0);

    std::atomic_int completionCount{0};
    std::atomic_bool nopSucceeded{false};
    auto operation = std::make_shared<ucp::IoOperation>(
        1, ucp::OperationType::write, false);
    operation->setCompletionCallback([&](const ucp::IoResult& result) {
        nopSucceeded.store(
            result.hasValue() && result.value() == 0,
            std::memory_order_release);
        completionCount.fetch_add(1, std::memory_order_relaxed);
    });

    loop->queueControlInLoop([loop, operation] {
        const auto submitted = loop->submitOperation(
            operation, 1,
            [](std::span<io_uring_sqe*> sqes, ucp::IoOperation&) {
                io_uring_prep_nop(sqes.front());
            });
        CHECK(submitted.disposition != EventLoop::SubmitDisposition::rejected);
    });

    CHECK(TestSupport::waitUntil(
        [&] { return completionCount.load(std::memory_order_acquire) == 1; },
        2s));
    CHECK(nopSucceeded.load(std::memory_order_acquire));
    CHECK(TestSupport::waitUntil(
        [&] { return loop->inFlightOperationCount() == 0; }, 2s));

    loop->quit();
    return 0;
}
