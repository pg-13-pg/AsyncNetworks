#include "EventLoopThread.hpp"
#include "TestSupport.hpp"
#include "ucp/runtime/IoOperation.hpp"

#include <atomic>
#include <array>
#include <chrono>
#include <cstddef>
#include <fcntl.h>
#include <memory>
#include <span>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;

namespace {

EventLoop::Options testOptions()
{
    EventLoop::Options options;
    options.ringEntries = 32;
    options.sqpoll = false;
    options.registeredBuffersCount = 0;
    options.pendingQueueCapacity = 65536;
    options.pendingSubmissionCapacity = 4;
    return options;
}

void verifyQuitBeforeLoopStarts()
{
    EventLoopThread thread(testOptions(), [](EventLoop* loop) {
        loop->quit();
    });
    EventLoop* loop = thread.startLoop();
    CHECK(!loop->queueInLoop([] {}));
    CHECK(!loop->queueControlInLoop([] {}));
}

void verifyConcurrentShutdownLinearizesIngress()
{
    std::atomic_uint64_t acceptedNormal{0};
    std::atomic_uint64_t completedNormal{0};
    std::atomic_uint64_t acceptedControl{0};
    std::atomic_uint64_t completedControl{0};

    {
        EventLoopThread thread(testOptions());
        EventLoop* loop = thread.startLoop();

        std::vector<std::thread> producers;
        for (int i = 0; i < 2; ++i) {
            producers.emplace_back([&] {
                while (true) {
                    if (!loop->queueInLoop([&] {
                            completedNormal.fetch_add(
                                1, std::memory_order_relaxed);
                        })) {
                        break;
                    }
                    acceptedNormal.fetch_add(1, std::memory_order_relaxed);

                    if (!loop->queueControlInLoop([&] {
                            completedControl.fetch_add(
                                1, std::memory_order_relaxed);
                        })) {
                        break;
                    }
                    acceptedControl.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }

        CHECK(TestSupport::waitUntil(
            [&] {
                return acceptedNormal.load(std::memory_order_acquire) >= 100;
            },
            2s));

        std::vector<std::thread> quitters;
        for (int i = 0; i < 4; ++i) {
            quitters.emplace_back([loop] { loop->quit(); });
        }
        for (auto& quitter : quitters) {
            quitter.join();
        }
        for (auto& producer : producers) {
            producer.join();
        }

        CHECK(!loop->queueInLoop([] {}));
        CHECK(!loop->queueControlInLoop([] {}));
    }

    CHECK_EQ(completedNormal.load(std::memory_order_acquire),
             acceptedNormal.load(std::memory_order_acquire));
    CHECK_EQ(completedControl.load(std::memory_order_acquire),
             acceptedControl.load(std::memory_order_acquire));
}

void verifyOwnerDestructorDrainsAcceptedWork()
{
    std::atomic_bool ran{false};
    {
        EventLoopThread thread(testOptions());
        EventLoop* loop = thread.startLoop();
        CHECK(loop->queueInLoop([&] {
            ran.store(true, std::memory_order_release);
        }));
    }
    CHECK(ran.load(std::memory_order_acquire));
}

void verifyStoppingRejectsFinalDrainIo()
{
    std::atomic_bool blockerStarted{false};
    std::atomic_bool releaseBlocker{false};
    std::atomic_bool submissionRejected{false};

    {
        EventLoopThread thread(testOptions());
        EventLoop* loop = thread.startLoop();
        CHECK(loop->queueInLoop([&] {
            blockerStarted.store(true, std::memory_order_release);
            while (!releaseBlocker.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
        }));
        CHECK(loop->queueInLoop([&] {
            auto operation = std::make_shared<ucp::IoOperation>(
                loop->nextOperationId(), ucp::OperationType::write, false);
            const auto result = loop->submitOperation(
                operation, 1,
                [](std::span<io_uring_sqe*> sqes, ucp::IoOperation&) {
                    io_uring_prep_nop(sqes.front());
                });
            submissionRejected.store(
                result.disposition == EventLoop::SubmitDisposition::rejected
                    && result.error.code == ucp::ErrorCode::cancelled,
                std::memory_order_release);
        }));
        CHECK(TestSupport::waitUntil(
            [&] { return blockerStarted.load(std::memory_order_acquire); },
            2s));
        loop->quit();
        releaseBlocker.store(true, std::memory_order_release);
    }

    CHECK(submissionRejected.load(std::memory_order_acquire));
}

void verifyShutdownCancelsInflightIo()
{
    std::array<int, 2> pipeFds{-1, -1};
    CHECK_EQ(::pipe2(pipeFds.data(), O_NONBLOCK | O_CLOEXEC), 0);
    std::byte byte{};
    std::atomic_bool submitted{false};
    std::atomic_bool cancelled{false};

    {
        EventLoopThread thread(testOptions());
        EventLoop* loop = thread.startLoop();
        CHECK(loop->queueControlInLoop([&] {
            auto operation = std::make_shared<ucp::IoOperation>(
                loop->nextOperationId(), ucp::OperationType::read, false);
            operation->setCompletionCallback(
                [&](const ucp::IoResult& result) {
                    cancelled.store(
                        !result
                            && result.error().code
                                == ucp::ErrorCode::cancelled,
                        std::memory_order_release);
                });
            const auto result = loop->submitOperation(
                operation, 1,
                [&](std::span<io_uring_sqe*> sqes, ucp::IoOperation&) {
                    io_uring_prep_read(
                        sqes.front(), pipeFds[0], &byte, sizeof(byte), 0);
                });
            CHECK(result.disposition
                  != EventLoop::SubmitDisposition::rejected);
            submitted.store(true, std::memory_order_release);
        }));
        CHECK(TestSupport::waitUntil(
            [&] {
                return submitted.load(std::memory_order_acquire)
                    && loop->inFlightOperationCount() == 1;
            },
            2s));
    }

    CHECK(cancelled.load(std::memory_order_acquire));
    CHECK_EQ(::close(pipeFds[0]), 0);
    CHECK_EQ(::close(pipeFds[1]), 0);
}

void verifyShutdownDrainsAnExistingCancel()
{
    std::array<int, 2> pipeFds{-1, -1};
    CHECK_EQ(::pipe2(pipeFds.data(), O_NONBLOCK | O_CLOEXEC), 0);
    std::byte byte{};
    std::atomic_int completionCount{0};
    std::atomic_bool cancelled{false};
    std::atomic_bool cancelIssued{false};

    {
        EventLoopThread thread(testOptions());
        EventLoop* loop = thread.startLoop();
        CHECK(loop->queueControlInLoop([&] {
            auto operation = std::make_shared<ucp::IoOperation>(
                loop->nextOperationId(), ucp::OperationType::read, false);
            operation->setCompletionCallback(
                [&](const ucp::IoResult& result) {
                    cancelled.store(
                        !result
                            && result.error().code
                                == ucp::ErrorCode::cancelled,
                        std::memory_order_release);
                    completionCount.fetch_add(1, std::memory_order_release);
                });
            const auto result = loop->submitOperation(
                operation, 1,
                [&](std::span<io_uring_sqe*> sqes, ucp::IoOperation&) {
                    io_uring_prep_read(
                        sqes.front(), pipeFds[0], &byte, sizeof(byte), 0);
                });
            CHECK(result.disposition
                  == EventLoop::SubmitDisposition::submitted);
            CHECK(loop->cancelOperation(operation));
            loop->quit();
            cancelIssued.store(true, std::memory_order_release);
        }));
        CHECK(TestSupport::waitUntil(
            [&] { return cancelIssued.load(std::memory_order_acquire); },
            2s));
    }

    CHECK(cancelled.load(std::memory_order_acquire));
    CHECK_EQ(completionCount.load(std::memory_order_acquire), 1);
    CHECK_EQ(::close(pipeFds[0]), 0);
    CHECK_EQ(::close(pipeFds[1]), 0);
}

void verifyStoppedOwnerRejectsRunInLoop()
{
    EventLoop loop(testOptions());
    CHECK(loop.queueControlInLoop([&] { loop.quit(); }));
    loop.loop();
    CHECK(!loop.runInLoop([] {}));
}

} // namespace

int main()
{
    auto options = testOptions();
    options.pendingQueueCapacity = 1;

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
    CHECK(!loop->queueInLoop([] {}));
    CHECK(!loop->queueControlInLoop([] {}));

    verifyQuitBeforeLoopStarts();
    verifyConcurrentShutdownLinearizesIngress();
    verifyOwnerDestructorDrainsAcceptedWork();
    verifyStoppingRejectsFinalDrainIo();
    verifyShutdownCancelsInflightIo();
    verifyShutdownDrainsAnExistingCancel();
    verifyStoppedOwnerRejectsRunInLoop();
    return 0;
}
