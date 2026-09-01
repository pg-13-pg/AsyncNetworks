#pragma once

#include <liburing.h>

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <unordered_map>
#include <vector>

#include "Buffer.hpp"
#include "IoContext.hpp"
#include "LockFreeQueue.hpp"
#include "ucp/runtime/IoOperation.hpp"

/**
 * 事件循环类，负责管理和分发事件。
 * 封装io_uring实例，并循环处理完成队列 CQ 中的事件
 */

class EventLoop
{
  public:
    struct Options
    {
        size_t ringEntries = 32768;
        bool sqpoll = true;
        unsigned int sqpollIdleMs = 50;
        size_t registeredBuffersCount = 16384;
        size_t registeredBuffersSize = 4096;
        size_t pendingQueueCapacity = 65536;
        size_t pendingSubmissionCapacity = 4096;
        // 背压管理配置：用于控制跨线程任务队列(pendingFunctors_)的积压情况
        // 当队列长度达到高水位时，触发告警或回调，防止内存无限增长
        size_t pendingQueueHighWaterMark = 58982; // 默认高水位：容量的 90%
        // 当队列长度回落到低水位时，触发恢复回调，表示系统已消化积压任务
        size_t pendingQueueLowWaterMark = 26214; // 默认低水位：容量的 40%
        bool enableQueueFullStats = true;        // 是否开启队列满的统计告警
    };

    using Functor = std::function<void()>;
    using OperationPreparer =
        std::function<void(std::span<io_uring_sqe *>, ucp::IoOperation &)>;

    enum class SubmitDisposition
    {
        submitted,
        queued,
        rejected
    };

    struct SubmitResult
    {
        SubmitDisposition disposition;
        ucp::Error error;
    };
    // 背压回调：当队列从正常→高水位(true) 或 高水位→正常(false) 时调用
    // 业务层可利用此回调实现全局限流（如暂停接收新连接）
    using BackpressureCallback = std::function<void(bool highWaterMarkReached)>;

    EventLoop();
    explicit EventLoop(const Options &options);
    ~EventLoop();

    // 禁止拷贝和赋值
    EventLoop(const EventLoop &) = delete;
    EventLoop &operator=(const EventLoop &) = delete;

    void loop(); // 事件循环主函数

    // 让 EventLoop 停止运行
    void quit();

    // 在当前 Loop 线程执行回调，如果当前线程不是 Loop 所在线程，则将回调放入任务队列，并唤醒 Loop 所在线程执行
    bool runInLoop(Functor cb);
    // 把回调放入任务队列，并唤醒对应的 enentLoop 线程执行
    bool queueInLoop(Functor cb);
    bool queueControlInLoop(Functor cb);
    bool isInLoopThread() const noexcept;

    std::uint64_t nextOperationId() noexcept;
    SubmitResult submitOperation(std::shared_ptr<ucp::IoOperation> operation,
                                 std::size_t sqeCount,
                                 OperationPreparer prepare);
    bool cancelOperation(const std::shared_ptr<ucp::IoOperation> &operation);
    std::size_t inFlightOperationCount() const noexcept;

    // 协程恢复逻辑，当 io_uring_wait_cqe 返回时调用
    void handleCompletionEvent(struct io_uring_cqe *cqe);

    // 初始化缓冲区池
    bool initRegisteredBuffers();

    // 从可用缓冲区中获取一个缓冲区，返回缓冲区索引
    int getRegisteredBufferIndex();

    // 归还缓冲区到缓冲区池
    void returnRegisteredBuffer(int idx);

    // 根据索引取得缓冲区指针
    void *getRegisteredBuffer(int idx);

    // 设置背压回调（当队列水位变化时触发）
    void setBackpressureCallback(const BackpressureCallback &cb)
    {
        backpressureCallback_ = cb;
    }

    // 获取背压统计信息
    struct BackpressureStats
    {
        size_t maxPendingQueueSize = 0;   // 观测到的最大队列大小
        uint64_t queueFullCount = 0;      // 队列满的次数
        uint64_t highWaterMarkEvents = 0; // 触发高水位的次数
        uint64_t lowWaterMarkEvents = 0;  // 触发低水位的次数
    };
    BackpressureStats getBackpressureStats() const;
    void resetBackpressureStats();

    // io_uring 实例，公开以便 Acceptor/Connection 提交请求
    struct io_uring ring_;

  private:
    enum class LifecycleState
    {
        initialized,
        running,
        stopping,
        stopped
    };

    // io_uring I/O完成时，需要唤醒子线程
    void handleWakeup();
    // 执行任务队列中的任务，通常是建立新连接
    void doPendingFunctors();
    void doControlFunctors();
    void flushPendingSubmissions();
    bool prepareSubmission(const std::shared_ptr<ucp::IoOperation> &operation,
                           std::size_t sqeCount,
                           const OperationPreparer &prepare);
    void finishRejectedOperation(
        const std::shared_ptr<ucp::IoOperation> &operation,
        ucp::Error error);
    bool submitCancel(const std::shared_ptr<ucp::IoOperation> &operation);
    void retryCancel(const std::shared_ptr<ucp::IoOperation> &operation);
    // 提交异步读操作以监听 wakeupFd_
    void asyncReadWakeup();
    void wakeup();
    void beginStopping(bool wakeLoop);
    void drainOperationsForShutdown();
    void releaseResources();

    struct PendingSubmission
    {
        std::shared_ptr<ucp::IoOperation> operation;
        std::size_t sqeCount;
        OperationPreparer prepare;
    };

    Options options_;          // 配置
    std::atomic_bool running_; // 事件循环是否在运行
    std::atomic_bool quit_;    // 是否请求退出事件循环
    std::atomic_bool stoppingRequested_{false};
    mutable std::shared_mutex lifecycleMutex_;
    LifecycleState lifecycleState_{LifecycleState::initialized};
    const pid_t threadId_;     // 事件循环所属线程的ID ，使用pid_t更加贴近内核，便于调试

    int wakeupFd_;            // 用于唤醒当前 EventLoop 所在线程，实现跨线程任务通知，即eventfd
    uint64_t wakeupBuffer_;   // eventfd 读取数据的缓冲区
    IoContext wakeupContext_; // 提供给io_uring的唤醒事件的上下文

    //   std::mutex mutex_;
    //   std::vector<Functor> pendingFunctors_;
    LockFreeQueue<Functor> pendingFunctors_; // 任务队列，回调任务
    std::atomic_size_t pendingFunctorCount_{0};
    bool callingPendingFunctors_;            // 是否正在执行任务队列
    std::mutex controlMutex_;
    std::deque<Functor> controlFunctors_;
    std::deque<PendingSubmission> pendingSubmissions_;
    std::unordered_map<std::uint64_t, std::shared_ptr<ucp::IoOperation>> inFlightOperations_;
    std::atomic_size_t inFlightOperationCount_{0};
    std::uint64_t nextOperationId_{1};

    // 背压管理
    BackpressureCallback backpressureCallback_; // 水位变化回调
    std::atomic_size_t maxPendingQueueSize_{0};
    std::atomic_uint64_t queueFullCount_{0};
    std::atomic_uint64_t highWaterMarkEvents_{0};
    std::atomic_uint64_t lowWaterMarkEvents_{0};
    std::atomic_bool inHighWaterMark_{false};   // 是否已处于高水位状态

    std::vector<void *> registeredBuffersPool;  // 缓冲区池，给TcpConnection复用，接发数据的缓冲区
    std::vector<struct iovec> registeredIovecs; // iovec 数组描述的是同一批缓冲区的地址和长度，并用于注册到 io_uring
    bool registeredBuffersInitialized_{false};
    bool registeredBuffersActive_{false};
    bool resourcesReleased_{false};

    // 极致性能优化：单线程模型下无需锁或原子操作，直接用 vector 当栈
    std::vector<int> freeBufferIndices_; // 可用缓冲区索引栈
};
