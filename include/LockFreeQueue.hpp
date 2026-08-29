#pragma once
#include <atomic>
#include <cassert>
#include <cstddef>
#include <utility>
#include <vector>

// MPMC环形无锁队列（多生产者多消费者）

template <typename T> class LockFreeQueue
{
  private:
    struct Slot
    {
        std::atomic<size_t> sequence; // 序列号，可代表轮次，且用于解决ABA问题
        T data;
    };

    // 使用 alignas 避免伪共享，cpu读取一个缓存行
    // 64字节，把这两个变量放在不同的缓存行，避免不同核心访问同一缓存行时失效
    alignas(64) std::atomic<size_t> enqueuePos_; // 逻辑入队位置，递增
    alignas(64) std::atomic<size_t> dequeuePos_; // 逻辑出队位置，递增

    std::vector<Slot> buffer_; // 环形缓冲区
    size_t bufferMask_;        // 用于快速取模，bufferMask_=buffer_.size()-1 为全11111....

    static size_t roundUpToPowerOf2(size_t n);   // 向上取整到2的幂
    static constexpr size_t kCacheLineSize = 64; // 缓存行大小

    template <typename U> bool enqueueImpl(U &&data);

  public:
    explicit LockFreeQueue(size_t capacity);
    ~LockFreeQueue() = default;

    // 禁止拷贝和赋值
    LockFreeQueue(const LockFreeQueue &) = delete;
    LockFreeQueue &operator=(const LockFreeQueue &) = delete;
    // 禁止移动
    LockFreeQueue(LockFreeQueue &&) = delete;
    LockFreeQueue &operator=(LockFreeQueue &&) = delete;

    bool enqueue(const T &data);
    bool enqueue(T &&data);
    bool dequeue(T &data);

    bool empty() const;
    size_t size() const; // 并发环境下只是近似大小
};

// 队列大小向上取整到2的幂，保证环形缓冲区的大小是2的幂，便于快速取模计算槽索引
template <typename T> inline size_t LockFreeQueue<T>::roundUpToPowerOf2(size_t n)
{
    assert(n > 0);
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    return n + 1;
}

// inline允许函数在多个编译单元中具有相同定义 a.cpp + 包含的头文件 → a.o b.cpp + 包含的头文件 → b.o  a.o + b.o →
// 可执行程序 （没有inline不允许重复定义则会导致链接错误）
template <typename T>
inline LockFreeQueue<T>::LockFreeQueue(size_t capacity)
    : buffer_(roundUpToPowerOf2(capacity)), bufferMask_(buffer_.size() - 1), enqueuePos_(0), dequeuePos_(0)
{
    // 初始化每个槽的序列号
    for (size_t i = 0; i < buffer_.size(); ++i)
    {
        buffer_[i].sequence.store(i, std::memory_order_relaxed); // 初始化无多线程竞争
    }
}

template <typename T> template <typename U> inline bool LockFreeQueue<T>::enqueueImpl(U &&data)
{
    Slot *slot;
    size_t pos = enqueuePos_.load(std::memory_order_relaxed);

    while (true) // 其他生产者也可能在竞争同一个槽位，循环使用CAS原子操作尝试抢占槽位
    {
        slot = &buffer_[pos & bufferMask_]; // 利用快速取模计算槽索引
        size_t seq = slot->sequence.load(std::memory_order_acquire);
        // seq 与 pos 的差值用于判断槽位状态：0=可写，<0=队列满，>0=被其他线程占用
        intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos); // 计算序列号差值

        if (dif == 0)
        {
            // 槽位可用，CAS 抢占该槽位，因为多线程都可以访问seq和pos，所以需要CAS原子操作保证只有一个线程成功抢占该槽位
            // 当pos与enqueuePos_相等时，将enqueuePos_更新为pos+1，伪失败：=时，底层写入失败，重试一次，（线程挂起、中断，其他线程写入）
            if (enqueuePos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
            {
                break;
            }
        }
        else if (dif < 0) // seq还处于上一轮的序列号，说明队列满了，无法入队
        {
            return false; // 队列满
        }
        else // 另外线程修改了seq（++），pos已经过时，重新读取最新入队位置再重试
        {
            // 竞争失败，重新读取最新入队位置再重试
            pos = enqueuePos_.load(std::memory_order_relaxed);
        }
    }

    // 将数据写入槽位，再发布序列号表示“可读”
    slot->data = std::forward<U>(data);
    slot->sequence.store(pos + 1, std::memory_order_release);
    return true;
}

template <typename T> inline bool LockFreeQueue<T>::enqueue(const T &data)
{
    return enqueueImpl(data);
}

template <typename T> inline bool LockFreeQueue<T>::enqueue(T &&data)
{
    return enqueueImpl(std::forward<T>(data)); // 不能写(data) 尽管data是右值引用，但data本身是一个左值（有名字）
}

template <typename T> inline bool LockFreeQueue<T>::dequeue(T &data)
{
    Slot *slot;
    size_t pos = dequeuePos_.load(std::memory_order_relaxed);

    while (true)
    {
        slot = &buffer_[pos & bufferMask_]; // 计算槽索引
        size_t seq = slot->sequence.load(std::memory_order_acquire);
        // seq 与 pos+1 的差值用于判断槽位状态：0=可读，<0=队列空，>0=被其他线程占用
        intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1); // 计算序列号差值

        if (dif == 0)
        {
            // 槽位有数据，CAS 抢占该槽位
            if (dequeuePos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
            {
                break;
            }
        }
        else if (dif < 0)
        {
            return false; // 队列空
        }
        else
        {
            // 竞争失败，重新读取最新出队位置再重试
            pos = dequeuePos_.load(std::memory_order_relaxed);
        }
    }

    // 取出数据后，发布新的序列号表示“可写”进入下一轮
    data = std::move(slot->data);
    slot->sequence.store(pos + buffer_.size(), std::memory_order_release);
    return true;
}

template <typename T> inline bool LockFreeQueue<T>::empty() const
{
    size_t head = dequeuePos_.load(std::memory_order_relaxed);
    size_t tail = enqueuePos_.load(std::memory_order_relaxed);
    return head == tail;
}

template <typename T> inline size_t LockFreeQueue<T>::size() const
{
    size_t head = dequeuePos_.load(std::memory_order_relaxed);
    size_t tail = enqueuePos_.load(std::memory_order_relaxed);
    return tail - head; // 并发环境下是近似值
}
