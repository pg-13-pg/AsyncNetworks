#include "EventLoopThread.hpp"

#include <thread>

#include "EventLoop.hpp"
#include "Logger.hpp"

EventLoopThread::EventLoopThread(const EventLoop::Options &options, const ThreadInitCallback &cb)
    : loop_(nullptr), exiting_(false), options_(options), callback_(cb)
{
}

EventLoopThread::~EventLoopThread()
{
    exiting_ = true;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (loop_ != nullptr)
        {
            loop_->quit();
        }
    }
    if (thread_.joinable())
    {
        thread_.join();
    }
}

// 真正创建线程的函数
EventLoop *EventLoopThread::startLoop()
{
    thread_ = std::thread(std::bind(&EventLoopThread::threadFunc, this)); // 创建工作线程

    EventLoop *loop = nullptr;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this] { return loop_ != nullptr; }); // 主线程执行 ,等待工作线程创建loop直到 loop_ 被设置
        loop = loop_;
    }
    return loop;
}

// 工作线程的主函数，创建 EventLoop 对象并启动事件循环
void EventLoopThread::threadFunc()
{
    EventLoop loop(options_); // 栈上创建EventLoop对象

    LOG_INFO("EventLoop thread start, loop={}", static_cast<void *>(&loop));

    if (!loop.initRegisteredBuffers())
    {
        LOG_WARN("EventLoop registered-buffer initialization failed; using ordinary buffers");
    }

    if (callback_)
    {
        callback_(&loop); // 执行回调
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        loop_ = &loop;
    }
    cond_.notify_one();

    loop.loop();

    LOG_INFO("EventLoop thread exit");

    std::lock_guard<std::mutex> lock(mutex_);
    loop_ = nullptr;
}

/*
EventLoop *EventLoopThread::startLoop()
{
    std::promise<EventLoop *> p;
    std::future<EventLoop *> f = p.get_future();

    thread_ = std::thread(std::bind(&EventLoopThread::threadFunc, this,
std::move(p)));

    EventLoop *loop = f.get(); // 阻塞等待直到 promise 设置值
    return loop;
}

void EventLoopThread::threadFunc(std::promise<EventLoop *> &&p)
{
    EventLoop loop; // 栈上创建EventLoop对象

    if (callback_)
    {
        callback_(&loop); // 允许用户在 Loop 开始循环前做一些定制化设置
    }

    {
        std::unique_lock<std::mutex> lock(mutex_);
        loop_ = &loop;
    }

    // 通知主线程 loop 已经初始化完毕
    p.set_value(&loop);

    // 开始循环
    loop.loop();

    std::lock_guard<std::mutex> lock(mutex_);
    loop_ = nullptr;
}
*/
