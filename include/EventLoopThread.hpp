#pragma once

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

#include "EventLoop.hpp"

class EventLoop;

/**
 * 该类封装一个线程，在线程中运行一个 EventLoop 对象
 */

class EventLoopThread
{
  public:
    using ThreadInitCallback = std::function<void(EventLoop *)>;

    EventLoopThread(const EventLoop::Options &options,
                    const ThreadInitCallback &cb = ThreadInitCallback()); // 默认为空回调
    ~EventLoopThread();

    // 禁止拷贝和赋值
    EventLoopThread(const EventLoopThread &) = delete;
    EventLoopThread &operator=(const EventLoopThread &) = delete;

    EventLoop *startLoop();

  private:
    // promise/future版本，开销较大
    // void threadFunc(std::promise<EventLoop *> &&p);
    void threadFunc();

    std::unique_ptr<EventLoop> loopOwner_;
    EventLoop *loop_; // loopOwner_ 的非拥有观察指针
    bool exiting_;
    bool ready_{false};
    EventLoop::Options options_;
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable cond_;
    ThreadInitCallback callback_;
};
