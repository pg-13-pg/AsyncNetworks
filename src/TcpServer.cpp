#include "TcpServer.hpp"

#include <iostream>
#include <thread>

TcpServer::TcpServer(EventLoop *loop, const InetAddress &listenAddr, const std::string &name)
    : loop_(loop), name_(name), ipPort_(listenAddr.toIpPort()), acceptor_(new Acceptor(loop, listenAddr, true)),
      started_(false), nextConnId_(1), threadPool_(loop)
{
    // 设置新连接到来的回调函数,传递给Acceptor对象调用，非TcpServer中的void setNewConnectionCallback()
    acceptor_->setNewConnectionCallback(std::bind(&TcpServer::newConnection, this, std::placeholders::_1,
                                                  std::placeholders::_2)); // 成员函数不能脱离对象使用，需要使用this
}

TcpServer::~TcpServer()
{
    for (auto &pair : connections_)
    {
        auto conn = pair.second;
        pair.second.reset(); // 释放shared_ptr(pair.second)，减少引用计数
        conn->forceClose();  // 强制关闭连接
    }
}

void TcpServer::start()
{
    // 防止重复启动
    if (started_.load())
    {
        return;
    }
    // 初始化线程池
    threadPool_.start();
    // 开始监听
    auto listen = [this]() { acceptor_->listen(); };
    if (!loop_->runInLoop(listen))
    {
        loop_->queueControlInLoop(std::move(listen));
    }
    started_.store(true);
}

void TcpServer::setThreadNum(int numThreads)
{
    threadPool_.setThreadNum(numThreads);
}

// newConnection是Acceptor对象调用的回调函数
void TcpServer::newConnection(int sockfd, const InetAddress &peerAddr)
{
    // 选择一个 EventLoop 来处理新连接
    EventLoop *ioLoop = threadPool_.getNextLoop();
    // 生成连接名称，连接名称格式为：服务器名称-服务器IP:端口#连接ID，例如
    // MyServer-192.168.1.1:8080#1
    char buf[32];
    snprintf(buf, sizeof buf, "-%s#%d", ipPort_.c_str(), nextConnId_++); //-192.168.1.1:8080#1
    std::string connName = name_ + buf;

    // 创建 TcpConnection 对象，使用 shared_ptr 管理生命周期
    auto conn = std::make_shared<TcpConnection>(connName, ioLoop, sockfd, peerAddr);
    // 设置业务逻辑回调函数，TCPConnection对象调用
    conn->setConnectionCallback(connectionCallback_);
    // 设置关闭连接时的回调函数，TCPConnection对象调用
    conn->setCloseCallback(std::bind(&TcpServer::removeConnection, this, std::placeholders::_1));
    conn->setTimeout(readTimeout_); // 设置读超时，清除空闲死连接（僵尸连接）

    // 保存连接到活动连接列表
    connections_[connName] = conn;

    // 在对应的 EventLoop 线程中建立连接
    auto establish = std::bind(&TcpConnection::connectEstablished, conn);
    if (!ioLoop->runInLoop(establish))
    {
        ioLoop->queueControlInLoop(std::move(establish));
    }
}

void TcpServer::removeConnection(const std::shared_ptr<TcpConnection> &conn)
{
    // 在主线程的 EventLoop 中移除连接
    auto remove = [this, conn]() {
        // 从活动连接列表中移除连接
        connections_.erase(conn->getName());
        // 在连接所属的 EventLoop 线程中销毁连接
        EventLoop *ioLoop = conn->getLoop();
        ioLoop->queueControlInLoop(
            std::bind(&TcpConnection::connectDestroyed, conn));
    };
    if (!loop_->runInLoop(remove))
    {
        loop_->queueControlInLoop(std::move(remove));
    }
}
