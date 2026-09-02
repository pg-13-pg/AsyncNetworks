#pragma once

#include <functional>
#include <memory>

class EventLoop;
class InetAddress;

class Acceptor
{
  public:
    Acceptor(const Acceptor &) = delete;
    Acceptor &operator=(const Acceptor &) = delete;

    using NewConnectionCallback =
        std::function<void(int sockfd, const InetAddress &peerAddr)>;

    Acceptor(EventLoop *loop, const InetAddress &listenAddr, bool reuseport);
    ~Acceptor();

    void setNewConnectionCallback(const NewConnectionCallback &cb);
    bool isListening() const;
    void listen();
    void stop();

  private:
    struct State;
    std::shared_ptr<State> state_;
};
