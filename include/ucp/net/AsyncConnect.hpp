#pragma once

#include "ucp/runtime/Result.hpp"
#include "ucp/runtime/Task.hpp"

#include <chrono>
#include <memory>
#include <string>

class EventLoop;
class InetAddress;
class TcpConnection;

namespace ucp {

class IoOperation;

class AsyncConnectControl {
public:
    explicit AsyncConnectControl(EventLoop& loop) noexcept;

    AsyncConnectControl(const AsyncConnectControl&) = delete;
    AsyncConnectControl& operator=(const AsyncConnectControl&) = delete;

    void cancel();
    bool cancellationRequested() const noexcept;

    // Used by the connect awaitable on the owning EventLoop.
    void attach(const std::shared_ptr<IoOperation>& operation);
    void detach() noexcept;

private:
    EventLoop& loop_;
    std::weak_ptr<IoOperation> operation_;
    bool cancellationRequested_{false};
};

Task<Result<std::shared_ptr<TcpConnection>>> asyncConnect(
    EventLoop& loop,
    const InetAddress& peer,
    std::chrono::steady_clock::time_point deadline,
    std::string connectionName,
    AsyncConnectControl* control = nullptr);

} // namespace ucp
