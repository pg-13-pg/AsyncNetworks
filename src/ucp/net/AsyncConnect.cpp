#include "ucp/net/AsyncConnect.hpp"

#include "EventLoop.hpp"
#include "InetAddress.hpp"
#include "Socket.hpp"
#include "TcpConnection.hpp"
#include "ucp/runtime/IoOperation.hpp"

#include <cerrno>
#include <cassert>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <liburing.h>
#include <netinet/in.h>
#include <optional>
#include <span>
#include <sys/socket.h>
#include <utility>

namespace ucp {
namespace {

class ConnectOperation final : public IoOperation {
public:
    ConnectOperation(std::uint64_t id, const sockaddr_in& address)
        : IoOperation(id, OperationType::connect, true)
        , peerAddress(address)
    {
    }

    sockaddr_in peerAddress{};
    __kernel_timespec timeout{};
};

bool setRelativeTimeout(
    std::chrono::steady_clock::time_point deadline,
    __kernel_timespec& timeout)
{
    const auto remaining = std::chrono::duration_cast<std::chrono::nanoseconds>(
        deadline - std::chrono::steady_clock::now());
    if (remaining <= std::chrono::nanoseconds::zero()) {
        return false;
    }

    constexpr auto nanosecondsPerSecond = 1'000'000'000LL;
    timeout.tv_sec = remaining.count() / nanosecondsPerSecond;
    timeout.tv_nsec = remaining.count() % nanosecondsPerSecond;
    return true;
}

class ConnectAwaitable {
public:
    ConnectAwaitable(
        EventLoop& loop,
        int fd,
        const sockaddr_in& peerAddress,
        std::chrono::steady_clock::time_point deadline,
        AsyncConnectControl* control)
        : loop_(loop)
        , fd_(fd)
        , peerAddress_(peerAddress)
        , deadline_(deadline)
        , control_(control)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    bool await_suspend(std::coroutine_handle<> continuation)
    {
        if (!loop_.isInLoopThread()) {
            immediateResult_.emplace(IoResult::failure(
                {ErrorCode::system, EPERM,
                 "connect submission must run on the owning EventLoop"}));
            return false;
        }
        if (control_ && control_->cancellationRequested()) {
            immediateResult_.emplace(IoResult::failure(
                {ErrorCode::cancelled, ECANCELED,
                 "connect cancelled before submission"}));
            return false;
        }

        auto operation = std::make_shared<ConnectOperation>(
            loop_.nextOperationId(), peerAddress_);
        operation_ = operation;
        if (!setRelativeTimeout(deadline_, operation->timeout)) {
            operation->reject(
                {ErrorCode::timedOut, ETIMEDOUT,
                 "connect deadline has expired"});
            return false;
        }

        operation->setContinuation(continuation);
        if (control_) {
            control_->attach(operation);
        }
        const auto submitted = loop_.submitOperation(
            operation, 2,
            [fd = fd_, operation, deadline = deadline_](
                std::span<io_uring_sqe*> sqes, IoOperation&) {
                io_uring_prep_connect(
                    sqes[0], fd,
                    reinterpret_cast<const sockaddr*>(&operation->peerAddress),
                    sizeof(operation->peerAddress));
                if (!setRelativeTimeout(deadline, operation->timeout)) {
                    operation->timeout = {};
                    operation->timeout.tv_nsec = 1;
                }
                io_uring_sqe_set_flags(sqes[0], IOSQE_IO_LINK);
                io_uring_prep_link_timeout(
                    sqes[1], &operation->timeout, 0);
            });
        if (submitted.disposition == EventLoop::SubmitDisposition::rejected
            && control_) {
            control_->detach();
        }
        return submitted.disposition
            != EventLoop::SubmitDisposition::rejected;
    }

    IoResult await_resume()
    {
        if (control_) {
            control_->detach();
        }
        if (immediateResult_) {
            return std::move(*immediateResult_);
        }
        return operation_->result();
    }

private:
    EventLoop& loop_;
    int fd_;
    sockaddr_in peerAddress_{};
    std::chrono::steady_clock::time_point deadline_;
    std::shared_ptr<ConnectOperation> operation_;
    std::optional<IoResult> immediateResult_;
    AsyncConnectControl* control_;
};

Result<std::shared_ptr<TcpConnection>> connectFailure(
    ErrorCode code, int errorNumber, std::string message)
{
    return Result<std::shared_ptr<TcpConnection>>::failure(
        {code, errorNumber, std::move(message)});
}

Task<Result<std::shared_ptr<TcpConnection>>> connectOwned(
    EventLoop& loop,
    sockaddr_in peerAddress,
    std::chrono::steady_clock::time_point deadline,
    std::string connectionName,
    AsyncConnectControl* control)
{
    if (!loop.isInLoopThread()) {
        co_return connectFailure(
            ErrorCode::system, EPERM,
            "connect must run on the owning EventLoop");
    }

    const int fd = ::socket(
        AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        const int errorNumber = errno;
        co_return connectFailure(
            ErrorCode::system, errorNumber,
            "failed to create outbound socket");
    }

    Socket socket(fd);
    if (!socket.setTcpNoDelay(true) || !socket.setKeepAlive(true)) {
        const int errorNumber = errno;
        co_return connectFailure(
            ErrorCode::system, errorNumber,
            "failed to configure outbound socket");
    }

    auto result = co_await ConnectAwaitable(
        loop, socket.getFd(), peerAddress, deadline, control);
    if (!result) {
        co_return Result<std::shared_ptr<TcpConnection>>::failure(
            result.error());
    }

    auto connection = std::make_shared<TcpConnection>(
        connectionName, &loop, socket.releaseFd(),
        InetAddress(peerAddress));
    connection->connectEstablished();
    co_return Result<std::shared_ptr<TcpConnection>>::success(
        std::move(connection));
}

} // namespace

AsyncConnectControl::AsyncConnectControl(EventLoop& loop) noexcept
    : loop_(loop)
{
}

void AsyncConnectControl::cancel()
{
    assert(loop_.isInLoopThread());
    cancellationRequested_ = true;
    if (auto operation = operation_.lock()) {
        loop_.cancelOperation(operation);
    }
}

bool AsyncConnectControl::cancellationRequested() const noexcept
{
    return cancellationRequested_;
}

void AsyncConnectControl::attach(
    const std::shared_ptr<IoOperation>& operation)
{
    assert(loop_.isInLoopThread());
    operation_ = operation;
}

void AsyncConnectControl::detach() noexcept
{
    operation_.reset();
}

Task<Result<std::shared_ptr<TcpConnection>>> asyncConnect(
    EventLoop& loop,
    const InetAddress& peer,
    std::chrono::steady_clock::time_point deadline,
    std::string connectionName,
    AsyncConnectControl* control)
{
    return connectOwned(
        loop, peer.getSockAddrIn(), deadline, std::move(connectionName),
        control);
}

} // namespace ucp
