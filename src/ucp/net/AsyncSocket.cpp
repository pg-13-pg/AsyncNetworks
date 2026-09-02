#include "ucp/net/AsyncSocket.hpp"

#include "EventLoop.hpp"
#include "TcpConnection.hpp"

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <liburing.h>
#include <poll.h>
#include <sys/socket.h>
#include <utility>

namespace ucp {
namespace {

class SocketOperation final : public IoOperation {
public:
    SocketOperation(std::uint64_t id, OperationType type, bool linkedTimeout)
        : IoOperation(id, type, linkedTimeout)
    {
    }

    __kernel_timespec timeout{};
};

IoResult notConnectedResult()
{
    return IoResult::failure(
        {ErrorCode::notConnected, ENOTCONN, "socket is not connected"});
}

bool setRelativeTimeout(Deadline deadline, __kernel_timespec& timeout)
{
    if (!deadline) {
        return true;
    }

    const auto remaining = std::chrono::duration_cast<std::chrono::nanoseconds>(
        *deadline - std::chrono::steady_clock::now());
    if (remaining <= std::chrono::nanoseconds::zero()) {
        return false;
    }

    constexpr auto nanosecondsPerSecond = 1'000'000'000LL;
    timeout.tv_sec = remaining.count() / nanosecondsPerSecond;
    timeout.tv_nsec = remaining.count() % nanosecondsPerSecond;
    return true;
}

} // namespace

ReadSomeAwaitable::ReadSomeAwaitable(
    std::shared_ptr<TcpConnection> connection,
    std::span<std::byte> buffer,
    Deadline deadline)
    : connection_(std::move(connection))
    , buffer_(buffer)
    , deadline_(deadline)
{
}

bool ReadSomeAwaitable::await_ready() const noexcept
{
    return false;
}

bool ReadSomeAwaitable::await_suspend(
    std::coroutine_handle<> continuation)
{
    if (!connection_ || !connection_->getLoop()) {
        immediateResult_.emplace(notConnectedResult());
        return false;
    }
    if (!connection_->checkConnected()) {
        immediateResult_.emplace(notConnectedResult());
        return false;
    }
    if (buffer_.empty()) {
        immediateResult_.emplace(IoResult::success(0));
        return false;
    }

    EventLoop* loop = connection_->getLoop();
    const auto operationId = loop->isInLoopThread()
        ? loop->nextOperationId()
        : 0;
    auto operation = std::make_shared<SocketOperation>(
        operationId, OperationType::read, deadline_.has_value());
    operation_ = operation;

    if (!setRelativeTimeout(deadline_, operation->timeout)) {
        operation->reject(
            {ErrorCode::timedOut, ETIMEDOUT,
             "operation deadline has expired"});
        return false;
    }

    operation->setContinuation(continuation);
    if (loop->isInLoopThread()) {
        connection_->trackOperation(operation);
        tracked_ = true;
    }

    const auto submitted = loop->submitOperation(
        operation, deadline_ ? 2U : 1U,
        [fd = connection_->fd(), buffer = buffer_, operation,
         deadline = deadline_](
            std::span<io_uring_sqe*> sqes, IoOperation&) {
            io_uring_prep_recv(
                sqes[0], fd, buffer.data(), buffer.size(), 0);
            if (deadline) {
                if (!setRelativeTimeout(deadline, operation->timeout)) {
                    operation->timeout = {};
                    operation->timeout.tv_nsec = 1;
                }
                io_uring_sqe_set_flags(sqes[0], IOSQE_IO_LINK);
                io_uring_prep_link_timeout(
                    sqes[1], &operation->timeout, 0);
            }
        });
    return submitted.disposition != EventLoop::SubmitDisposition::rejected;
}

IoResult ReadSomeAwaitable::await_resume()
{
    if (tracked_) {
        connection_->untrackOperation(operation_->id());
        tracked_ = false;
    }
    if (immediateResult_) {
        return std::move(*immediateResult_);
    }
    return operation_->result();
}

WriteSomeAwaitable::WriteSomeAwaitable(
    std::shared_ptr<TcpConnection> connection,
    std::span<const std::byte> buffer,
    Deadline deadline)
    : connection_(std::move(connection))
    , buffer_(buffer)
    , deadline_(deadline)
{
}

bool WriteSomeAwaitable::await_ready() const noexcept
{
    return false;
}

bool WriteSomeAwaitable::await_suspend(
    std::coroutine_handle<> continuation)
{
    if (!connection_ || !connection_->getLoop()) {
        immediateResult_.emplace(notConnectedResult());
        return false;
    }
    if (!connection_->checkConnected()) {
        immediateResult_.emplace(notConnectedResult());
        return false;
    }
    if (buffer_.empty()) {
        immediateResult_.emplace(IoResult::success(0));
        return false;
    }

    EventLoop* loop = connection_->getLoop();
    const auto operationId = loop->isInLoopThread()
        ? loop->nextOperationId()
        : 0;
    auto operation = std::make_shared<SocketOperation>(
        operationId, OperationType::write, deadline_.has_value());
    operation_ = operation;

    if (!setRelativeTimeout(deadline_, operation->timeout)) {
        operation->reject(
            {ErrorCode::timedOut, ETIMEDOUT,
             "operation deadline has expired"});
        return false;
    }

    operation->setContinuation(continuation);
    if (loop->isInLoopThread()) {
        connection_->trackOperation(operation);
        tracked_ = true;
    }

    const auto submitted = loop->submitOperation(
        operation, deadline_ ? 2U : 1U,
        [fd = connection_->fd(), buffer = buffer_, operation,
         deadline = deadline_](
            std::span<io_uring_sqe*> sqes, IoOperation&) {
            io_uring_prep_send(
                sqes[0], fd, buffer.data(), buffer.size(), MSG_NOSIGNAL);
            if (deadline) {
                if (!setRelativeTimeout(deadline, operation->timeout)) {
                    operation->timeout = {};
                    operation->timeout.tv_nsec = 1;
                }
                io_uring_sqe_set_flags(sqes[0], IOSQE_IO_LINK);
                io_uring_prep_link_timeout(
                    sqes[1], &operation->timeout, 0);
            }
        });
    return submitted.disposition != EventLoop::SubmitDisposition::rejected;
}

IoResult WriteSomeAwaitable::await_resume()
{
    if (tracked_) {
        connection_->untrackOperation(operation_->id());
        tracked_ = false;
    }
    if (immediateResult_) {
        return std::move(*immediateResult_);
    }
    return operation_->result();
}

PeerCloseAwaitable::PeerCloseAwaitable(
    std::shared_ptr<TcpConnection> connection)
    : connection_(std::move(connection))
{
}

bool PeerCloseAwaitable::await_ready() const noexcept
{
    return false;
}

bool PeerCloseAwaitable::await_suspend(
    std::coroutine_handle<> continuation)
{
    if (!connection_ || !connection_->getLoop()
        || !connection_->checkConnected()) {
        immediateResult_.emplace(notConnectedResult());
        return false;
    }

    EventLoop* loop = connection_->getLoop();
    const auto operationId = loop->isInLoopThread()
        ? loop->nextOperationId()
        : 0;
    auto operation = std::make_shared<SocketOperation>(
        operationId, OperationType::poll, false);
    operation_ = operation;
    operation->setContinuation(continuation);
    if (loop->isInLoopThread()) {
        connection_->trackOperation(operation);
        tracked_ = true;
    }

    const auto submitted = loop->submitOperation(
        operation, 1,
        [fd = connection_->fd()](
            std::span<io_uring_sqe*> sqes, IoOperation&) {
            io_uring_prep_poll_add(
                sqes[0], fd, POLLRDHUP | POLLHUP | POLLERR);
        });
    return submitted.disposition != EventLoop::SubmitDisposition::rejected;
}

IoResult PeerCloseAwaitable::await_resume()
{
    if (tracked_) {
        connection_->untrackOperation(operation_->id());
        tracked_ = false;
    }
    if (immediateResult_) {
        return std::move(*immediateResult_);
    }
    return operation_->result();
}

ReadSomeAwaitable asyncReadSome(
    std::shared_ptr<TcpConnection> connection,
    std::span<std::byte> buffer,
    Deadline deadline)
{
    return ReadSomeAwaitable(
        std::move(connection), buffer, deadline);
}

WriteSomeAwaitable asyncWriteSome(
    std::shared_ptr<TcpConnection> connection,
    std::span<const std::byte> buffer,
    Deadline deadline)
{
    return WriteSomeAwaitable(
        std::move(connection), buffer, deadline);
}

PeerCloseAwaitable asyncWaitPeerClose(
    std::shared_ptr<TcpConnection> connection)
{
    return PeerCloseAwaitable(std::move(connection));
}

Task<IoResult> asyncWriteAll(
    std::shared_ptr<TcpConnection> connection,
    std::span<const std::byte> buffer,
    Deadline deadline)
{
    std::size_t written = 0;
    while (!buffer.empty()) {
        auto result = co_await asyncWriteSome(connection, buffer, deadline);
        if (!result) {
            co_return IoResult::failure(result.error());
        }

        const std::size_t count = result.value();
        if (count == 0) {
            co_return IoResult::failure(
                {ErrorCode::connectionReset, ECONNRESET,
                 "socket write made no progress"});
        }
        if (count > buffer.size()) {
            co_return IoResult::failure(
                {ErrorCode::system, EIO,
                 "socket write exceeded the submitted buffer"});
        }

        written += count;
        buffer = buffer.subspan(count);
    }
    co_return IoResult::success(written);
}

} // namespace ucp
