#include "ucp/runtime/IoOperation.hpp"

#include <cassert>
#include <cerrno>
#include <stdexcept>
#include <utility>

namespace ucp {
namespace {

Error errorFromKernelResult(int kernelResult)
{
    const int errorNumber = -kernelResult;
    switch (errorNumber) {
    case ECANCELED:
        return {ErrorCode::cancelled, errorNumber, "operation cancelled"};
    case ETIME:
    case ETIMEDOUT:
        return {ErrorCode::timedOut, errorNumber, "operation timed out"};
    case ECONNRESET:
    case EPIPE:
        return {ErrorCode::connectionReset, errorNumber, "connection reset"};
    case ENOTCONN:
        return {ErrorCode::notConnected, errorNumber, "socket not connected"};
    case EAGAIN:
    case ENOBUFS:
    case ENOMEM:
        return {ErrorCode::resourceExhausted, errorNumber, "local io resource exhausted"};
    default:
        return {ErrorCode::system, errorNumber, "io operation failed"};
    }
}

} // namespace

CompletionToken::CompletionToken(
    IoOperation* owner, CompletionKind value) noexcept
    : CompletionData(CompletionDataKind::operationToken)
    , operation(owner)
    , completionKind(value)
{
}

IoOperation::IoOperation(
    std::uint64_t id, OperationType type, bool linkedTimeout)
    : id_(id)
    , type_(type)
    , linkedTimeout_(linkedTimeout)
    , result_(IoResult::failure(
          {ErrorCode::system, 0, "operation has not completed"}))
    , ioToken_(this, CompletionKind::io)
    , timeoutToken_(this, CompletionKind::timeout)
    , cancelToken_(this, CompletionKind::cancel)
{
}

void IoOperation::setContinuation(
    std::coroutine_handle<> continuation) noexcept
{
    assert(!completionCallback_);
    continuation_ = continuation;
}

void IoOperation::setCompletionCallback(
    std::function<void(const IoResult&)> callback)
{
    if (continuation_) {
        throw std::logic_error(
            "an IoOperation cannot have both a callback and a continuation");
    }
    completionCallback_ = std::move(callback);
}

void IoOperation::arm(unsigned expectedCqes)
{
    if (expectedCqes == 0 || pendingCqes_ != 0) {
        throw std::logic_error("IoOperation must be armed once with CQEs");
    }
    pendingCqes_ = expectedCqes;
}

void IoOperation::addExpectedCqe() noexcept
{
    ++pendingCqes_;
}

bool IoOperation::requestCancel() noexcept
{
    if (externallyCompleted_ || cancelRequested_) {
        return false;
    }
    cancelRequested_ = true;
    return true;
}

CompletionDecision IoOperation::onCompletion(
    CompletionKind kind, int kernelResult)
{
    if (pendingCqes_ == 0) {
        return {false, true};
    }
    --pendingCqes_;

    bool resume = false;
    if (!externallyCompleted_ && kind != CompletionKind::cancel) {
        if (kind == CompletionKind::io && kernelResult >= 0) {
            if (type_ == OperationType::read && kernelResult == 0) {
                resume = selectResult(IoResult::failure(
                    {ErrorCode::eof, 0, "end of stream"}));
            } else {
                resume = selectResult(IoResult::success(
                    static_cast<std::size_t>(kernelResult)));
            }
        } else if (cancelRequested_
                   && kind == CompletionKind::io
                   && kernelResult < 0) {
            resume = selectResult(IoResult::failure(
                {ErrorCode::cancelled, ECANCELED, "operation cancelled"}));
        } else if (kind == CompletionKind::timeout) {
            if (kernelResult == -ETIME || kernelResult == -ETIMEDOUT) {
                resume = selectResult(IoResult::failure(
                    {ErrorCode::timedOut, -kernelResult, "operation timed out"}));
            } else if (kernelResult != -ECANCELED) {
                resume = selectResult(
                    IoResult::failure(errorFromKernelResult(kernelResult)));
            }
        } else if (!(linkedTimeout_ && kernelResult == -ECANCELED)) {
            resume = selectResult(
                IoResult::failure(errorFromKernelResult(kernelResult)));
        }
    }

    if (!externallyCompleted_ && pendingCqes_ == 0) {
        resume = selectResult(IoResult::failure(
            {ErrorCode::cancelled, ECANCELED, "operation cancelled"}));
    }
    return {resume, pendingCqes_ == 0};
}

void IoOperation::reject(Error error)
{
    if (!externallyCompleted_) {
        selectResult(IoResult::failure(std::move(error)));
    }
}

std::uint64_t IoOperation::id() const noexcept
{
    return id_;
}

const IoResult& IoOperation::result() const noexcept
{
    return result_;
}

std::coroutine_handle<> IoOperation::takeContinuation() noexcept
{
    return std::exchange(continuation_, {});
}

std::function<void(const IoResult&)> IoOperation::takeCompletionCallback()
{
    return std::exchange(completionCallback_, {});
}

bool IoOperation::externallyCompleted() const noexcept
{
    return externallyCompleted_;
}

bool IoOperation::drained() const noexcept
{
    return pendingCqes_ == 0;
}

CompletionToken& IoOperation::ioToken() noexcept
{
    return ioToken_;
}

CompletionToken& IoOperation::timeoutToken() noexcept
{
    return timeoutToken_;
}

CompletionToken& IoOperation::cancelToken() noexcept
{
    return cancelToken_;
}

bool IoOperation::selectResult(IoResult result)
{
    if (externallyCompleted_) {
        return false;
    }
    result_ = std::move(result);
    externallyCompleted_ = true;
    return true;
}

} // namespace ucp
