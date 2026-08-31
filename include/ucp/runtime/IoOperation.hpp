#pragma once

#include "ucp/runtime/CompletionData.hpp"
#include "ucp/runtime/Result.hpp"

#include <coroutine>
#include <cstdint>
#include <functional>

namespace ucp {

enum class OperationType {
    read,
    write,
    connect,
    accept,
    cancel
};

enum class CompletionKind {
    io,
    timeout,
    cancel
};

struct CompletionDecision {
    bool resume;
    bool drained;
};

class IoOperation;

struct CompletionToken : CompletionData {
    CompletionToken(IoOperation* owner, CompletionKind value) noexcept;

    IoOperation* operation;
    CompletionKind completionKind;
};

class IoOperation {
public:
    IoOperation(std::uint64_t id, OperationType type, bool linkedTimeout);

    void setContinuation(std::coroutine_handle<> continuation) noexcept;
    void setCompletionCallback(std::function<void(const IoResult&)> callback);
    void arm(unsigned expectedCqes);
    void addExpectedCqe() noexcept;
    bool requestCancel() noexcept;
    CompletionDecision onCompletion(CompletionKind kind, int kernelResult);
    void reject(Error error);

    std::uint64_t id() const noexcept;
    const IoResult& result() const noexcept;
    std::coroutine_handle<> takeContinuation() noexcept;
    std::function<void(const IoResult&)> takeCompletionCallback();
    bool externallyCompleted() const noexcept;
    bool drained() const noexcept;

    CompletionToken& ioToken() noexcept;
    CompletionToken& timeoutToken() noexcept;
    CompletionToken& cancelToken() noexcept;

private:
    bool selectResult(IoResult result);

    std::uint64_t id_;
    OperationType type_;
    bool linkedTimeout_;
    bool cancelRequested_{false};
    bool externallyCompleted_{false};
    unsigned pendingCqes_{0};
    IoResult result_;
    std::coroutine_handle<> continuation_{};
    std::function<void(const IoResult&)> completionCallback_;
    CompletionToken ioToken_;
    CompletionToken timeoutToken_;
    CompletionToken cancelToken_;
};

} // namespace ucp
