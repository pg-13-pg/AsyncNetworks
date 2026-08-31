#pragma once

#include "ucp/runtime/IoOperation.hpp"
#include "ucp/runtime/Task.hpp"

#include <chrono>
#include <coroutine>
#include <cstddef>
#include <memory>
#include <optional>
#include <span>

class TcpConnection;

namespace ucp {

using Deadline = std::optional<std::chrono::steady_clock::time_point>;

class ReadSomeAwaitable {
public:
    ReadSomeAwaitable(std::shared_ptr<TcpConnection> connection,
                      std::span<std::byte> buffer,
                      Deadline deadline);

    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> continuation);
    IoResult await_resume();

private:
    std::shared_ptr<TcpConnection> connection_;
    std::span<std::byte> buffer_;
    Deadline deadline_;
    std::shared_ptr<IoOperation> operation_;
    std::optional<IoResult> immediateResult_;
    bool tracked_{false};
};

class WriteSomeAwaitable {
public:
    WriteSomeAwaitable(std::shared_ptr<TcpConnection> connection,
                       std::span<const std::byte> buffer,
                       Deadline deadline);

    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> continuation);
    IoResult await_resume();

private:
    std::shared_ptr<TcpConnection> connection_;
    std::span<const std::byte> buffer_;
    Deadline deadline_;
    std::shared_ptr<IoOperation> operation_;
    std::optional<IoResult> immediateResult_;
    bool tracked_{false};
};

ReadSomeAwaitable asyncReadSome(std::shared_ptr<TcpConnection> connection,
                                std::span<std::byte> buffer,
                                Deadline deadline = std::nullopt);

WriteSomeAwaitable asyncWriteSome(std::shared_ptr<TcpConnection> connection,
                                  std::span<const std::byte> buffer,
                                  Deadline deadline = std::nullopt);

Task<IoResult> asyncWriteAll(std::shared_ptr<TcpConnection> connection,
                             std::span<const std::byte> buffer,
                             Deadline deadline = std::nullopt);

} // namespace ucp
