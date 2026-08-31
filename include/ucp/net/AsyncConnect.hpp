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

Task<Result<std::shared_ptr<TcpConnection>>> asyncConnect(
    EventLoop& loop,
    const InetAddress& peer,
    std::chrono::steady_clock::time_point deadline,
    std::string connectionName);

} // namespace ucp
