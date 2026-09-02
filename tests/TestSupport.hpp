#pragma once

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <utility>

namespace TestSupport {

template <typename Predicate>
bool waitUntil(Predicate&& predicate,
               std::chrono::milliseconds timeout,
               std::chrono::milliseconds pollInterval =
                   std::chrono::milliseconds(1))
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (std::forward<Predicate>(predicate)()) {
            return true;
        }
        std::this_thread::sleep_for(pollInterval);
    }
    return std::forward<Predicate>(predicate)();
}

} // namespace TestSupport

#define CHECK(expression) \
    do { \
        if (!(expression)) { \
            std::cerr << __FILE__ << ':' << __LINE__ \
                      << ": CHECK failed: " #expression << '\n'; \
            std::abort(); \
        } \
    } while (false)

#define CHECK_EQ(lhs, rhs) \
    do { \
        if (!((lhs) == (rhs))) { \
            std::cerr << __FILE__ << ':' << __LINE__ \
                      << ": CHECK_EQ failed: " #lhs " == " #rhs << '\n'; \
            std::abort(); \
        } \
    } while (false)
