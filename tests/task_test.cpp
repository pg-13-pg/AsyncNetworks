#include "TestSupport.hpp"
#include "ucp/runtime/Task.hpp"
#include <array>
#include <cstdio>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unistd.h>

class ThrowingStreamBuffer : public std::streambuf {
protected:
    int_type overflow(int_type) override
    {
        throw std::runtime_error("stderr stream failure");
    }
};

ucp::Task<int> valueTask() { co_return 42; }
ucp::Task<int> failingTask()
{
    throw std::runtime_error("task failure");
    co_return 0;
}
ucp::DetachedTask consumeValue(int& value)
{
    value = co_await valueTask();
}
ucp::DetachedTask consumeFailure(bool& caught)
{
    try { (void)co_await failingTask(); }
    catch (const std::runtime_error&) { caught = true; }
}
ucp::DetachedTask detachedFailure()
{
    throw std::runtime_error("detached failure");
    co_return;
}

int main()
{
    int value = 0;
    bool caught = false;
    consumeValue(value);
    consumeFailure(caught);
    CHECK_EQ(value, 42);
    CHECK(caught);

    int stderrPipe[2];
    CHECK_EQ(::pipe(stderrPipe), 0);
    const int originalStderr = ::dup(STDERR_FILENO);
    CHECK(originalStderr >= 0);
    CHECK_EQ(::dup2(stderrPipe[1], STDERR_FILENO), STDERR_FILENO);
    ::close(stderrPipe[1]);

    ThrowingStreamBuffer throwingBuffer;
    auto* originalBuffer = std::cerr.rdbuf(&throwingBuffer);
    const auto originalExceptions = std::cerr.exceptions();
    std::cerr.exceptions(std::ios::badbit);
    detachedFailure();
    std::cerr.exceptions(std::ios::goodbit);
    std::cerr.rdbuf(originalBuffer);
    std::cerr.clear();
    std::cerr.exceptions(originalExceptions);

    std::fflush(stderr);
    CHECK_EQ(::dup2(originalStderr, STDERR_FILENO), STDERR_FILENO);
    ::close(originalStderr);

    std::array<char, 128> diagnostic{};
    const auto diagnosticSize = ::read(
        stderrPipe[0], diagnostic.data(), diagnostic.size());
    ::close(stderrPipe[0]);
    CHECK(diagnosticSize > 0);
    CHECK_EQ(std::string(diagnostic.data(), diagnosticSize),
             "DetachedTask exception: detached failure\n");
    return 0;
}
