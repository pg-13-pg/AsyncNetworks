#include "TestSupport.hpp"
#include "ucp/runtime/Task.hpp"
#include <stdexcept>

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

int main()
{
    int value = 0;
    bool caught = false;
    consumeValue(value);
    consumeFailure(caught);
    CHECK_EQ(value, 42);
    CHECK(caught);
    return 0;
}
