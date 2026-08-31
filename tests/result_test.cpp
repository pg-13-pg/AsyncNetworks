#include "TestSupport.hpp"
#include "ucp/runtime/Result.hpp"

int main()
{
    auto ok = ucp::Result<std::size_t>::success(7);
    CHECK(ok.hasValue());
    CHECK_EQ(ok.value(), 7U);

    auto eof = ucp::Result<std::size_t>::failure({ucp::ErrorCode::eof, 0});
    CHECK(!eof.hasValue());
    CHECK_EQ(eof.error().code, ucp::ErrorCode::eof);

    auto moved = ucp::Result<std::string>::success("payload");
    CHECK_EQ(std::move(moved).takeValue(), "payload");
    return 0;
}
