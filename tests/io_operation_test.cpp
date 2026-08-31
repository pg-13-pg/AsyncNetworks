#include "TestSupport.hpp"
#include "ucp/runtime/IoOperation.hpp"

#include <cerrno>

int main()
{
    ucp::IoOperation read(1, ucp::OperationType::read, true);
    read.arm(2);
    auto first = read.onCompletion(ucp::CompletionKind::timeout, -ETIME);
    CHECK(first.resume);
    CHECK(!first.drained);
    CHECK_EQ(read.result().error().code, ucp::ErrorCode::timedOut);
    auto loser = read.onCompletion(ucp::CompletionKind::io, -ECANCELED);
    CHECK(!loser.resume);
    CHECK(loser.drained);

    ucp::IoOperation race(2, ucp::OperationType::read, true);
    race.arm(2);
    CHECK(!race.onCompletion(ucp::CompletionKind::io, -ECANCELED).resume);
    CHECK(race.onCompletion(ucp::CompletionKind::timeout, -ETIME).resume);
    CHECK(race.drained());

    ucp::IoOperation cancel(3, ucp::OperationType::write, false);
    cancel.arm(1);
    CHECK(cancel.requestCancel());
    CHECK(cancel.onCompletion(ucp::CompletionKind::io, -ECANCELED).resume);
    CHECK_EQ(cancel.result().error().code, ucp::ErrorCode::cancelled);
    return 0;
}
