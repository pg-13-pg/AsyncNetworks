#pragma once

#include <cstdint>

namespace ucp {

enum class CompletionDataKind : std::uint32_t {
    legacyContext,
    operationToken
};

struct CompletionData {
    static constexpr std::uint32_t magicValue = 0x55435031U;

    std::uint32_t magic{magicValue};
    CompletionDataKind kind;

protected:
    explicit CompletionData(CompletionDataKind value)
        : kind(value)
    {
    }
};

} // namespace ucp
