#pragma once

#include <cstddef>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>

namespace ucp {

enum class ErrorCode {
    none,
    eof,
    cancelled,
    timedOut,
    connectionReset,
    resourceExhausted,
    notConnected,
    protocol,
    system
};

struct Error {
    ErrorCode code{ErrorCode::none};
    int systemError{0};
    std::string message;
};

template <typename T> class Result {
public:
    static Result success(T value)
    {
        return Result(std::variant<T, Error>(std::in_place_type<T>, std::move(value)));
    }

    static Result failure(Error error)
    {
        return Result(std::variant<T, Error>(std::in_place_type<Error>, std::move(error)));
    }

    bool hasValue() const noexcept
    {
        return std::holds_alternative<T>(value_);
    }

    explicit operator bool() const noexcept
    {
        return hasValue();
    }

    T& value() &
    {
        if (!hasValue()) {
            throw std::logic_error("Result does not contain a value");
        }
        return std::get<T>(value_);
    }

    const T& value() const&
    {
        if (!hasValue()) {
            throw std::logic_error("Result does not contain a value");
        }
        return std::get<T>(value_);
    }

    T takeValue() &&
    {
        if (!hasValue()) {
            throw std::logic_error("Result does not contain a value");
        }
        return std::move(std::get<T>(value_));
    }

    const Error& error() const&
    {
        if (hasValue()) {
            throw std::logic_error("Result does not contain an error");
        }
        return std::get<Error>(value_);
    }

private:
    explicit Result(std::variant<T, Error> value)
        : value_(std::move(value))
    {
    }

    std::variant<T, Error> value_;
};

using IoResult = Result<std::size_t>;

} // namespace ucp
