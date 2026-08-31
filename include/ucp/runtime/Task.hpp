#pragma once

#include <coroutine>
#include <exception>
#include <iostream>
#include <optional>
#include <utility>

namespace ucp {

struct FinalAwaiter {
    bool await_ready() const noexcept { return false; }

    template <typename Promise>
    std::coroutine_handle<> await_suspend(
        std::coroutine_handle<Promise> handle) const noexcept
    {
        return handle.promise().continuation
            ? handle.promise().continuation
            : std::noop_coroutine();
    }

    void await_resume() const noexcept {}
};

template <typename T> class Task {
public:
    struct promise_type;
    using Handle = std::coroutine_handle<promise_type>;

    class Awaiter {
    public:
        explicit Awaiter(Handle handle) noexcept
            : handle_(handle)
        {
        }

        Awaiter(const Awaiter&) = delete;
        Awaiter& operator=(const Awaiter&) = delete;

        Awaiter(Awaiter&& other) noexcept
            : handle_(std::exchange(other.handle_, {}))
        {
        }

        Awaiter& operator=(Awaiter&& other) noexcept
        {
            if (this != &other) {
                destroy();
                handle_ = std::exchange(other.handle_, {});
            }
            return *this;
        }

        ~Awaiter()
        {
            destroy();
        }

        bool await_ready() const noexcept { return false; }

        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<> continuation) noexcept
        {
            handle_.promise().continuation = continuation;
            return handle_;
        }

        T await_resume()
        {
            if (handle_.promise().exception) {
                std::rethrow_exception(handle_.promise().exception);
            }
            return std::move(*handle_.promise().value);
        }

    private:
        void destroy() noexcept
        {
            if (handle_) {
                handle_.destroy();
                handle_ = {};
            }
        }

        Handle handle_;
    };

    struct promise_type {
        Task get_return_object() noexcept
        {
            return Task{Handle::from_promise(*this)};
        }

        std::suspend_always initial_suspend() const noexcept { return {}; }
        FinalAwaiter final_suspend() const noexcept { return {}; }

        void return_value(T value)
        {
            this->value.emplace(std::move(value));
        }

        void unhandled_exception() noexcept
        {
            exception = std::current_exception();
        }

        std::coroutine_handle<> continuation{};
        std::exception_ptr exception;
        std::optional<T> value;
    };

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    Task(Task&& other) noexcept
        : handle_(std::exchange(other.handle_, {}))
    {
    }

    Task& operator=(Task&& other) noexcept
    {
        if (this != &other) {
            destroy();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    ~Task()
    {
        destroy();
    }

    Awaiter operator co_await() && noexcept
    {
        return Awaiter{std::exchange(handle_, {})};
    }

    auto operator co_await() & = delete;
    auto operator co_await() const& = delete;

private:
    explicit Task(Handle handle) noexcept
        : handle_(handle)
    {
    }

    void destroy() noexcept
    {
        if (handle_) {
            handle_.destroy();
            handle_ = {};
        }
    }

    Handle handle_;
};

template <> class Task<void> {
public:
    struct promise_type;
    using Handle = std::coroutine_handle<promise_type>;

    class Awaiter {
    public:
        explicit Awaiter(Handle handle) noexcept
            : handle_(handle)
        {
        }

        Awaiter(const Awaiter&) = delete;
        Awaiter& operator=(const Awaiter&) = delete;

        Awaiter(Awaiter&& other) noexcept
            : handle_(std::exchange(other.handle_, {}))
        {
        }

        Awaiter& operator=(Awaiter&& other) noexcept
        {
            if (this != &other) {
                destroy();
                handle_ = std::exchange(other.handle_, {});
            }
            return *this;
        }

        ~Awaiter()
        {
            destroy();
        }

        bool await_ready() const noexcept { return false; }

        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<> continuation) noexcept
        {
            handle_.promise().continuation = continuation;
            return handle_;
        }

        void await_resume()
        {
            if (handle_.promise().exception) {
                std::rethrow_exception(handle_.promise().exception);
            }
        }

    private:
        void destroy() noexcept
        {
            if (handle_) {
                handle_.destroy();
                handle_ = {};
            }
        }

        Handle handle_;
    };

    struct promise_type {
        Task get_return_object() noexcept
        {
            return Task{Handle::from_promise(*this)};
        }

        std::suspend_always initial_suspend() const noexcept { return {}; }
        FinalAwaiter final_suspend() const noexcept { return {}; }
        void return_void() const noexcept {}

        void unhandled_exception() noexcept
        {
            exception = std::current_exception();
        }

        std::coroutine_handle<> continuation{};
        std::exception_ptr exception;
    };

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    Task(Task&& other) noexcept
        : handle_(std::exchange(other.handle_, {}))
    {
    }

    Task& operator=(Task&& other) noexcept
    {
        if (this != &other) {
            destroy();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    ~Task()
    {
        destroy();
    }

    Awaiter operator co_await() && noexcept
    {
        return Awaiter{std::exchange(handle_, {})};
    }

    auto operator co_await() & = delete;
    auto operator co_await() const& = delete;

private:
    explicit Task(Handle handle) noexcept
        : handle_(handle)
    {
    }

    void destroy() noexcept
    {
        if (handle_) {
            handle_.destroy();
            handle_ = {};
        }
    }

    Handle handle_;
};

class DetachedTask {
public:
    struct promise_type {
        DetachedTask get_return_object() const noexcept { return {}; }
        std::suspend_never initial_suspend() const noexcept { return {}; }
        std::suspend_never final_suspend() const noexcept { return {}; }
        void return_void() const noexcept {}

        void unhandled_exception() noexcept
        {
            try {
                throw;
            } catch (const std::exception& exception) {
                std::cerr << "DetachedTask exception: " << exception.what() << '\n';
            } catch (...) {
                std::cerr << "DetachedTask exception: unknown exception\n";
            }
        }
    };
};

} // namespace ucp
