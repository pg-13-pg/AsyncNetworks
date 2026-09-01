#pragma once

#include "TestSupport.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

class MockHttpUpstream {
public:
    struct Request {
        std::string head;
        std::string body;
    };

    explicit MockHttpUpstream(std::vector<std::string> responseBodies)
        : responseBodies_(std::move(responseBodies))
    {
        listener_ = ::socket(
            AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        CHECK(listener_ >= 0);
        int reuse = 1;
        CHECK_EQ(::setsockopt(
            listener_, SOL_SOCKET, SO_REUSEADDR,
            &reuse, sizeof(reuse)), 0);

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        CHECK_EQ(::bind(
            listener_, reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)), 0);
        CHECK_EQ(::listen(listener_, 8), 0);
        socklen_t length = sizeof(address);
        CHECK_EQ(::getsockname(
            listener_, reinterpret_cast<sockaddr*>(&address), &length), 0);
        port_ = ntohs(address.sin_port);
        thread_ = std::thread([this] { run(); });
    }

    ~MockHttpUpstream()
    {
        stop_.store(true, std::memory_order_release);
        thread_.join();
        CHECK_EQ(::close(listener_), 0);
    }

    MockHttpUpstream(const MockHttpUpstream&) = delete;
    MockHttpUpstream& operator=(const MockHttpUpstream&) = delete;

    std::uint16_t port() const noexcept
    {
        return port_;
    }

    std::size_t accepts() const noexcept
    {
        return accepts_.load(std::memory_order_acquire);
    }

    std::vector<Request> requests() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return requests_;
    }

private:
    static std::size_t contentLength(std::string_view head)
    {
        std::string lower(head);
        std::transform(
            lower.begin(), lower.end(), lower.begin(),
            [](unsigned char ch) {
                return ch >= 'A' && ch <= 'Z'
                    ? static_cast<char>(ch + ('a' - 'A'))
                    : static_cast<char>(ch);
            });
        const std::string marker = "content-length:";
        const auto start = lower.find(marker);
        CHECK(start != std::string::npos);
        auto cursor = start + marker.size();
        while (cursor < lower.size() && lower[cursor] == ' ') {
            ++cursor;
        }
        std::size_t result = 0;
        CHECK(cursor < lower.size() && lower[cursor] >= '0'
              && lower[cursor] <= '9');
        while (cursor < lower.size()
               && lower[cursor] >= '0' && lower[cursor] <= '9') {
            result = result * 10
                + static_cast<std::size_t>(lower[cursor] - '0');
            ++cursor;
        }
        return result;
    }

    bool readRequest(int connection, std::string& pending, Request& request)
    {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        std::size_t headerBytes = std::string::npos;
        std::size_t bodyBytes = 0;
        while (!stop_.load(std::memory_order_acquire)
               && std::chrono::steady_clock::now() < deadline) {
            if (headerBytes == std::string::npos) {
                const auto headerEnd = pending.find("\r\n\r\n");
                if (headerEnd != std::string::npos) {
                    headerBytes = headerEnd + 4;
                    bodyBytes = contentLength(
                        std::string_view(pending).substr(0, headerBytes));
                }
            }
            if (headerBytes != std::string::npos
                && pending.size() >= headerBytes + bodyBytes) {
                request.head = pending.substr(0, headerBytes);
                request.body = pending.substr(headerBytes, bodyBytes);
                pending.erase(0, headerBytes + bodyBytes);
                return true;
            }

            char buffer[4096];
            const auto count = ::recv(
                connection, buffer, sizeof(buffer), 0);
            if (count > 0) {
                pending.append(buffer, static_cast<std::size_t>(count));
            } else if (count == 0) {
                return false;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK
                       || errno == EINTR) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            } else {
                CHECK(false);
            }
        }
        return false;
    }

    bool sendAll(int connection, std::string_view bytes)
    {
        while (!bytes.empty()
               && !stop_.load(std::memory_order_acquire)) {
            const auto count = ::send(
                connection, bytes.data(), bytes.size(), MSG_NOSIGNAL);
            if (count > 0) {
                bytes.remove_prefix(static_cast<std::size_t>(count));
            } else if (count < 0
                       && (errno == EAGAIN || errno == EWOULDBLOCK
                           || errno == EINTR)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            } else {
                return false;
            }
        }
        return bytes.empty();
    }

    bool sendFragmented(int connection, const std::string& body)
    {
        const std::string response =
            "HTTP/1.1 200 OK\r\nContent-Length: "
            + std::to_string(body.size())
            + "\r\nConnection: keep-alive\r\n\r\n" + body;
        constexpr std::size_t fragments[] = {1, 2, 3, 5};
        std::size_t offset = 0;
        std::size_t index = 0;
        while (offset < response.size()) {
            const auto size = std::min(
                fragments[index % 4], response.size() - offset);
            if (!sendAll(
                    connection,
                    std::string_view(response).substr(offset, size))) {
                return false;
            }
            offset += size;
            ++index;
        }
        return true;
    }

    void run()
    {
        int connection = -1;
        while (!stop_.load(std::memory_order_acquire)) {
            connection = ::accept4(
                listener_, nullptr, nullptr,
                SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (connection >= 0) {
                accepts_.fetch_add(1, std::memory_order_release);
                break;
            }
            CHECK(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (connection < 0) {
            return;
        }

        std::string pending;
        for (const auto& body : responseBodies_) {
            Request request;
            CHECK(readRequest(connection, pending, request));
            {
                std::lock_guard<std::mutex> lock(mutex_);
                requests_.push_back(std::move(request));
            }
            CHECK(sendFragmented(connection, body));
        }
        while (!stop_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        CHECK_EQ(::close(connection), 0);
    }

    int listener_{-1};
    std::uint16_t port_{0};
    std::vector<std::string> responseBodies_;
    mutable std::mutex mutex_;
    std::vector<Request> requests_;
    std::atomic_bool stop_{false};
    std::atomic_size_t accepts_{0};
    std::thread thread_;
};

class FaultHttpUpstream {
public:
    enum class Mode {
        normal,
        malformedResponse,
        delayedResponse,
        partialResponse,
        holdResponse,
        stalledUpload
    };

    explicit FaultHttpUpstream(
        Mode mode,
        std::chrono::milliseconds delay = std::chrono::milliseconds::zero())
        : mode_(mode), delay_(delay)
    {
        listener_ = ::socket(
            AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        CHECK(listener_ >= 0);
        int reuse = 1;
        CHECK_EQ(::setsockopt(
            listener_, SOL_SOCKET, SO_REUSEADDR,
            &reuse, sizeof(reuse)), 0);
        if (mode_ == Mode::stalledUpload) {
            int receiveBuffer = 1024;
            CHECK_EQ(::setsockopt(
                listener_, SOL_SOCKET, SO_RCVBUF,
                &receiveBuffer, sizeof(receiveBuffer)), 0);
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        CHECK_EQ(::bind(
            listener_, reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)), 0);
        CHECK_EQ(::listen(listener_, 8), 0);
        socklen_t length = sizeof(address);
        CHECK_EQ(::getsockname(
            listener_, reinterpret_cast<sockaddr*>(&address), &length), 0);
        port_ = ntohs(address.sin_port);
        thread_ = std::thread([this] { run(); });
    }

    ~FaultHttpUpstream()
    {
        stop_.store(true, std::memory_order_release);
        thread_.join();
        CHECK_EQ(::close(listener_), 0);
    }

    FaultHttpUpstream(const FaultHttpUpstream&) = delete;
    FaultHttpUpstream& operator=(const FaultHttpUpstream&) = delete;

    std::uint16_t port() const noexcept { return port_; }
    std::size_t accepts() const noexcept
    {
        return accepts_.load(std::memory_order_acquire);
    }
    std::size_t requests() const noexcept
    {
        return requests_.load(std::memory_order_acquire);
    }
    std::size_t uploadedBodyBytes() const noexcept
    {
        return uploadedBodyBytes_.load(std::memory_order_acquire);
    }
    bool peerClosed() const noexcept
    {
        return peerClosed_.load(std::memory_order_acquire);
    }
    void resumeUploadReads() noexcept
    {
        resumeUploadReads_.store(true, std::memory_order_release);
    }

private:
    bool sendBytes(int connection, std::string_view bytes)
    {
        while (!bytes.empty() && !stop_.load(std::memory_order_acquire)) {
            const auto written = ::send(
                connection, bytes.data(), bytes.size(), MSG_NOSIGNAL);
            if (written > 0) {
                bytes.remove_prefix(static_cast<std::size_t>(written));
            } else if (written < 0 && (errno == EAGAIN
                       || errno == EWOULDBLOCK || errno == EINTR)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            } else {
                return false;
            }
        }
        return bytes.empty();
    }

    void readUntilPeerClose(int connection)
    {
        char buffer[4096];
        while (!stop_.load(std::memory_order_acquire)) {
            const auto count = ::recv(connection, buffer, sizeof(buffer), 0);
            if (count > 0) {
                uploadedBodyBytes_.fetch_add(
                    static_cast<std::size_t>(count),
                    std::memory_order_release);
            } else if (count == 0) {
                peerClosed_.store(true, std::memory_order_release);
                return;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK
                       || errno == EINTR) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            } else {
                peerClosed_.store(true, std::memory_order_release);
                return;
            }
        }
    }

    void drainStalledUpload(int connection)
    {
        while (!stop_.load(std::memory_order_acquire)
               && !resumeUploadReads_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (!stop_.load(std::memory_order_acquire)) {
            readUntilPeerClose(connection);
        }
    }

    void run()
    {
        int connection = -1;
        while (!stop_.load(std::memory_order_acquire)) {
            connection = ::accept4(
                listener_, nullptr, nullptr,
                SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (connection >= 0) {
                accepts_.fetch_add(1, std::memory_order_release);
                break;
            }
            CHECK(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (connection < 0) {
            return;
        }
        if (mode_ == Mode::stalledUpload) {
            int receiveBuffer = 1024;
            CHECK_EQ(::setsockopt(
                connection, SOL_SOCKET, SO_RCVBUF,
                &receiveBuffer, sizeof(receiveBuffer)), 0);
        }

        std::string request;
        while (!stop_.load(std::memory_order_acquire)
               && request.find("\r\n\r\n") == std::string::npos) {
            char buffer[4096];
            const auto count = ::recv(connection, buffer, sizeof(buffer), 0);
            if (count > 0) {
                request.append(buffer, static_cast<std::size_t>(count));
            } else if (count == 0) {
                peerClosed_.store(true, std::memory_order_release);
                break;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK
                       || errno == EINTR) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            } else {
                break;
            }
        }
        if (request.find("\r\n\r\n") != std::string::npos) {
            requests_.fetch_add(1, std::memory_order_release);
            const auto bodyStart = request.find("\r\n\r\n") + 4;
            uploadedBodyBytes_.fetch_add(
                request.size() - bodyStart, std::memory_order_release);
        }

        if (mode_ == Mode::holdResponse) {
            readUntilPeerClose(connection);
        } else if (mode_ == Mode::stalledUpload) {
            drainStalledUpload(connection);
        } else {
            const auto sendAt = std::chrono::steady_clock::now() + delay_;
            while (!stop_.load(std::memory_order_acquire)
                   && std::chrono::steady_clock::now() < sendAt) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            if (mode_ == Mode::malformedResponse) {
                (void)sendBytes(
                    connection,
                    "BROKEN\r\nContent-Length: 0\r\n\r\n");
            } else if (mode_ == Mode::partialResponse) {
                (void)sendBytes(
                    connection,
                    "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n"
                    "Connection: close\r\n\r\nabc");
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                linger reset{1, 0};
                CHECK_EQ(::setsockopt(
                    connection, SOL_SOCKET, SO_LINGER,
                    &reset, sizeof(reset)), 0);
            } else {
                (void)sendBytes(
                    connection,
                    "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n"
                    "Connection: close\r\n\r\nok");
            }
        }
        CHECK_EQ(::close(connection), 0);
        peerClosed_.store(true, std::memory_order_release);
    }

    int listener_{-1};
    std::uint16_t port_{0};
    Mode mode_;
    std::chrono::milliseconds delay_;
    std::atomic_bool stop_{false};
    std::atomic_bool peerClosed_{false};
    std::atomic_bool resumeUploadReads_{false};
    std::atomic_size_t accepts_{0};
    std::atomic_size_t requests_{0};
    std::atomic_size_t uploadedBodyBytes_{0};
    std::thread thread_;
};
