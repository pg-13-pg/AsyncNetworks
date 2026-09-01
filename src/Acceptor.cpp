#include "Acceptor.hpp"

#include "EventLoop.hpp"
#include "InetAddress.hpp"
#include "Logger.hpp"
#include "Socket.hpp"
#include "ucp/runtime/IoOperation.hpp"

#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <liburing.h>
#include <memory>
#include <span>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

namespace
{
class AcceptOperation final : public ucp::IoOperation
{
  public:
    explicit AcceptOperation(std::uint64_t id)
        : IoOperation(id, ucp::OperationType::accept, false)
    {
    }

    sockaddr_in peerAddress{};
    socklen_t peerAddressLength{sizeof(peerAddress)};
};

int createNonblockingSocket()
{
    const int fd = ::socket(
        AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
    if (fd < 0)
    {
        LOG_ERROR("Acceptor socket create failed: {}", std::strerror(errno));
        std::abort();
    }
    return fd;
}
} // namespace

struct Acceptor::State : std::enable_shared_from_this<Acceptor::State>
{
    State(EventLoop *owner, const InetAddress &listenAddress, bool reuseport)
        : loop(owner), listenSocket(createNonblockingSocket())
    {
        listenSocket.setReuseAddr(true);
        listenSocket.setReusePort(reuseport);
        listenSocket.bindAddress(listenAddress);
    }

    ~State()
    {
        listening.store(false, std::memory_order_release);
        newConnectionCallback = {};
        listenSocket.closeFd();
    }

    void listen()
    {
        if (!loop->isInLoopThread()
            || listening.exchange(true, std::memory_order_acq_rel))
        {
            return;
        }
        listenSocket.listen();
        asyncAccept();
    }

    void stop()
    {
        if (loop->isInLoopThread())
        {
            stopInLoop();
            return;
        }
        std::weak_ptr<State> weak = shared_from_this();
        loop->queueControlInLoop([weak] {
            if (auto state = weak.lock())
            {
                state->stopInLoop();
            }
        });
    }

    void stopInLoop()
    {
        if (!listening.exchange(false, std::memory_order_acq_rel))
        {
            return;
        }
        auto operation = std::exchange(acceptOperation, nullptr);
        if (operation)
        {
            loop->cancelOperation(operation);
        }
        listenSocket.closeFd();
    }

    void asyncAccept()
    {
        if (!listening.load(std::memory_order_acquire)
            || listenSocket.getFd() < 0 || acceptOperation)
        {
            return;
        }

        auto operation = std::make_shared<AcceptOperation>(
            loop->nextOperationId());
        AcceptOperation *request = operation.get();
        std::weak_ptr<State> weak = shared_from_this();
        operation->setCompletionCallback(
            [weak, request](const ucp::IoResult &result) {
                if (auto state = weak.lock())
                {
                    state->handleRead(result, request->peerAddress);
                }
                else if (result)
                {
                    ::close(static_cast<int>(result.value()));
                }
            });
        acceptOperation = operation;

        const int listenFd = listenSocket.getFd();
        const auto submitted = loop->submitOperation(
            operation, 1,
            [listenFd, operation](std::span<io_uring_sqe *> sqes,
                                  ucp::IoOperation &) {
                auto *request =
                    static_cast<AcceptOperation *>(operation.get());
                request->peerAddress = {};
                request->peerAddressLength = sizeof(request->peerAddress);
                io_uring_prep_accept(
                    sqes[0], listenFd,
                    reinterpret_cast<sockaddr *>(&request->peerAddress),
                    &request->peerAddressLength,
                    SOCK_NONBLOCK | SOCK_CLOEXEC);
            });
        if (submitted.disposition == EventLoop::SubmitDisposition::rejected)
        {
            acceptOperation.reset();
            LOG_WARN("Acceptor accept submission rejected: {}",
                     submitted.error.message);
            loop->queueControlInLoop([weak] {
                if (auto state = weak.lock())
                {
                    state->asyncAccept();
                }
            });
        }
    }

    void handleRead(
        const ucp::IoResult &result, const sockaddr_in &peerAddress)
    {
        acceptOperation.reset();
        const bool accepting = listening.load(std::memory_order_acquire);
        if (result)
        {
            const int connection = static_cast<int>(result.value());
            if (accepting && newConnectionCallback)
            {
                newConnectionCallback(connection, InetAddress(peerAddress));
            }
            else
            {
                ::close(connection);
            }

            while (listening.load(std::memory_order_acquire))
            {
                sockaddr_in nextAddress{};
                socklen_t nextLength = sizeof(nextAddress);
                const int nextConnection = ::accept4(
                    listenSocket.getFd(),
                    reinterpret_cast<sockaddr *>(&nextAddress), &nextLength,
                    SOCK_NONBLOCK | SOCK_CLOEXEC);
                if (nextConnection >= 0)
                {
                    if (newConnectionCallback)
                    {
                        newConnectionCallback(
                            nextConnection, InetAddress(nextAddress));
                    }
                    else
                    {
                        ::close(nextConnection);
                    }
                }
                else if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    break;
                }
                else if (errno != EINTR)
                {
                    LOG_ERROR("Acceptor accept4 failed: {}",
                              std::strerror(errno));
                    break;
                }
            }
        }
        else if (result.error().code != ucp::ErrorCode::cancelled && accepting)
        {
            LOG_ERROR("Acceptor accept failed: {} (errno={})",
                      result.error().message, result.error().systemError);
        }

        if (listening.load(std::memory_order_acquire))
        {
            asyncAccept();
        }
    }

    EventLoop *loop;
    Socket listenSocket;
    std::atomic_bool listening{false};
    NewConnectionCallback newConnectionCallback;
    std::shared_ptr<ucp::IoOperation> acceptOperation;
};

Acceptor::Acceptor(
    EventLoop *loop, const InetAddress &listenAddr, bool reuseport)
    : state_(std::make_shared<State>(loop, listenAddr, reuseport))
{
}

Acceptor::~Acceptor() = default;

void Acceptor::setNewConnectionCallback(const NewConnectionCallback &cb)
{
    state_->newConnectionCallback = cb;
}

bool Acceptor::isListening() const
{
    return state_->listening.load(std::memory_order_acquire);
}

void Acceptor::listen()
{
    state_->listen();
}

void Acceptor::stop()
{
    state_->stop();
}
