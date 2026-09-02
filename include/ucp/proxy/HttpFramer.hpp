#pragma once

#include <cstddef>
#include <string>

class Buffer;

namespace ucp::proxy {

struct HttpLimits {
    std::size_t maxHeaderBytes{16 * 1024};
    std::size_t maxBodyBytes{1024 * 1024};
};

enum class ParseStatus {
    needMore,
    complete,
    error
};

enum class HttpParseError {
    none,
    badSyntax,
    headerTooLarge,
    bodyTooLarge,
    unsupportedMethod,
    unsupportedFraming
};

struct HttpRequestHead {
    std::string method;
    std::string path;
    std::string version;
    std::string forwardHead;
    std::size_t contentLength{0};
    bool keepAlive{true};
};

struct HttpResponseHead {
    int statusCode{0};
    std::string version;
    std::string forwardHead;
    std::size_t contentLength{0};
    bool keepAlive{true};
};

struct RequestParseResult {
    ParseStatus status{ParseStatus::needMore};
    HttpParseError error{HttpParseError::none};
    HttpRequestHead request;
};

struct ResponseParseResult {
    ParseStatus status{ParseStatus::needMore};
    HttpParseError error{HttpParseError::none};
    HttpResponseHead response;
};

RequestParseResult parseRequestHead(
    Buffer& input, const HttpLimits& limits = {});

ResponseParseResult parseResponseHead(
    Buffer& input, const HttpLimits& limits = {});

} // namespace ucp::proxy
