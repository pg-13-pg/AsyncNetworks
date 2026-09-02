#include "Buffer.hpp"
#include "TestSupport.hpp"
#include "ucp/proxy/HttpFramer.hpp"

#include <cstddef>
#include <string>
#include <string_view>

namespace proxy = ucp::proxy;

namespace {

proxy::RequestParseResult parseRequest(
    std::string_view bytes,
    const proxy::HttpLimits& limits = {})
{
    Buffer buffer;
    buffer.append(bytes.data(), bytes.size());
    return proxy::parseRequestHead(buffer, limits);
}

proxy::ResponseParseResult parseResponse(
    std::string_view bytes,
    const proxy::HttpLimits& limits = {})
{
    Buffer buffer;
    buffer.append(bytes.data(), bytes.size());
    return proxy::parseResponseHead(buffer, limits);
}

void checkError(
    const proxy::RequestParseResult& result,
    proxy::HttpParseError error)
{
    CHECK_EQ(result.status, proxy::ParseStatus::error);
    CHECK_EQ(result.error, error);
}

void checkError(
    const proxy::ResponseParseResult& result,
    proxy::HttpParseError error)
{
    CHECK_EQ(result.status, proxy::ParseStatus::error);
    CHECK_EQ(result.error, error);
}

std::string requestWithExactHeaderSize(std::size_t size)
{
    const std::string prefix =
        "GET /limit HTTP/1.1\r\n"
        "Host: example\r\n"
        "Content-Length: 0\r\n"
        "X-Fill: ";
    const std::string suffix = "\r\n\r\n";
    CHECK(size >= prefix.size() + suffix.size());
    return prefix
        + std::string(size - prefix.size() - suffix.size(), 'a')
        + suffix;
}

} // namespace

int main()
{
    const std::string postHead =
        "POST /api/items HTTP/1.1\r\n"
        "Host: example\r\n"
        "Content-Length: 5\r\n"
        "Connection: keep-alive\r\n"
        "\r\n";

    Buffer fragmented;
    for (std::size_t i = 0; i < postHead.size(); ++i) {
        fragmented.append(postHead.data() + i, 1);
        const auto result = proxy::parseRequestHead(fragmented, {});
        if (i + 1 < postHead.size()) {
            CHECK_EQ(result.status, proxy::ParseStatus::needMore);
            CHECK_EQ(fragmented.readableBytes(), i + 1);
        } else {
            CHECK_EQ(result.status, proxy::ParseStatus::complete);
            CHECK_EQ(result.request.method, "POST");
            CHECK_EQ(result.request.path, "/api/items");
            CHECK_EQ(result.request.version, "HTTP/1.1");
            CHECK_EQ(result.request.contentLength, 5U);
            CHECK(result.request.keepAlive);
            CHECK_EQ(fragmented.readableBytes(), 0U);
        }
    }

    const auto lowerCase = parseRequest(
        "POST /lower HTTP/1.1\r\n"
        "host: example\r\n"
        "content-length: 3\r\n"
        "connection: close\r\n"
        "proxy-connection: keep-alive\r\n"
        "\r\n");
    CHECK_EQ(lowerCase.status, proxy::ParseStatus::complete);
    CHECK_EQ(lowerCase.request.contentLength, 3U);
    CHECK(!lowerCase.request.keepAlive);
    CHECK(lowerCase.request.forwardHead.find("proxy-connection")
          == std::string::npos);
    CHECK(lowerCase.request.forwardHead.find("connection: close")
          == std::string::npos);
    CHECK(lowerCase.request.forwardHead.find("Connection: close\r\n")
          != std::string::npos);

    const auto duplicateLength = parseRequest(
        "POST /duplicate HTTP/1.1\r\n"
        "Content-Length: 7\r\n"
        "content-length: 7\r\n\r\n");
    CHECK_EQ(duplicateLength.status, proxy::ParseStatus::complete);
    CHECK_EQ(duplicateLength.request.contentLength, 7U);

    checkError(parseRequest(
        "POST /conflict HTTP/1.1\r\n"
        "Content-Length: 7\r\n"
        "Content-Length: 8\r\n\r\n"),
        proxy::HttpParseError::badSyntax);
    checkError(parseRequest(
        "GET / HTTP/1.0\r\nContent-Length: 0\r\n\r\n"),
        proxy::HttpParseError::badSyntax);
    checkError(parseRequest(
        "GET / HTTP/1.1\r\nBad Header: value\r\n\r\n"),
        proxy::HttpParseError::badSyntax);
    checkError(parseRequest(
        "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"),
        proxy::HttpParseError::unsupportedFraming);
    checkError(parseRequest(
        "GET / HTTP/1.1\r\nUpgrade: websocket\r\n\r\n"),
        proxy::HttpParseError::unsupportedFraming);
    checkError(parseRequest(
        "CONNECT example:443 HTTP/1.1\r\n\r\n"),
        proxy::HttpParseError::unsupportedMethod);
    checkError(parseRequest(
        "PUT / HTTP/1.1\r\nContent-Length: 0\r\n\r\n"),
        proxy::HttpParseError::unsupportedMethod);
    checkError(parseRequest(
        "POST / HTTP/1.1\r\nContent-Length: 1048577\r\n\r\n"),
        proxy::HttpParseError::bodyTooLarge);
    checkError(parseRequest(
        "POST / HTTP/1.1\r\nContent-Length: 12x\r\n\r\n"),
        proxy::HttpParseError::badSyntax);
    checkError(parseRequest(
        "POST / HTTP/1.1\r\nContent-Length: "
        "184467440737095516160\r\n\r\n"),
        proxy::HttpParseError::badSyntax);

    proxy::HttpLimits limits;
    limits.maxHeaderBytes = 16 * 1024;
    const auto exactHeader = requestWithExactHeaderSize(
        limits.maxHeaderBytes);
    CHECK_EQ(parseRequest(exactHeader, limits).status,
             proxy::ParseStatus::complete);
    checkError(parseRequest(
        requestWithExactHeaderSize(limits.maxHeaderBytes + 1), limits),
        proxy::HttpParseError::headerTooLarge);

    Buffer retained;
    retained.append(postHead + "helloNEXT");
    const auto retainedResult = proxy::parseRequestHead(retained, {});
    CHECK_EQ(retainedResult.status, proxy::ParseStatus::complete);
    CHECK_EQ(retained.readableBytes(), 9U);
    CHECK_EQ(std::string_view(
                 retained.readBeginAddr(), retained.readableBytes()),
             "helloNEXT");

    const auto response = parseResponse(
        "HTTP/1.1 200 OK\r\n"
        "Content-Length: 5\r\n"
        "Connection: keep-alive\r\n\r\n");
    CHECK_EQ(response.status, proxy::ParseStatus::complete);
    CHECK_EQ(response.response.statusCode, 200);
    CHECK_EQ(response.response.contentLength, 5U);
    CHECK(response.response.keepAlive);

    checkError(parseResponse(
        "HTTP/1.1 abc Nope\r\nContent-Length: 0\r\n\r\n"),
        proxy::HttpParseError::badSyntax);
    checkError(parseResponse(
        "HTTP/1.1 99 Nope\r\nContent-Length: 0\r\n\r\n"),
        proxy::HttpParseError::badSyntax);
    checkError(parseResponse(
        "HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n"),
        proxy::HttpParseError::badSyntax);
    checkError(parseResponse(
        "HTTP/1.1 200 OK\r\n\r\n"),
        proxy::HttpParseError::unsupportedFraming);
    checkError(parseResponse(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"),
        proxy::HttpParseError::unsupportedFraming);
    checkError(parseResponse(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n"),
        proxy::HttpParseError::unsupportedFraming);

    const auto noContent = parseResponse(
        "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n");
    CHECK_EQ(noContent.status, proxy::ParseStatus::complete);
    CHECK_EQ(noContent.response.statusCode, 204);
    CHECK_EQ(noContent.response.contentLength, 0U);
    CHECK(!noContent.response.keepAlive);
    checkError(parseResponse(
        "HTTP/1.1 204 No Content\r\nContent-Length: 1\r\n\r\n"),
        proxy::HttpParseError::unsupportedFraming);

    return 0;
}
