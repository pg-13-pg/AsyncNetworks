#include "ucp/proxy/HttpFramer.hpp"

#include "Buffer.hpp"

#include <algorithm>
#include <limits>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace ucp::proxy {
namespace {

struct HeaderLine {
    std::string lowerName;
    std::string_view value;
    std::string_view original;
};

struct ParsedHeaders {
    std::vector<HeaderLine> lines;
    std::optional<std::size_t> contentLength;
    bool keepAlive{true};
};

std::string lowerAscii(std::string_view value)
{
    std::string result;
    result.reserve(value.size());
    for (const unsigned char ch : value) {
        if (ch >= 'A' && ch <= 'Z') {
            result.push_back(static_cast<char>(ch + ('a' - 'A')));
        } else {
            result.push_back(static_cast<char>(ch));
        }
    }
    return result;
}

std::string_view trimOws(std::string_view value)
{
    while (!value.empty()
           && (value.front() == ' ' || value.front() == '\t')) {
        value.remove_prefix(1);
    }
    while (!value.empty()
           && (value.back() == ' ' || value.back() == '\t')) {
        value.remove_suffix(1);
    }
    return value;
}

bool isTokenCharacter(unsigned char ch)
{
    if ((ch >= 'a' && ch <= 'z')
        || (ch >= 'A' && ch <= 'Z')
        || (ch >= '0' && ch <= '9')) {
        return true;
    }
    constexpr std::string_view punctuation{"!#$%&'*+-.^_`|~"};
    return punctuation.find(static_cast<char>(ch))
        != std::string_view::npos;
}

bool validHeaderName(std::string_view name)
{
    return !name.empty()
        && std::all_of(name.begin(), name.end(), [](unsigned char ch) {
               return isTokenCharacter(ch);
           });
}

bool validHeaderValue(std::string_view value)
{
    return std::all_of(value.begin(), value.end(), [](unsigned char ch) {
        return ch == '\t' || (ch >= 0x20 && ch != 0x7f);
    });
}

bool parseDecimal(std::string_view value, std::size_t& parsed)
{
    value = trimOws(value);
    if (value.empty()) {
        return false;
    }

    std::size_t result = 0;
    for (const unsigned char ch : value) {
        if (ch < '0' || ch > '9') {
            return false;
        }
        const std::size_t digit = ch - '0';
        if (result > (std::numeric_limits<std::size_t>::max() - digit) / 10) {
            return false;
        }
        result = result * 10 + digit;
    }
    parsed = result;
    return true;
}

std::optional<std::size_t> findHeaderBytes(std::string_view bytes)
{
    const auto end = bytes.find("\r\n\r\n");
    if (end == std::string_view::npos) {
        return std::nullopt;
    }
    return end + 4;
}

bool parseConnectionTokens(
    std::string_view value, bool& keepAlive)
{
    bool sawToken = false;
    while (true) {
        const auto comma = value.find(',');
        const auto token = lowerAscii(trimOws(value.substr(0, comma)));
        if (token.empty()
            || (token != "close" && token != "keep-alive")) {
            return false;
        }
        sawToken = true;
        if (token == "close") {
            keepAlive = false;
        }
        if (comma == std::string_view::npos) {
            break;
        }
        value.remove_prefix(comma + 1);
    }
    return sawToken;
}

HttpParseError parseHeaderLines(
    std::string_view block,
    std::size_t firstLineEnd,
    const HttpLimits& limits,
    ParsedHeaders& parsed)
{
    std::size_t cursor = firstLineEnd + 2;
    while (cursor + 2 <= block.size()) {
        const auto lineEnd = block.find("\r\n", cursor);
        if (lineEnd == std::string_view::npos) {
            return HttpParseError::badSyntax;
        }
        if (lineEnd == cursor) {
            break;
        }

        const std::string_view line = block.substr(cursor, lineEnd - cursor);
        if (line.front() == ' ' || line.front() == '\t') {
            return HttpParseError::badSyntax;
        }
        const auto colon = line.find(':');
        if (colon == std::string_view::npos) {
            return HttpParseError::badSyntax;
        }
        const auto name = line.substr(0, colon);
        const auto value = trimOws(line.substr(colon + 1));
        if (!validHeaderName(name) || !validHeaderValue(value)) {
            return HttpParseError::badSyntax;
        }

        HeaderLine header{lowerAscii(name), value, line};
        if (header.lowerName == "transfer-encoding"
            || header.lowerName == "upgrade") {
            return HttpParseError::unsupportedFraming;
        }
        if (header.lowerName == "content-length") {
            std::size_t length = 0;
            if (!parseDecimal(value, length)) {
                return HttpParseError::badSyntax;
            }
            if (parsed.contentLength && *parsed.contentLength != length) {
                return HttpParseError::badSyntax;
            }
            if (length > limits.maxBodyBytes) {
                return HttpParseError::bodyTooLarge;
            }
            parsed.contentLength = length;
        } else if (header.lowerName == "connection"
                   || header.lowerName == "proxy-connection") {
            if (!parseConnectionTokens(value, parsed.keepAlive)) {
                return HttpParseError::unsupportedFraming;
            }
        }
        parsed.lines.push_back(std::move(header));
        cursor = lineEnd + 2;
    }
    return HttpParseError::none;
}

std::string buildForwardHead(
    std::string_view startLine,
    const ParsedHeaders& headers)
{
    std::string result;
    result.reserve(startLine.size() + 64);
    result.append(startLine);
    result.append("\r\n");
    for (const auto& header : headers.lines) {
        if (header.lowerName == "connection"
            || header.lowerName == "proxy-connection") {
            continue;
        }
        result.append(header.original);
        result.append("\r\n");
    }
    result.append(headers.keepAlive
                      ? "Connection: keep-alive\r\n\r\n"
                      : "Connection: close\r\n\r\n");
    return result;
}

RequestParseResult requestError(HttpParseError error)
{
    return {ParseStatus::error, error, {}};
}

ResponseParseResult responseError(HttpParseError error)
{
    return {ParseStatus::error, error, {}};
}

bool statusHasNoBody(int statusCode)
{
    return (statusCode >= 100 && statusCode < 200)
        || statusCode == 204 || statusCode == 304;
}

} // namespace

RequestParseResult parseRequestHead(
    Buffer& input, const HttpLimits& limits)
{
    const std::string_view bytes(
        input.readBeginAddr(), input.readableBytes());
    const auto headerBytes = findHeaderBytes(bytes);
    if (!headerBytes) {
        if (bytes.size() >= limits.maxHeaderBytes) {
            return requestError(HttpParseError::headerTooLarge);
        }
        return {};
    }
    if (*headerBytes > limits.maxHeaderBytes) {
        return requestError(HttpParseError::headerTooLarge);
    }

    const std::string_view block = bytes.substr(0, *headerBytes);
    const auto firstLineEnd = block.find("\r\n");
    if (firstLineEnd == std::string_view::npos || firstLineEnd == 0) {
        return requestError(HttpParseError::badSyntax);
    }
    const std::string_view startLine = block.substr(0, firstLineEnd);
    const auto firstSpace = startLine.find(' ');
    const auto secondSpace = firstSpace == std::string_view::npos
        ? std::string_view::npos
        : startLine.find(' ', firstSpace + 1);
    if (firstSpace == std::string_view::npos
        || secondSpace == std::string_view::npos
        || startLine.find(' ', secondSpace + 1) != std::string_view::npos) {
        return requestError(HttpParseError::badSyntax);
    }

    const auto method = startLine.substr(0, firstSpace);
    const auto path = startLine.substr(
        firstSpace + 1, secondSpace - firstSpace - 1);
    const auto version = startLine.substr(secondSpace + 1);
    if (method != "GET" && method != "POST") {
        return requestError(HttpParseError::unsupportedMethod);
    }
    if (path.empty() || path.front() != '/' || version != "HTTP/1.1") {
        return requestError(HttpParseError::badSyntax);
    }

    ParsedHeaders headers;
    const auto headerError = parseHeaderLines(
        block, firstLineEnd, limits, headers);
    if (headerError != HttpParseError::none) {
        return requestError(headerError);
    }
    if (method == "POST" && !headers.contentLength) {
        return requestError(HttpParseError::unsupportedFraming);
    }

    HttpRequestHead request;
    request.method = method;
    request.path = path;
    request.version = version;
    request.contentLength = headers.contentLength.value_or(0);
    request.keepAlive = headers.keepAlive;
    request.forwardHead = buildForwardHead(startLine, headers);
    input.retrieve(*headerBytes);
    return {ParseStatus::complete, HttpParseError::none,
            std::move(request)};
}

ResponseParseResult parseResponseHead(
    Buffer& input, const HttpLimits& limits)
{
    const std::string_view bytes(
        input.readBeginAddr(), input.readableBytes());
    const auto headerBytes = findHeaderBytes(bytes);
    if (!headerBytes) {
        if (bytes.size() >= limits.maxHeaderBytes) {
            return responseError(HttpParseError::headerTooLarge);
        }
        return {};
    }
    if (*headerBytes > limits.maxHeaderBytes) {
        return responseError(HttpParseError::headerTooLarge);
    }

    const std::string_view block = bytes.substr(0, *headerBytes);
    const auto firstLineEnd = block.find("\r\n");
    if (firstLineEnd == std::string_view::npos || firstLineEnd == 0) {
        return responseError(HttpParseError::badSyntax);
    }
    const std::string_view startLine = block.substr(0, firstLineEnd);
    const auto firstSpace = startLine.find(' ');
    const auto secondSpace = firstSpace == std::string_view::npos
        ? std::string_view::npos
        : startLine.find(' ', firstSpace + 1);
    const auto version = firstSpace == std::string_view::npos
        ? std::string_view{}
        : startLine.substr(0, firstSpace);
    const auto statusToken = firstSpace == std::string_view::npos
        ? std::string_view{}
        : startLine.substr(
              firstSpace + 1,
              (secondSpace == std::string_view::npos
                   ? startLine.size()
                   : secondSpace)
                  - firstSpace - 1);
    if (version != "HTTP/1.1" || statusToken.size() != 3) {
        return responseError(HttpParseError::badSyntax);
    }

    int statusCode = 0;
    for (const unsigned char ch : statusToken) {
        if (ch < '0' || ch > '9') {
            return responseError(HttpParseError::badSyntax);
        }
        statusCode = statusCode * 10 + (ch - '0');
    }
    if (statusCode < 100 || statusCode > 599) {
        return responseError(HttpParseError::badSyntax);
    }

    ParsedHeaders headers;
    const auto headerError = parseHeaderLines(
        block, firstLineEnd, limits, headers);
    if (headerError != HttpParseError::none) {
        return responseError(headerError);
    }

    const bool noBody = statusHasNoBody(statusCode);
    if (noBody) {
        if (headers.contentLength.value_or(0) != 0) {
            return responseError(HttpParseError::unsupportedFraming);
        }
    } else if (!headers.contentLength) {
        return responseError(HttpParseError::unsupportedFraming);
    }

    HttpResponseHead response;
    response.statusCode = statusCode;
    response.version = version;
    response.contentLength = noBody
        ? 0
        : headers.contentLength.value_or(0);
    response.keepAlive = headers.keepAlive;
    response.forwardHead = buildForwardHead(startLine, headers);
    input.retrieve(*headerBytes);
    return {ParseStatus::complete, HttpParseError::none,
            std::move(response)};
}

} // namespace ucp::proxy
