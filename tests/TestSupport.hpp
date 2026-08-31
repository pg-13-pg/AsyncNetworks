#pragma once

#include <cstdlib>
#include <iostream>

#define CHECK(expression) \
    do { \
        if (!(expression)) { \
            std::cerr << __FILE__ << ':' << __LINE__ \
                      << ": CHECK failed: " #expression << '\n'; \
            std::abort(); \
        } \
    } while (false)

#define CHECK_EQ(lhs, rhs) \
    do { \
        if (!((lhs) == (rhs))) { \
            std::cerr << __FILE__ << ':' << __LINE__ \
                      << ": CHECK_EQ failed: " #lhs " == " #rhs << '\n'; \
            std::abort(); \
        } \
    } while (false)
