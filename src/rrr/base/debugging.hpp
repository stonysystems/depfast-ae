#pragma once

#include <stdio.h>
#include <assert.h>
#include <iostream>

#ifndef likely
#define likely(x)   __builtin_expect((x), 1)
#endif // likely

#ifndef unlikely
#define unlikely(x)   __builtin_expect((x), 0)
#endif // unlikely

/**
 * Use assert() when the test is only intended for debugging.
 * Use verify() when the test is crucial for both debug and release binary.
 */
#ifdef NDEBUG
#define verify(expr) do { if (unlikely(!(expr))) { std::cout << "  *** verify failed: " << #expr << " at " << __FILE__ << ", line " << __LINE__ << std::endl; ::rrr::print_stack_trace(); ::abort(); } } while (0)
#else
#define verify(expr) assert(expr)
#endif

namespace rrr {

void print_stack_trace(FILE* fp = stderr) __attribute__((noinline));

} // namespace base
