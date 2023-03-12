#pragma once

typedef unordered_map<uint64_t, uint64_t> unordered_map_uint64_uint64_t;
typedef unordered_map<uint64_t, pair<uint64_t, bool_t>> unordered_map_uint64_pair;

#define _PARAMS0(...)
#define _PARAMS1(first, second, ...) second
#define _PARAMS2(first, second, ...) second, _PARAMS1(__VA_ARGS__)
#define _PARAMS3(first, second, ...) second, _PARAMS2(__VA_ARGS__)
#define _PARAMS4(first, second, ...) second, _PARAMS3(__VA_ARGS__)
#define _PARAMS5(first, second, ...) second, _PARAMS4(__VA_ARGS__)
#define _PARAMS6(first, second, ...) second, _PARAMS5(__VA_ARGS__)
#define _PARAMS7(first, second, ...) second, _PARAMS6(__VA_ARGS__)
#define _PARAMS8(first, second, ...) second, _PARAMS7(__VA_ARGS__)
#define _PARAMS9(first, second, ...) second, _PARAMS8(__VA_ARGS__)
#define _PARAMS10(first, second, ...) second, _PARAMS9(__VA_ARGS__)
#define _PARAMS11(first, second, ...) second, _PARAMS10(__VA_ARGS__)
#define _PARAMS12(first, second, ...) second, _PARAMS11(__VA_ARGS__)
#define _PARAMS13(first, second, ...) second, _PARAMS12(__VA_ARGS__)
#define _PARAMS14(first, second, ...) second, _PARAMS13(__VA_ARGS__)
#define _PARAMS15(first, second, ...) second, _PARAMS14(__VA_ARGS__)
#define _PARAMS16(first, second, ...) second, _PARAMS15(__VA_ARGS__)
#define _PARAMS17(first, second, ...) second, _PARAMS16(__VA_ARGS__)
#define _PARAMS18(first, second, ...) second, _PARAMS17(__VA_ARGS__)
#define _PARAMS19(first, second, ...) second, _PARAMS18(__VA_ARGS__)
#define _PARAMS20(first, second, ...) second, _PARAMS19(__VA_ARGS__)

#define _ARGPAIRS0(...)
#define _ARGPAIRS1(first, second, ...) first second _ARGPAIRS0(__VA_ARGS__)
#define _ARGPAIRS2(first, second, ...) first second, _ARGPAIRS1(__VA_ARGS__)
#define _ARGPAIRS3(first, second, ...) first second, _ARGPAIRS2(__VA_ARGS__)
#define _ARGPAIRS4(first, second, ...) first second, _ARGPAIRS3(__VA_ARGS__)
#define _ARGPAIRS5(first, second, ...) first second, _ARGPAIRS4(__VA_ARGS__)
#define _ARGPAIRS6(first, second, ...) first second, _ARGPAIRS5(__VA_ARGS__)
#define _ARGPAIRS7(first, second, ...) first second, _ARGPAIRS6(__VA_ARGS__)
#define _ARGPAIRS8(first, second, ...) first second, _ARGPAIRS7(__VA_ARGS__)
#define _ARGPAIRS9(first, second, ...) first second, _ARGPAIRS8(__VA_ARGS__)
#define _ARGPAIRS10(first, second, ...) first second, _ARGPAIRS9(__VA_ARGS__)
#define _ARGPAIRS11(first, second, ...) first second, _ARGPAIRS10(__VA_ARGS__)
#define _ARGPAIRS12(first, second, ...) first second, _ARGPAIRS11(__VA_ARGS__)
#define _ARGPAIRS13(first, second, ...) first second, _ARGPAIRS12(__VA_ARGS__)
#define _ARGPAIRS14(first, second, ...) first second, _ARGPAIRS13(__VA_ARGS__)
#define _ARGPAIRS15(first, second, ...) first second, _ARGPAIRS14(__VA_ARGS__)
#define _ARGPAIRS16(first, second, ...) first second, _ARGPAIRS15(__VA_ARGS__)
#define _ARGPAIRS17(first, second, ...) first second, _ARGPAIRS16(__VA_ARGS__)
#define _ARGPAIRS18(first, second, ...) first second, _ARGPAIRS17(__VA_ARGS__)
#define _ARGPAIRS19(first, second, ...) first second, _ARGPAIRS18(__VA_ARGS__)
#define _ARGPAIRS20(first, second, ...) first second, _ARGPAIRS19(__VA_ARGS__)

#define _PARAMS(n, ...) _PARAMS##n(__VA_ARGS__)
#define _ARGPAIRS(n, ...) _ARGPAIRS##n(__VA_ARGS__)

#define RpcHandler(name, ...) \
  void name(_ARGPAIRS(__VA_ARGS__), rrr::DeferredReply* defer) override { \
    verify(svr_ != nullptr); \
    if (svr_->IsDisconnected()) { \
      OnDisconnected##name(_PARAMS(__VA_ARGS__)); \
      defer->reply(); \
    }  else { \
      Handle##name(_PARAMS(__VA_ARGS__), defer); \
    } \
  } \
  void Handle##name(_ARGPAIRS(__VA_ARGS__), rrr::DeferredReply* defer); \
  void OnDisconnected##name(_ARGPAIRS(__VA_ARGS__))

#ifdef EPAXOS_TEST_CORO
#define _RPC_COUNT() { \
    std::lock_guard<std::recursive_mutex> lock(rpc_mtx_); \
    rpc_count_++; \
  }
#else
#define _RPC_COUNT()
#endif

#define Call_Async(proxy, name, ...) { \
  auto f = proxy->async##_##name(__VA_ARGS__); \
  _RPC_COUNT(); \
  Future::safe_release(f); \
}
