#pragma once

#include "rrr.hpp"

#include <errno.h>


namespace janus {

class ExampleClientService: public rrr::Service {
public:
    enum {
        HELLO = 0x177f6de2,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(HELLO, this, &ExampleClientService::__hello__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(HELLO);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void hello(const std::vector<int32_t>& _req, rrr::DeferredReply* defer) = 0;
private:
    void __hello__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        std::vector<int32_t>* in_0 = new std::vector<int32_t>;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->hello(*in_0, __defer__);
    }
};

class ExampleClientProxy {
protected:
    rrr::Client* __cl__;
public:
    ExampleClientProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_hello(const std::vector<int32_t>& _req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(ExampleClientService::HELLO, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << _req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 hello(const std::vector<int32_t>& _req) {
        rrr::Future* __fu__ = this->async_hello(_req);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
};

} // namespace janus



