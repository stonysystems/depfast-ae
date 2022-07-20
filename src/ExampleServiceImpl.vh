#include <stdio.h>
#include "rpc.h"


class ExampleServiceImpl{
    
    public: 
        ExampleServiceImpl(const std::string server_uri);
        void hello(erpc::ReqHandle *req_handle, void *){};
        void add(erpc::ReqHandle *req_handle, void *){};
    private:
        rpc::Rpc<erpc::CTransport> *rpc;

}