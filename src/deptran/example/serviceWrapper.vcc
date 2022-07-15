#include "serviceWrapper.h"


ExampleClientServiceImplWrapper::ExampleClientServiceImplWrapper(const std::string& server_uri){
    erpc::Rpc<erpc::CTransport> *rpc;
    erpc::Nexus nexus(server_uri);
    nexus.register_req_func(kReqType, req_handler);
    rpc = new erpc::Rpc<erpc::CTransport>(&nexus, nullptr, 0, nullptr);
    rpc->run_event_loop(100000);
}


void ExampleClientServiceImplWrapper::hello(const std::vector<int32_t>& _req, erpc::ReqHandle *req_handle){
    printf("received %s\n", reinterpret_cast<char *>(req_handle->get_req_msgbuf()->buf_));
    auto &resp = req_handle->pre_resp_msgbuf_;
    rpc->resize_msg_buffer(&resp, kMsgSize);
    sprintf(reinterpret_cast<char *>(resp.buf_), "[message from server]");
    rpc->enqueue_response(req_handle, &resp);
}