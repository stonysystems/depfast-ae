
// #include "example_service.h"
// #include "../example.h"



// // int run_server(){
// //     ExampleClientServiceImpl *impl = new ExampleClientServiceImpl();
// //     rrr::PollMgr *pm = new rrr::PollMgr();
// //     base::ThreadPool *tp = new base::ThreadPool();
// //     rrr::Server *server = new rrr::Server(pm, tp);
// //     server->reg(impl);
// //     string s1 = "127.0.0.1:8090";
// //     server->start(s1.c_str());
// //     while (1) {
// //         sleep(1);
// //     }

// //     return 0;
// // }

//move hellolcient and helloserver to src
#include "server_wrapper.h"

int main(){
    HelloServer* sv = new HelloServer();
    sv->StartServer();
    return 0;
}