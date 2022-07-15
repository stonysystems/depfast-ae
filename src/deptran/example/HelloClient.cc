
// #include "../example.h"
// #include "example_service.h"

//     int run_client(){
//         rrr::PollMgr *pm = new rrr::PollMgr();
//         rrr::Client *client = new rrr::Client(pm);
//         string s1 = "127.0.0.1:8090";
//         while (client->connect(s1.c_str())!=0) {
//             usleep(100 * 1000); // retry to connect
//         }
//         janus::ExampleClientProxy *client_proxy = new janus::ExampleClientProxy(client);
//         vector<int> _req;
//         FutureAttr fuattr; // fuattr
//         fuattr.callback = [] (Future* fu) {
//             std::cout << "received a response back from server..." << std::endl;
//         };
//         for (int i = 1; i <= 5; i++)
//             _req.push_back(i);
//         client_proxy->async_hello(_req, fuattr);
//         return 0;
//     }

#include "client_wrapper.h"

int main(){
    HelloClient* cli = new HelloClient();
    cli->connect();
    cli->request(5);
    return 0;
}
