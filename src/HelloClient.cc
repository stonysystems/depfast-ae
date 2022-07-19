#include "__dep__.h"
#include <iostream>
#include <vector>
#include <sys/time.h>
#include <thread>
#include <string>
#include <cstring>
#include <unistd.h>
#include "example/example_impl.h"
#include <pthread.h>

using namespace example_client;
using namespace janus;

int run_client(){
    rrr::PollMgr *pm = new rrr::PollMgr();
    auto client = std::make_shared<Client>(pm);

    while (client->connect(std::string("127.0.0.1:8090").c_str())!=0) {
        usleep(100 * 1000); // retry to connect
    }
    ExampleClientProxy *client_proxy = new ExampleClientProxy(client.get());
    vector<int> _req;
    std::atomic<int64_t> done(0);
    FutureAttr fuattr;  // fuattr
    fuattr.callback = [&done] (Future* fu) {
        std::cout << "received a response back from server..." << std::endl;
    };
    for (int i = 1; i <= 5; i++)
        _req.push_back(i);
    client_proxy->hello(_req);
    int32_t a = 5;
    int32_t b = 16;
    client_proxy->add(a,b);
    return 0;
}

int main(){
    run_client();
    return 0;
}