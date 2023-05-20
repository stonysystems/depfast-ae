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

int run_client(){
    rrr::PollMgr *pm = new rrr::PollMgr();
    auto client = std::make_shared<Client>(pm);

    while (client->connect(std::string("127.0.0.1:8090").c_str())!=0) {
        usleep(100 * 1000); // retry to connect
    }
    std::atomic<int64_t> done(0);
    ExampleClientProxy *client_proxy = new ExampleClientProxy(client.get());
    for(int i=0;i<4;i++) {
        vector<int> _req;
        FutureAttr fuattr;  // fuattr
        fuattr.callback = [&done] (Future* fu) {
            std::cout << "received a response back from server..." << std::endl;
            done++;
        };
        for (int i = 1; i <= 5; i++)
            _req.push_back(i);
        // example for the sync call
        client_proxy->hello(_req);
        int32_t a = rand() % 100;
        int32_t b = rand() % 100;
        // example for the async call
        client_proxy->async_add(a,b, fuattr);
        sleep(1);
    }
    std::cout<<"completed tasks:"<<done;
    return 0;
}

int main(){
    run_client();
    return 0;
}
