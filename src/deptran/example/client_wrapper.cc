#include "client_wrapper.h"

HelloClient::HelloClient(){};


int HelloClient::connect(){
    while (client->connect(s1.c_str())!=0) {
        usleep(100 * 1000); // retry to connect
    }
    return 0;
}

void HelloClient::request(uint16_t n){
    janus::ExampleClientProxy *client_proxy = new janus::ExampleClientProxy(client);
    vector<int> _req;
    FutureAttr fuattr; // fuattr
    fuattr.callback = [] (Future* fu) {
        std::cout << "received a response back from server..." << std::endl;
    };
    for (int i = 1; i <= n; i++)
        _req.push_back(i);
    client_proxy->async_hello(_req, fuattr);
    return;
}

void HelloClient::change_ip(const std::string& str){
    s1 = str;
    return;
}