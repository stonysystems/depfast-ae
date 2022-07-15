#include "server_wrapper.h"

HelloServer::HelloServer(){};

void HelloServer::StartServer(){
    ExampleClientServiceImpl *impl = new ExampleClientServiceImpl();
    server->reg(impl);
    server->start(s1.c_str());
    while (1) {
        sleep(1);
    }
    return;
}

void HelloServer::Change_IP(const std::string& str){
    s1 = str;
    return;
}
