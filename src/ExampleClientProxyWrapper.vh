#include <stdio.h>
#include "rpc.h"

class ExampleClientProxyWrapper{
    public:
        ExampleClientProxyWrapper(const std::string client_uri, const std::string server_uri);
        int hello();
        int add(int x, int y);
    private:
        erpc::Rpc<erpc::CTransport> *rpc;
        erpc::MsgBuffer req;
        erpc::MsgBuffer resp;
        int session_num;
};