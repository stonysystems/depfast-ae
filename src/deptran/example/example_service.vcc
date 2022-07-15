
#include "../example.h"
#include "example_service.h"



    void ExampleClientServiceImpl::hello(const std::vector<int32_t>& _req, rrr::DeferredReply* defer){
        for(int i : _req)
            cout << "hello =>" << i << endl;
        defer->reply();
    }

