#include "example_impl.h"

namespace example_client {
    ExampleClientServiceImpl::ExampleClientServiceImpl() {  }

    void ExampleClientServiceImpl::hello(const std::vector<int32_t>& _req, rrr::DeferredReply* defer) {
	std::cout << "received a message" << std::endl;
	for(int i : _req)
          cout << "hello => " << i << endl;
        defer->reply();
    }
}
