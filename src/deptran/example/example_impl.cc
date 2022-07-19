#include "example_impl.h"

namespace example_client {
    ExampleClientServiceImpl::ExampleClientServiceImpl() {  }

    void ExampleClientServiceImpl::hello(const std::vector<int32_t>& _req, rrr::DeferredReply* defer) {
	std::cout << "received a hello request" << std::endl;
	for(int i : _req)
          cout << "hello => " << i << endl;
        defer->reply();
    }

    void ExampleClientServiceImpl::add(const int32_t& x, const int32_t& y, rrr::DeferredReply* defer){
        std::cout<< "recieved an add request" << std::endl;
        int32_t z = x + y;
        cout << "Sum of " << x << " and " << y << " equals " << z << std::endl;
        defer->reply();
    }
}
