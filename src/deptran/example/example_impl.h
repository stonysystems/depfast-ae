#include "__dep__.h"
#include "constants.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "deptran/procedure.h"
#include "../command_marshaler.h"
#include "../example.h"

namespace example_client {
    class ExampleClientServiceImpl : public ExampleClientService {
    public: 
            ExampleClientServiceImpl() ;
	    void hello(const std::vector<int32_t>& _req, rrr::DeferredReply* defer) override;
    };
}
