#pragma once

#include "__dep__.h"
#include "constants.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "deptran/procedure.h"
#include "../command_marshaler.h"
#include "../example.h"

class SimpleCommand;

    class ExampleClientServiceImpl : public janus::ExampleClientService{
        public:
            void hello(const std::vector<int32_t>& _req, rrr::DeferredReply* defer) override;
    };
