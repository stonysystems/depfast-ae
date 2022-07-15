#pragma once

#include "__dep__.h"
#include "example_service.h"
#include "../example.h"

class HelloServer{
    public:
        HelloServer();
        void StartServer();
        void Change_IP(const std::string& str);
    private:
        rrr::PollMgr *pm = new rrr::PollMgr();
        base::ThreadPool *tp = new base::ThreadPool();
        rrr::Server *server = new rrr::Server(pm, tp);
        string s1 = "127.0.0.1:8090";
};