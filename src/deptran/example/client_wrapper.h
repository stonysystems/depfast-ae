#pragma once

#include "__dep__.h"
#include "example_service.h"
#include "../example.h"

class HelloClient{
    public:
        HelloClient();
        int connect();
        void request(uint16_t n);
        void change_ip(const std::string& str);


    private:
        rrr::PollMgr *pm = new rrr::PollMgr();
        rrr::Client *client = new rrr::Client(pm);
        string s1 = "127.0.0.1:8090";
};