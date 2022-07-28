#include <stdio.h>
#include <iostream>
#include "rpc.h"

static const std::string kServerHostname = "130.245.173.104";
static const std::string kClientHostname = "130.245.173.104";

static constexpr uint16_t kUDPPort = 45678;
static constexpr uint16_t kUDPPortClient = 45679;
static constexpr uint8_t kReqType = 2;
static constexpr size_t kMsgSize = 2600;
