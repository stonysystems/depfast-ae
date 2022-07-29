#include "common.h"


using namespace std;
erpc::Rpc<erpc::CTransport> *rpc;


void hello_func(vector<int> _req){
  std::cout << "received a hello request" << std::endl;
  for(int i : _req)
    cout << "hello => " << i << endl;
}

void add_func(int x, int y){
  std::cout << "received an add request" << std::endl;
  int z = x + y;
  cout << "Sum of " << x << " and " << y << " equals " << z << std::endl;
}

void add_wrapper(erpc::ReqHandle *req_handle, void *) {
  std::string s = reinterpret_cast<char *>(req_handle->get_req_msgbuf()->buf_);
  //decode string back into vector
  std::vector<int> vect;
  std::stringstream ss(s);
  for (int i; ss >> i;) {
      vect.push_back(i);    
      if (ss.peek() == ',')
        ss.ignore();
  }
  int x = vect[0];
  int y = vect[1];
  add_func(x, y);

  auto &resp = req_handle->pre_resp_msgbuf_;
  rpc->resize_msg_buffer(&resp, strlen("add request was processed"));
  sprintf(reinterpret_cast<char *>(resp.buf_), "add request was processed");
  rpc->enqueue_response(req_handle, &resp);
}

void hello_wrapper(erpc::ReqHandle *req_handle, void *) {
  std::string s = reinterpret_cast<char *>(req_handle->get_req_msgbuf()->buf_);
  //decode string back into vector
  std::vector<int> vect;
  std::stringstream ss(s);
  for (int i; ss >> i;) {
      vect.push_back(i);    
      if (ss.peek() == ',')
        ss.ignore();
  }
  
  hello_func(vect);

  auto &resp = req_handle->pre_resp_msgbuf_;
  rpc->resize_msg_buffer(&resp, strlen("hello request was processed"));
  sprintf(reinterpret_cast<char *>(resp.buf_), "hello request was processed");
  rpc->enqueue_response(req_handle, &resp);
}

int main() {
  std::string server_uri = kServerHostname + ":" + std::to_string(kUDPPort);
  std::cout << "server_uri: " <<  server_uri << std::endl;
  erpc::Nexus nexus(server_uri);
  
  nexus.register_req_func(1, hello_wrapper);
  nexus.register_req_func(2, add_wrapper);

  rpc = new erpc::Rpc<erpc::CTransport>(&nexus, nullptr, 0, nullptr);
  while (true) {
    int num_pkts = rpc->run_event_loop_once();
    if (num_pkts > 0)
      std::cout << "num_pkts: " << num_pkts << std::endl;
  }
}