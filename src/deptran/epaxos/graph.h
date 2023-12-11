#pragma once

#include "__dep__.h"
#include "constants.h"

namespace janus {

template<class T>
class EVertex {
 public:
  EVertex(){}

  virtual uint64_t id() {
    verify(0);
    return 0;
  }
  
  virtual bool isFirstInSCC(shared_ptr<T> &rhs) {
    verify(0);
    return 0;
  }

};

// V is vertex type
template<typename V>
class EGraph {
 private:
  function<void(shared_ptr<V>&)> exec;
  function<vector<shared_ptr<V>>(shared_ptr<V>&)> get_dependencies;

  unordered_map<uint64_t, int> disc;
  unordered_map<uint64_t, int> low;
  unordered_map<uint64_t, bool> in_stk;
  stack<shared_ptr<V>> stk;
  int time = 0;

 public:
  EGraph(){}
  
  ~EGraph(){
    disc.clear();
    low.clear();
    in_stk.clear();
  }

 private:
  void ExecuteSCC(shared_ptr<V>& vertex) {
    time++;
    uint64_t id = vertex->id();
    disc[id] = low[id] = time;
    stk.push(vertex);
    in_stk[id] = true;
    for (auto &dep_vertex : get_dependencies(vertex)) {
      uint64_t dep_vertex_id = dep_vertex->id();
      if (disc.count(dep_vertex_id) == 0) {
        disc[dep_vertex_id] = -1;
        ExecuteSCC(dep_vertex);
        low[id] = min(low[id], low[dep_vertex_id]);
      } else if (in_stk[dep_vertex_id]) {
        low[id] = min(low[id], disc[dep_vertex_id]);
      }
    }

    vector<shared_ptr<V>> scc;
    if (low[id] == disc[id]) {
      while (stk.top()->id() != id) {
        shared_ptr<V>& poppedV = stk.top();
        scc.push_back(poppedV);
        in_stk[poppedV->id()] = false;
        stk.pop();
      }
      shared_ptr<V>& poppedV = stk.top();
      scc.push_back(poppedV);
      in_stk[poppedV->id()] = false;
      stk.pop();
    }
    sort(scc.begin(), scc.end(), [](shared_ptr<V>& v1, shared_ptr<V>& v2) -> bool {
      return v1->isFirstInSCC(v2);
    });
    for (auto &vertex : scc) {
      exec(vertex);
    }
  }

 public:
  void Execute(shared_ptr<V>& vertex, function<void(shared_ptr<V>&)> exec, function<vector<shared_ptr<V>>(shared_ptr<V>&)> get_dependencies) {
    this->exec = exec;
    this->get_dependencies = get_dependencies;
    ExecuteSCC(vertex);
  }
};

}
