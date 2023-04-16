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
  unordered_map<uint64_t, shared_ptr<V>> vertices;
  unordered_map<uint64_t, vector<shared_ptr<V>> > scc_vertices;
  unordered_map<uint64_t, unordered_set<uint64_t>> parents;

 public:
  EGraph(){}

 private:
  void AddVertex(shared_ptr<V> &v) {
    vertices[v->id()] = v;
  }

  void AddParentEdge(shared_ptr<V> &v, shared_ptr<V> &parent) {
    parents[v->id()].insert(parent->id());
  }

 public:
  // returns true if already existed
  virtual bool FindOrCreateVertex(shared_ptr<V> &v) {
    if (vertices.count(v->id())) {
      return true;
    }
    this->AddVertex(v);
    return false;
  }

  virtual bool FindOrCreateParentEdge(shared_ptr<V> &v, shared_ptr<V> &parent) {
    if (!vertices.count(v->id())) {
      verify(0);
    }
    if (parents[v->id()].count(parent->id())) {
      return true;
    }
    this->AddParentEdge(v, parent);
    return false;
  }

 private:
  void FindSCC(uint64_t id,
               unordered_map<uint64_t, int> &disc,
               unordered_map<uint64_t, int> &low,
               unordered_map<uint64_t, bool> &in_stk,
               stack<uint64_t> &stk,
               int *time) {
    *time = *time + 1;
    disc[id] = low[id] = *time;
    stk.push(id);
    in_stk[id] = true;
    for (auto parent_id : parents[id]) {
      if (disc[parent_id] == -1) {
        FindSCC(parent_id, disc, low, in_stk, stk, time);
        low[id] = min(low[id], low[parent_id]);
      } else if (in_stk[parent_id]) {
        low[id] = min(low[id], disc[parent_id]);
      }
    }

    vector<shared_ptr<V>> scc;
    if (low[id] == disc[id]) {
      while (stk.top() != id) {
        uint64_t poppedId = stk.top();
        scc.push_back(vertices[poppedId]);
        in_stk[poppedId] = false;
        stk.pop();
      }
      uint64_t poppedId = stk.top();
      scc.push_back(vertices[poppedId]);
      in_stk[poppedId] = false;
      stk.pop();
    }
    sort(scc.begin(), scc.end(), [](shared_ptr<V> &v1, shared_ptr<V> &v2) -> bool {
      return v1->isFirstInSCC(v2);
    });
    for (auto &vertex : scc) {
      scc_vertices[vertex->id()] = scc;
    }
  }

  // uses tarjan's algorithm for finding SCC
  void PopulateSCCs() {
    unordered_map<uint64_t, int> disc;
    unordered_map<uint64_t, int> low;
    unordered_map<uint64_t, bool> in_stk;
    stack<uint64_t> stk;
    int time = 0;

    for (auto itr : vertices) {
      uint64_t id = itr.first;
      disc[id] = -1;
      low[id] = -1;
      in_stk[id] = false;
    }

    for (auto itr : vertices) {
      uint64_t id = itr.first;
      if (disc[id] == -1) {
        FindSCC(id, disc, low, in_stk, stk, &time);
      }
    }
  }

  void TopologicalSortUtil(uint64_t id, unordered_map<uint64_t, bool> &visited, vector<shared_ptr<V>> &que) {
    for (auto scc_v : scc_vertices[id]) {
      visited[scc_v->id()] = true;
    }
    for (auto scc_v : scc_vertices[id]) {
      for (auto parent_id : parents[scc_v->id()]) {
        if(!visited[parent_id]) {
          TopologicalSortUtil(parent_id, visited, que);
        }
      }
    }
    for (auto scc_v : scc_vertices[id]) {
      que.push_back(scc_v);
    }
  }

 public:
  vector<shared_ptr<V>> GetSortedVertices() {
    PopulateSCCs();
    unordered_map<uint64_t, bool> visited;
    vector<shared_ptr<V>> sorted_vertices;
    for (auto itr : vertices) {
      uint64_t id = itr.first;
      if (!visited[id]) {
        TopologicalSortUtil(id, visited, sorted_vertices);
      }
    }
    return sorted_vertices;
  }

};

}
