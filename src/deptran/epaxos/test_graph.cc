#include "graph.h"

namespace janus {

#define AssertOrder(res, vertex1, vertex2) { \
          verify(find(res.begin(), res.end(), vertex1) - res.begin() < find(res.begin(), res.end(), vertex2) - res.begin()); \
        }

class TestEVertex: public EVertex<TestEVertex> {
 public:
  uint64_t id_;
  uint64_t priority;

  TestEVertex(uint64_t id_, uint64_t priority) : EVertex() {
    this->id_ = id_;
    this->priority = priority;
  }

  uint64_t id() override {
    return id_;
  }

  bool operator>(TestEVertex &rhs) const {
    return this->priority > rhs.priority;
  }
  
  bool operator<(TestEVertex &rhs) const {
    return this->priority < rhs.priority;
  }
};

class TestEGraph: public EGraph<TestEVertex> {};

class TestGraph {
 public:
  static void Run() {
    TestEGraph graph;
    shared_ptr<TestEVertex> v1 = make_shared<TestEVertex>(1, 1);
    shared_ptr<TestEVertex> v2 = make_shared<TestEVertex>(2, 2);
    shared_ptr<TestEVertex> v3 = make_shared<TestEVertex>(3, 3);
    shared_ptr<TestEVertex> v4 = make_shared<TestEVertex>(4, 2);
    shared_ptr<TestEVertex> v5 = make_shared<TestEVertex>(5, 4);
    shared_ptr<TestEVertex> v6 = make_shared<TestEVertex>(6, 3);
    graph.FindOrCreateVertex(v1);
    graph.FindOrCreateVertex(v2);
    graph.FindOrCreateVertex(v3);
    graph.FindOrCreateVertex(v4);
    graph.FindOrCreateVertex(v5);
    graph.FindOrCreateVertex(v6);
    graph.FindOrCreateParentEdge(v2, v1);
    graph.FindOrCreateParentEdge(v1, v3);
    graph.FindOrCreateParentEdge(v3, v2);
    graph.FindOrCreateParentEdge(v4, v2);
    graph.FindOrCreateParentEdge(v4, v5);
    graph.FindOrCreateParentEdge(v4, v6);
    graph.FindOrCreateParentEdge(v5, v6);
    auto res = graph.GetSortedVertices();
    // AssertOrder(res, v3 , v2);
    // AssertOrder(res, v2 , v1);
    // AssertOrder(res, v1 , v4);
    // AssertOrder(res, v5 , v4);
    // AssertOrder(res, v6 , v5);

    string log = "";
    for (auto itr : res) {
      log += to_string(itr->id()) + ", ";
    }
    Log_debug("Sorted order: %s", log.c_str());
  }
};

}
