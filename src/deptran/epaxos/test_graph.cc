#include "graph.h"


namespace janus {

#define AssertOrder(res, vertex1, vertex2) { \
          auto occ1 = find(res.begin(), res.end(), vertex1); \
          auto occ2 = find(res.begin(), res.end(), vertex2); \
          verify(occ1 != res.end()); \
          verify(occ2 != res.end()); \
          verify(occ1 - res.begin() < occ2  - res.begin()); \
        }
        
#define AssertExists(res, vertex1) { \
          verify(find(res.begin(), res.end(), vertex1) != res.end()); \
        }

#define AssertEmpty(res) verify(res.size() == 0);

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

  bool isFirstInSCC(shared_ptr<TestEVertex> &rhs) override {
    return this->priority > rhs->priority;
  }
};

class TestEGraph: public EGraph<TestEVertex> {};

class TestGraph {
 public:
  void EmptyGraph() {
    // TestEGraph graph;
    // auto res = graph.GetSortedVertices();
    // AssertEmpty(res);
  }

  void SingleNodeGraph() {
    // TestEGraph graph;
    // shared_ptr<TestEVertex> v1 = make_shared<TestEVertex>(1, 3);
    // graph.FindOrCreateVertex(v1);
    // auto res = graph.GetSortedVertices();
    // AssertExists(res, v1);
  }

  void UnconnectedNodesGraph() {
    // TestEGraph graph;
    // shared_ptr<TestEVertex> v1 = make_shared<TestEVertex>(1, 3);
    // shared_ptr<TestEVertex> v2 = make_shared<TestEVertex>(2, 2);
    // shared_ptr<TestEVertex> v3 = make_shared<TestEVertex>(3, 1);
    // graph.FindOrCreateVertex(v1);
    // graph.FindOrCreateVertex(v2);
    // graph.FindOrCreateVertex(v3);
    // auto res = graph.GetSortedVertices();
    // AssertExists(res, v1);
    // AssertExists(res, v2);
    // AssertExists(res, v3);
  }

  void SingleTreeGraph() {
    // TestEGraph graph;
    // shared_ptr<TestEVertex> v1 = make_shared<TestEVertex>(1, 3);
    // shared_ptr<TestEVertex> v2 = make_shared<TestEVertex>(2, 2);
    // shared_ptr<TestEVertex> v3 = make_shared<TestEVertex>(3, 1);
    // shared_ptr<TestEVertex> v4 = make_shared<TestEVertex>(4, 1);
    // shared_ptr<TestEVertex> v5 = make_shared<TestEVertex>(5, 2);
    // shared_ptr<TestEVertex> v6 = make_shared<TestEVertex>(6, 3);
    // graph.FindOrCreateVertex(v1);
    // graph.FindOrCreateVertex(v2);
    // graph.FindOrCreateVertex(v3);
    // graph.FindOrCreateVertex(v4);
    // graph.FindOrCreateVertex(v5);
    // graph.FindOrCreateVertex(v6);
    // graph.FindOrCreateParentEdge(v1, v2);
    // graph.FindOrCreateParentEdge(v1, v3);
    // graph.FindOrCreateParentEdge(v2, v4);
    // graph.FindOrCreateParentEdge(v2, v5);
    // graph.FindOrCreateParentEdge(v3, v6);
    // auto res = graph.GetSortedVertices();
    // AssertOrder(res, v2, v1);
    // AssertOrder(res, v3, v1);
    // AssertOrder(res, v4, v2);
    // AssertOrder(res, v5, v2);
    // AssertOrder(res, v6, v3);
  }

  void SingleSCCGraph() {
    // TestEGraph graph;
    // shared_ptr<TestEVertex> v1 = make_shared<TestEVertex>(1, 3);
    // shared_ptr<TestEVertex> v2 = make_shared<TestEVertex>(2, 2);
    // shared_ptr<TestEVertex> v3 = make_shared<TestEVertex>(3, 1);
    // shared_ptr<TestEVertex> v4 = make_shared<TestEVertex>(4, 1);
    // shared_ptr<TestEVertex> v5 = make_shared<TestEVertex>(5, 2);
    // shared_ptr<TestEVertex> v6 = make_shared<TestEVertex>(6, 3);
    // graph.FindOrCreateVertex(v1);
    // graph.FindOrCreateVertex(v2);
    // graph.FindOrCreateVertex(v3);
    // graph.FindOrCreateVertex(v4);
    // graph.FindOrCreateVertex(v5);
    // graph.FindOrCreateVertex(v6);
    // graph.FindOrCreateParentEdge(v1, v2);
    // graph.FindOrCreateParentEdge(v1, v3);
    // graph.FindOrCreateParentEdge(v2, v4);
    // graph.FindOrCreateParentEdge(v2, v5);
    // graph.FindOrCreateParentEdge(v3, v6);
    // graph.FindOrCreateParentEdge(v4, v1);
    // graph.FindOrCreateParentEdge(v5, v3);
    // graph.FindOrCreateParentEdge(v6, v2);
    // auto res = graph.GetSortedVertices();
    // AssertOrder(res, v1, v6);
    // AssertOrder(res, v6, v2);
    // AssertOrder(res, v2, v5);
    // AssertOrder(res, v5, v3);
    // AssertOrder(res, v3, v4);
  }

  void MultipleSCCGraph() {
    // TestEGraph graph;
    // shared_ptr<TestEVertex> v1 = make_shared<TestEVertex>(1, 3);
    // shared_ptr<TestEVertex> v2 = make_shared<TestEVertex>(2, 2);
    // shared_ptr<TestEVertex> v3 = make_shared<TestEVertex>(3, 1);
    // shared_ptr<TestEVertex> v4 = make_shared<TestEVertex>(4, 1);
    // shared_ptr<TestEVertex> v5 = make_shared<TestEVertex>(5, 2);
    // shared_ptr<TestEVertex> v6 = make_shared<TestEVertex>(6, 3);
    // shared_ptr<TestEVertex> v7 = make_shared<TestEVertex>(7, 2);
    // graph.FindOrCreateVertex(v1);
    // graph.FindOrCreateVertex(v2);
    // graph.FindOrCreateVertex(v3);
    // graph.FindOrCreateVertex(v4);
    // graph.FindOrCreateVertex(v5);
    // graph.FindOrCreateVertex(v6);
    // graph.FindOrCreateVertex(v7);
    // graph.FindOrCreateParentEdge(v1, v3);
    // graph.FindOrCreateParentEdge(v3, v2);
    // graph.FindOrCreateParentEdge(v2, v1);
    // graph.FindOrCreateParentEdge(v4, v2);
    // graph.FindOrCreateParentEdge(v4, v5);
    // graph.FindOrCreateParentEdge(v4, v6);
    // graph.FindOrCreateParentEdge(v4, v7);
    // graph.FindOrCreateParentEdge(v7, v4);
    // graph.FindOrCreateParentEdge(v5, v6);
    // auto res = graph.GetSortedVertices();
    // AssertOrder(res, v1, v2);
    // AssertOrder(res, v2, v3);
    // AssertOrder(res, v3, v4);
    // AssertOrder(res, v7, v4);
    // AssertOrder(res, v5, v4);
    // AssertOrder(res, v6, v5);
  }

  void Run() {
    SingleNodeGraph();
    UnconnectedNodesGraph();
    SingleTreeGraph();
    MultipleSCCGraph();
  }
};

}
