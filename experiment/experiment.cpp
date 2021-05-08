#include <gtest/gtest.h>
#include <math.h>
#include <set>
#include <chrono>
#include "../include/buffer_tree.h"

#define KB 1 << 10
#define MB 1 << 20
#define GB 1 << 30

// helper function to run a basic test of the buffer tree with
// various parameters
// this test only works if the depth of the tree does not exceed 1
// and no work is claimed off of the work queue
// to work correctly num_updates must be a multiple of nodes
void run_test(const int nodes, const int num_updates, const int buffer_size, const int branch_factor) {
  printf("Running Test: nodes=%i num_updates=%i buffer_size %i branch_factor %i\n",
         nodes, num_updates, buffer_size, branch_factor);

  BufferTree *buf_tree = new BufferTree("./test_", buffer_size, branch_factor, nodes, true);
  
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < num_updates; i++) {
    update_t upd;
    upd.first = i % nodes;
    upd.second = (nodes - 1) - (i % nodes);
    buf_tree->insert(upd);
  }
  std::chrono::duration<double> delta = std::chrono::steady_clock::now() - start;
  printf("insertions took %f seconds: average rate = %f\n", delta.count(), num_updates/delta.count());
  buf_tree->force_flush();

  std::set<Node> visited;

  // calculate the tag index for node 0 based upon max_level and max_buffer_size
  work_t task;
  while (!buf_tree->work_queue.empty()) {
    task = buf_tree->work_queue.front();
    buf_tree->work_queue.pop();

    if (visited.count(task.first) > 0) {
      continue;
    }
    visited.insert(task.first);
    
    // do the query
    data_ret_t ret = buf_tree->get_data(task);
    Node key = ret.first;
    std::vector<Node> updates = ret.second;
    // how many updates to we expect to see to each node
    int per_node = num_updates / nodes;

    int count = 0;

    for (Node upd : updates) {
      // printf("edge to %d\n", upd.first);
      ASSERT_EQ(nodes - (key + 1), upd) << "key " << key;
      count++;
    }
    ASSERT_EQ(per_node, count) << "key " << key;
  }
  
  delete buf_tree;
}

TEST(Experiment, LargeNarrow) {
  const int nodes = 512;
  const int num_updates = MB << 5;
  const int buf = MB;
  const int branch = 4;

  run_test(nodes, num_updates, buf, branch);
}

TEST(Experiment, LargeWide) {
  const int nodes = 512;
  const int num_updates = MB << 5;
  const int buf = MB;
  const int branch = 16;

  run_test(nodes, num_updates, buf, branch);
}

TEST(Experiment, ExtraLarge) {
  const int nodes = 1024;
  const int num_updates = MB << 8;
  const int buf = MB << 1;
  const int branch = 16;

  run_test(nodes, num_updates, buf, branch);
}
