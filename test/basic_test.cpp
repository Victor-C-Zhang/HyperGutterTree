#include <gtest/gtest.h>
#include <math.h>
#include <set>
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

  for (int i = 0; i < num_updates; i++) {
    update_t upd;
    upd.first = i % nodes;
    upd.second = (nodes - 1) - (i % nodes);
    buf_tree->insert(upd);
  }
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

TEST(BasicInsert, Small) {
  const int nodes = 10;
  const int num_updates = 400;
  const int buf = KB << 2;
  const int branch = 2;

  run_test(nodes, num_updates, buf, branch);
}

TEST(BasicInsert, Medium) {
  const int nodes = 100;
  const int num_updates = 360000;
  const int buf = MB;
  const int branch = 8;

  run_test(nodes, num_updates, buf, branch);
}

// test where we fill the lowest buffers as full as we can
// with insertions.
TEST(BasicInsert, FillLowest) {
  uint updates = (8 * MB) / BufferTree::serial_update_size;
  updates -= updates % 8 + 8;

  const int nodes = 8;
  const int num_updates = updates;
  const int buf = MB;
  const int branch = 2;

  run_test(nodes, num_updates, buf, branch);
}
