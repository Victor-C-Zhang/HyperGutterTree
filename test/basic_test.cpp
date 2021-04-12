#include <gtest/gtest.h>
#include "../include/buffer_tree.h"

#define KB 1 << 10
#define MB 1 << 20
#define GB 1 << 30

// helper function to run a basic test of the buffer tree with
// various parameters
// this test only works if the depth of the tree does not exceed 1
// and no work is claimed off of the work queue
void run_test(const int nodes, const int num_updates, const int buffer_size, const int branch_factor) {
  BufferTree *buf_tree = new BufferTree("./test_", buffer_size, branch_factor, nodes, true);

  for (int i = 0; i < num_updates; i++) {
    update_t upd;
    upd.first.first = i % nodes;
    upd.first.second = (nodes - 1) - (i % nodes);
    upd.second = true;
    buf_tree->insert(upd);
  }
  buf_tree->force_flush();

  // query for the 0 key at tag index 0
  data_ret_t ret = buf_tree->get_data(0, 0);
  Node key = ret.first;
  std::vector<std::pair<Node, bool>> updates = ret.second;
  // how many updates to we expect to see to each node
  int per_node = num_updates / nodes;

  ASSERT_EQ(0, key);

  int count = 0;

  for (std::pair<Node, bool> upd : updates) {
    // printf("edge to %d\n", upd.first);
    ASSERT_EQ(nodes - 1, upd.first);
    ASSERT_EQ(true, upd.second);
    count++;
  }
  ASSERT_EQ(per_node, count);
  delete buf_tree;
}

TEST(UtilTestSuite, TestInsertBasicSmall) {
  const int nodes = 10;
  const int num_updates = 400;
  const int buf = KB;
  const int branch = 2;

  run_test(nodes, num_updates, buf, branch);
}

TEST(UtilTestSuite, TestInsertBasicMedium) {
  const int nodes = 100;
  const int num_updates = 360000;
  const int buf = MB;
  const int branch = 8;

  run_test(nodes, num_updates, buf, branch);
}
