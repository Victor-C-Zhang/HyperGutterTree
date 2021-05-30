#include <gtest/gtest.h>
#include <math.h>
#include <thread>
#include <chrono>
#include "../include/buffer_tree.h"

#define KB 1 << 10
#define MB 1 << 20
#define GB 1 << 30

static bool shutdown = false;

// queries the buffer tree and verifies that the data
// returned makes sense
// Should be run in a seperate thread
void querier(BufferTree *buf_tree, int nodes) {
  printf("creating query thread for buffertree\n");
  data_ret_t data;
  while(true) {
    bool valid = buf_tree->get_data(shutdown, data);
    if (valid) {
      Node key = data.first;
      std::vector<Node> updates = data.second;
      // verify that the updates are all between the correct nodes
      for (Node upd : updates) {
        // printf("edge to %d\n", upd.first);
        ASSERT_EQ(nodes - (key + 1), upd) << "key " << key;
      }
    }
    else if(shutdown)
      return;
  }
}

// helper function to run a basic test of the buffer tree with
// various parameters
// this test only works if the depth of the tree does not exceed 1
// and no work is claimed off of the work queue
// to work correctly num_updates must be a multiple of nodes
void run_test(const int nodes, const int num_updates, const int buffer_size, const int branch_factor) {
  printf("Running Test: nodes=%i num_updates=%i buffer_size %i branch_factor %i\n",
         nodes, num_updates, buffer_size, branch_factor);

  BufferTree *buf_tree = new BufferTree("./test_", buffer_size, branch_factor, nodes, 1, true);
  shutdown = false;
  std::thread qworker(querier, buf_tree, nodes);

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
  shutdown = true;
  buf_tree->bypass_wait(); // tell any waiting threads to reset

  delta = std::chrono::steady_clock::now() - start;
  printf("insert+force_flush took %f seconds: average rate = %f\n", delta.count(), num_updates/delta.count());

  qworker.join();
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
