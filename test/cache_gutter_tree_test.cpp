#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <fstream>
#include <math.h>
#include "../include/cache_gutter_tree.h"

#define KB (1 << 10)
#define MB (1 << 20)
#define GB (1 << 30)

static bool shutdown = false;
static std::atomic<uint32_t> upd_processed;

// queries the buffer tree and verifies that the data
// returned makes sense
// Should be run in a seperate thread
static void querier(CacheGutterTree *cache_tree, int nodes) {
  data_ret_t data;
  while(true) {
    bool valid = cache_tree->get_data(data);
    if (valid) {
      node_id_t key = data.first;
      std::vector<size_t> updates = data.second;
      // verify that the updates are all between the correct nodes
      for (auto upd : updates) {
        // printf("edge to %d\n", upd.first);
        ASSERT_EQ(nodes - (key + 1), upd) << "key " << key;
        upd_processed += 1;
      }
    }
    else if(shutdown)
      return;
  }
}

static void write_configuration(int queue_factor, int gutter_factor) {
  std::ofstream conf("./buffering.conf");
  conf << "queue_factor=" << queue_factor << std::endl;
  conf << "gutter_factor=" << gutter_factor << std::endl;
}

// helper function to run a basic test of the buffer tree with
// various parameters
// this test only works if the depth of the tree does not exceed 1
// and no work is claimed off of the work queue
// to work correctly num_updates must be a multiple of nodes
static void run_test(const int nodes, const int num_updates, const int gutter_factor) {
  printf("Standalone Gutters => Running Test: nodes=%i num_updates=%i gutter_factor %i\n",
         nodes, num_updates, gutter_factor);

  write_configuration(8, gutter_factor); // 8 is queue_factor

  CacheGutterTree *cache_tree = new CacheGutterTree(nodes, 1); // 1 is the number of workers
  shutdown = false;
  upd_processed = 0;
  std::thread qworker(querier, cache_tree, nodes);

  for (int i = 0; i < num_updates; i++) {
    update_t upd;
    upd.first = i % nodes;
    upd.second = (nodes - 1) - (i % nodes);
    cache_tree->insert(upd);
  }
  printf("force flush\n");
  cache_tree->force_flush();
  shutdown = true;
  cache_tree->set_non_block(true); // switch to non-blocking calls in an effort to exit
  qworker.join();
  ASSERT_EQ(num_updates, upd_processed);
  delete cache_tree;
}

TEST(CacheGutterTree, Small) {
  const int nodes = 10;
  const int num_updates = 400;
  const int gutter_factor = 1;

  run_test(nodes, num_updates, gutter_factor);
}

TEST(CacheGutterTree, Medium) {
  const int nodes = 100;
  const int num_updates = 360000;
  const int gutter_factor = 1;

  run_test(nodes, num_updates, gutter_factor);
}

TEST(CacheGutterTree, ManyInserts) {
  const int nodes = 32;
  const int num_updates = 1000000;
  const int gutter_factor = 1;

  run_test(nodes, num_updates, gutter_factor);
}

TEST(CacheGutterTree, AsAbstract) {
  const int nodes = 10;
  const int num_updates = 400;
  const int gutter_factor = 1;

  write_configuration(8, gutter_factor);

  GutteringSystem *buf = new CacheGutterTree(nodes, 1);
  shutdown = false;
  upd_processed = 0;
  std::thread qworker(querier, (CacheGutterTree *) buf, nodes);

  for (int i = 0; i < num_updates; i++) {
    update_t upd;
    upd.first = i % nodes;
    upd.second = (nodes - 1) - (i % nodes);
    buf->insert(upd);
  }
  printf("force flush\n");
  buf->force_flush();
  shutdown = true;
  buf->set_non_block(true); // switch to non-blocking calls in an effort to exit
  qworker.join();
  ASSERT_EQ(num_updates, upd_processed);
  delete buf;
}

// test designed to stress test a small number of buffers
TEST(CacheGutterTree, HitNodePairs) {
  const int nodes       = 32;
  const int full_buffer = GutteringSystem::sketch_size(nodes) / sizeof(node_id_t);
  const int num_updates = 20 * full_buffer;

  write_configuration(8, 8); // 8 is queue_factor, -8 is gutter_factor (small gutters)

  CacheGutterTree *cache_tree = new CacheGutterTree(nodes, 1); // 1 is the number of workers
  shutdown = false;
  upd_processed = 0;
  std::thread qworker(querier, cache_tree, nodes);
  
  for (int n = 0; n < num_updates / full_buffer; n++) {
    for (int i = 0; i < full_buffer; i++) {
      update_t upd;
      upd.first = n;
      upd.second = (nodes - 1) - (n % nodes);
      cache_tree->insert(upd);
    }
  }
  cache_tree->force_flush();
  shutdown = true;
  cache_tree->set_non_block(true); // switch to non-blocking calls in an effort to exit

  qworker.join();
  ASSERT_EQ(num_updates, upd_processed);
  delete cache_tree;
}


TEST(CacheGutterTree, ManyQueryThreads) {
  const int nodes       = 1024;
  const int num_updates = 5206;

  // here we limit the number of slots in the circular queue to 
  // create contention between the threads. (we pass 5 threads and queue factor =1 instead of 20,8)
  write_configuration(1, -8); // 1 is queue_factor, -8 is gutter_factor

  CacheGutterTree *cache_tree = new CacheGutterTree(nodes, 5); // 5 is the number of workers
  shutdown = false;
  upd_processed = 0;
  std::thread query_threads[20];
  for (int t = 0; t < 20; t++) {
    query_threads[t] = std::thread(querier, cache_tree, nodes);
  }
  
  for (int i = 0; i < num_updates; i++) {
    update_t upd;
    upd.first = i % nodes;
    upd.second = (nodes - 1) - (i % nodes);
    cache_tree->insert(upd);
  }
  cache_tree->force_flush();
  shutdown = true;
  cache_tree->set_non_block(true); // switch to non-blocking calls in an effort to exit

  for (int t = 0; t < 20; t++) {
    query_threads[t].join();
  }
  ASSERT_EQ(num_updates, upd_processed);
  delete cache_tree;
}
