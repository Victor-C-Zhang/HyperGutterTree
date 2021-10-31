#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <fstream>
#include "../include/standalone_gutters.h"

static bool shutdown = false;

// queries the buffer tree and verifies that the data
// Should be run in a seperate thread
void querier(StandAloneGutters *wq) {
  data_ret_t data;
  while(true) {
    bool valid = wq->get_data(data);
    if(!valid && shutdown)
      return;
  }
}

void write_configuration(int queue_factor, int gutter_factor) {
  std::ofstream conf("./buffering.conf");
  conf << "queue_factor=" << queue_factor << std::endl;
  conf << "gutter_factor=" << gutter_factor << std::endl;
}

void run_test(const int nodes, const unsigned long updates) {
  shutdown = false;

  write_configuration(2, 1); // 2 is queue_factor, 1 is gutter_factor
  StandAloneGutters *wq = new StandAloneGutters(nodes, 40); // 40 is num workers

  // create queriers
  std::thread query_threads[40];
  for (int t = 0; t < 40; t++) {
    query_threads[t] = std::thread(querier, wq);
  }

  auto start = std::chrono::steady_clock::now();
  for (uint64_t i = 0; i < updates; i++) {
    if(i % 1000000000 == 0)
      printf("processed so far: %lu\n", i);
    
    update_t upd;
    upd.first = i % nodes;
    upd.second = (nodes - 1) - (i % nodes);
    wq->insert(upd);
    std::swap(upd.first, upd.second);
    wq->insert(upd);
  }
  wq->force_flush();
  shutdown = true;
  wq->set_non_block(true);

  std::chrono::duration<double> delta = std::chrono::steady_clock::now() - start;
  printf("Insertions took %f seconds: average rate = %f\n", delta.count(), updates/delta.count());

  for (int i = 0; i < 40; i++) {
    query_threads[i].join();
  }
  delete wq;
}

TEST(SA_Throughput, kron15) {
  run_test(8192, 17542263);
}

TEST(SA_Throughput, kron17) {
  run_test(131072, 4474931789);
}

TEST(SA_Throughput, kron18) {
  run_test(262144, 17891985703);
}
