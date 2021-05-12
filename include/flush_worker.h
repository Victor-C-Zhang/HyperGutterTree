//
// Created by victor on 5/11/21.
//

#ifndef FASTBUFFERTREE_FLUSH_WORKER_H
#define FASTBUFFERTREE_FLUSH_WORKER_H

#include <set>
#include <atomic>
#include <gtest/gtest_prod.h>
#include "buffer_control_block.h"
#include "buffer_tree.h"

// forward declaration
class BufferTree;

struct FlushQueueEntryCompare {
  bool operator()(BufferControlBlock const *e1, BufferControlBlock const *e2) {
    if (e1->priority != e2->priority) return e1->priority < e2->priority;
    if (e1->timestamp != e2->timestamp) return e1->timestamp < e2->timestamp;
    return e1 > e2;
  }
};

/**
 * Class to handle distributed async flushing.
 */
class FlushWorker {
  // priority-based queuing system
  std::set<BufferControlBlock*,FlushQueueEntryCompare> flush_queue;

  // sequential access to priority queue
  std::mutex pq_mutex;
  // to alert the manager thread when there are things to be flushed
  std::condition_variable pq_cv;

  BufferTree* bt;

  /**
   * Utility to flush the first BCB in the pqueue.
   */
  void do_flush();

  FRIEND_TEST(FlushWorkerTestSuite, PriorityQueueOrderingTest);
public:
  // whether we can quit processing flushes
  std::atomic_bool can_exit;

  FlushWorker(BufferTree* bufferTree);

  /**
   * Function to listen for and assign flush requests.
   */
  void listen();

  /**
   * Pushes a BCB onto the flush queue. If the priority requested is higher
   * than the current priority, update it.
   * @param bcb
   * @param priority    the priority of the job.
   */
  void request_flush(BufferControlBlock* bcb, int priority);
};


#endif //FASTBUFFERTREE_FLUSH_WORKER_H
