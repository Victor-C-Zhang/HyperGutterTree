#pragma once
#include "types.h"
#include "work_queue.h"
#include <math.h>

class GutteringSystem {
public:
  virtual insert_ret_t insert(const update_t &upd) = 0; //insert an element to the guttering system
  virtual flush_ret_t force_flush() = 0;                //force all data out of buffers
  virtual ~GutteringSystem() {};

  // get the size of a work queue elmement in bytes
  int gutter_size() {
    return upds_per_gutter() * sizeof(size_t);
  }

  static double sketch_size(node_id_t num_nodes) {
    return 42 * sizeof(node_id_t) * pow(log2(num_nodes), 2) / (log2(3) - 1); 
  }

  // get data out of the guttering system either one gutter at a time or in a batched fashion
  bool get_data_batched(std::vector<WorkQueue::DataNode *> &batched_data, int batch_size) 
    { return wq->peek_batch(batched_data, batch_size); }
  bool get_data(WorkQueue::DataNode *&data) { return wq->peek(data); }
  void get_data_callback(WorkQueue::DataNode *data) { wq->peek_callback(data); }
  void set_non_block(bool block) { wq->set_non_block(block);} //set non-blocking calls in wq
protected:
  virtual int upds_per_gutter() = 0;

  /*
   * Use buffering.conf configuration file to determine parameters of the guttering system
   * Sets the parameters listed below
   */
  static void configure_system();

  // various parameters utilized by the guttering systems
  static uint32_t page_size;    // write granularity
  static uint32_t buffer_size;  // size of an internal node buffer
  static uint32_t fanout;       // maximum number of children per node
  static uint32_t queue_factor; // number of elements in queue is this factor * num_workers
  static uint32_t num_flushers; // the number of flush threads
  static float gutter_factor;   // factor which increases/decreases the leaf gutter size

  WorkQueue *wq;
};
