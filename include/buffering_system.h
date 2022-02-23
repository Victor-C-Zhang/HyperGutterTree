#pragma once
#include <cstdint>
#include "types.h"
#include <math.h>

class BufferingSystem {
public:
  virtual insert_ret_t insert(const update_t &upd) = 0;
  virtual bool get_data(data_ret_t& data) = 0;
  virtual flush_ret_t force_flush() = 0;
  virtual void set_non_block(bool block) = 0;

  virtual ~BufferingSystem() {};

  static double sketch_size(node_id_t num_nodes) {
    return 42 * sizeof(node_id_t) * pow(log2(num_nodes), 2) / (log2(3) - 1); 
  }
protected:
    /**
   * Use buffering.conf configuration file to determine parameters of the buffering system
   * Sets the variables below
   * Queue_Factor :   The number of queue slots per worker removing data from the queue
   * Size_Factor  :   Decrease the amount of bytes used per node by this multiplicative factor
   */
  void configure_system();

  // various parameters utilized by the buffering systems
  uint32_t page_size;    // write granularity
  uint32_t buffer_size;  // size of an internal node buffer
  uint32_t fanout;       // maximum number of children per node
  uint32_t queue_factor; // number of elements in queue is this factor * num_workers
  uint32_t num_flushers; // the number of flush threads
  float gutter_factor;   // factor which increases/decreases the leaf gutter size
};
