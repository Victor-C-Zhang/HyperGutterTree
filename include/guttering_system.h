#pragma once
#include "types.h"
#include <math.h>

class GutteringSystem {
public:
  virtual insert_ret_t insert(const update_t &upd) = 0; // insert an element to the buffering system
  virtual bool get_data(data_ret_t& data) = 0;          // get data out of the buffering system
  virtual flush_ret_t force_flush() = 0;                // force all data out of buffers
  virtual void set_non_block(bool block) = 0;
  virtual int gutter_size() = 0; 

  virtual ~GutteringSystem() {};

  static double sketch_size(node_id_t num_nodes) {
    return 42 * sizeof(node_id_t) * pow(log2(num_nodes), 2) / (log2(3) - 1); 
  }
protected:
  /*
   * Use buffering.conf configuration file to determine parameters of the buffering system
   * Sets the parameters listed below
   */
  static void configure_system();

  // various parameters utilized by the buffering systems
  static uint32_t page_size;    // write granularity
  static uint32_t buffer_size;  // size of an internal node buffer
  static uint32_t fanout;       // maximum number of children per node
  static uint32_t queue_factor; // number of elements in queue is this factor * num_workers
  static uint32_t num_flushers; // the number of flush threads
  static float gutter_factor;   // factor which increases/decreases the leaf gutter size
};
