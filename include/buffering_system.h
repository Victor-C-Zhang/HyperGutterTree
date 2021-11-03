#pragma once
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
    return 42 * sizeof(node_id_t) * pow(log2(num_nodes), 2); 
  }
};
