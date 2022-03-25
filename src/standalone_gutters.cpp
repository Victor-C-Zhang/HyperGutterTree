#include <cassert>
#include <fstream>
#include "../include/standalone_gutters.h"

StandAloneGutters::StandAloneGutters(node_id_t num_nodes, int workers) : GutteringSystem(num_nodes, workers), gutters(num_nodes) {
  for (node_id_t i = 0; i < num_nodes; ++i) {
    gutters[i].buffer.reserve(leaf_gutter_size);
  }
}

insert_ret_t StandAloneGutters::insert(const update_t &upd) {
  Gutter &gutter = gutters[upd.first];
  const std::lock_guard<std::mutex> lock(gutter.mux);
  std::vector<node_id_t> &ptr = gutter.buffer;
  ptr.push_back(upd.second);
  if (ptr.size() == leaf_gutter_size) { // full, so request flush
    wq.push(upd.first, ptr);
    ptr.clear();
  }
}

flush_ret_t StandAloneGutters::force_flush() {
  for (node_id_t node_idx = 0; node_idx < gutters.size(); node_idx++) {
    const std::lock_guard<std::mutex> lock(gutters[node_idx].mux);
    if (!gutters[node_idx].buffer.empty()) { // have stuff to flush
      wq.push(node_idx, gutters[node_idx].buffer);
      gutters[node_idx].buffer.clear();
    }
  }
}
