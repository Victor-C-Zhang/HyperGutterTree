#include <cassert>
#include <fstream>
#include "../include/standalone_gutters.h"

StandAloneGutters::StandAloneGutters(node_id_t num_nodes, uint32_t workers, uint32_t inserters) : GutteringSystem(num_nodes, workers), gutters(num_nodes), inserters(inserters) 
{
  for (node_id_t i = 0; i < num_nodes; ++i) {
    gutters[i].buffer.reserve(leaf_gutter_size);
  }
  for (node_id_t i = 0; i < inserters; ++i) {
    local_buffers.emplace_back(num_nodes);
    for (node_id_t j = 0; j < num_nodes; ++j) {
      local_buffers[i][j].buffer.reserve(local_buf_size);
    }
  }
}

insert_ret_t StandAloneGutters::insert(const update_t &upd, int which) {
  Gutter &gutter = local_buffers[which][upd.first];
  //const std::lock_guard<std::mutex> lock(gutter.mux);
  std::vector<node_id_t> &ptr = gutter.buffer;
  ptr.push_back(upd.second);
  if (ptr.size() == local_buf_size) { // full, so request flush
    const std::lock_guard<std::mutex> lock(gutters[upd.first].mux);
    insert_batch(which, upd.first);
    ptr.clear();
  }
}
insert_ret_t StandAloneGutters::insert(const update_t &upd) {
  insert(upd, 0);
}

// We already hold the lock on both buffers
insert_ret_t StandAloneGutters::insert_batch(int which, node_id_t gutterid) {
  Gutter &gutter_local = local_buffers[which][gutterid];
  Gutter &gutter = gutters[gutterid];
  std::vector<node_id_t> &ptr_local = gutter_local.buffer;
  std::vector<node_id_t> &ptr = gutter.buffer;

  for (size_t i = 0; i < ptr_local.size(); i++)
  {
    ptr.push_back(ptr_local[i]);
    if (ptr.size() == leaf_gutter_size) { // full, so request flush
      wq.push(gutterid, ptr);
      ptr.clear();
    }
  }
  ptr_local.clear();
}

flush_ret_t StandAloneGutters::force_flush() {
  for (node_id_t node_idx = 0; node_idx < gutters.size(); node_idx++) {
    const std::lock_guard<std::mutex> lock(gutters[node_idx].mux);
    for (uint32_t which = 0; which < inserters; which++)
    {
      //const std::lock_guard<std::mutex> lock(local_buffers[which][node_idx].mux);
      insert_batch(which, node_idx);
    }
    if (!gutters[node_idx].buffer.empty()) { // have stuff to flush
      wq.push(node_idx, gutters[node_idx].buffer);
      gutters[node_idx].buffer.clear();
    }
  }
}
