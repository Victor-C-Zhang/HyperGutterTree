#include <cassert>
#include <fstream>
#include "../include/standalone_gutters.h"

StandAloneGutters::StandAloneGutters(node_id_t num_nodes, int workers) : gutters(num_nodes) {
  configure_system(); // read buffering configuration file

  // size of leaf proportional to size of sketch
  buffer_size = gutter_factor * sketch_size(num_nodes) / sizeof(node_id_t);
  if (buffer_size < 4) buffer_size = 4;
  wq = new WorkQueue(workers * queue_factor, buffer_size);

  for (node_id_t i = 0; i < num_nodes; ++i) {
    gutters[i].buffer.reserve(buffer_size);
    gutters[i].buffer.push_back(i); // second spot identifies the node to which the buffer
  }
}

StandAloneGutters::~StandAloneGutters() {
  delete wq;
}

void StandAloneGutters::flush(node_id_t node_idx, std::vector<node_id_t> &buffer) {
  wq->push(node_idx, buffer);
}

insert_ret_t StandAloneGutters::insert(const update_t &upd) {
  Gutter &gutter = gutters[upd.first];
  std::vector<node_id_t> &ptr = gutter.buffer;
  const std::lock_guard<std::mutex> lock(gutter.mux);
  ptr.push_back(upd.second);
  if (ptr.size() == buffer_size) { // full, so request flush
    flush(upd.first, ptr);
    ptr.clear();
  }
}

flush_ret_t StandAloneGutters::force_flush() {
  for (node_id_t node_idx = 0; node_idx < gutters.size(); node_idx++) {
    const std::lock_guard<std::mutex> lock(gutters[node_idx].mux);
    if (gutters[node_idx].buffer.size() > 1) { // have stuff to flush
      flush(node_idx, gutters[node_idx].buffer);
      gutters[node_idx].buffer.clear();
    }
  }
}
