#include <cassert>
#include <fstream>
#include "../include/standalone_gutters.h"

const unsigned first_idx = 2;

StandAloneGutters::StandAloneGutters(node_id_t num_nodes, int workers) :
buffers(num_nodes) {
  configure_system(); // read buffering configuration file

  // size of leaf proportional to size of sketch (add 2 because we have 2 metadata slots per buffer)
  uint32_t bytes_size = floor(gutter_factor * sketch_size(num_nodes)) + 2 * sizeof(node_id_t);
  buffer_size = bytes_size / sizeof(node_id_t);

  wq = new WorkQueue(workers * queue_factor, bytes_size);

  for (node_id_t i = 0; i < num_nodes; ++i) {
    buffers[i].reserve(buffer_size);
    buffers[i].push_back(first_idx); // first spot will point to the next free space
    buffers[i].push_back(i); // second spot identifies the node to which the buffer
  }
}

StandAloneGutters::~StandAloneGutters() {
  delete wq;
}

void StandAloneGutters::flush(node_id_t *buffer, uint32_t num_bytes) {
  wq->push(reinterpret_cast<char *>(buffer), num_bytes);
}

insert_ret_t StandAloneGutters::insert(const update_t &upd) {
  std::vector<node_id_t> &ptr = buffers[upd.first];
  ptr.emplace_back(upd.second);
  if (ptr.size() == buffer_size) { // full, so request flush
    ptr[0] = buffer_size;
    flush(ptr.data(), buffer_size*sizeof(node_id_t));
    node_id_t i = ptr[1];
    ptr.clear();
    ptr.push_back(first_idx);
    ptr.push_back(i);
  }
}

// basically a copy of BufferTree::get_data()
bool StandAloneGutters::get_data(data_ret_t &data) {
  // make a request to the circular buffer for data
  std::pair<int, queue_elm> queue_data;
  bool got_data = wq->peek(queue_data);

  if (!got_data)
    return false; // we got no data so return not valid

  int i         = queue_data.first;
  queue_elm elm = queue_data.second;
  auto *serial_data = reinterpret_cast<node_id_t *>(elm.data);
  uint32_t len      = elm.size;
  assert(len % sizeof(node_id_t) == 0);

  if (len == 0)
    return false; // we got no data so return not valid

  // assume the first key is correct so extract it
  node_id_t key = serial_data[1];
  data.first = key;

  data.second.clear(); // remove any old data from the vector
  uint32_t vec_len  = len / sizeof(node_id_t);
  data.second.reserve(vec_len); // reserve space for our updates

  for (uint32_t j = first_idx; j < vec_len; ++j) {
    data.second.push_back(serial_data[j]);
  }

  wq->pop(i); // mark the wq entry as clean
  return true;
}

flush_ret_t StandAloneGutters::force_flush() {
  for (auto & buffer : buffers) {
    if (buffer.size() != first_idx) { // have stuff to flush
      buffer[0] = buffer.size();
      flush(buffer.data(), buffer[0]*sizeof(node_id_t));
      node_id_t i = buffer[1];
      buffer.clear();
      buffer.push_back(0);
      buffer.push_back(i);
    }
  }
}

void StandAloneGutters::set_non_block(bool block) {
  if (block) {
    wq->no_block = true; // circular queue operations should no longer block
    wq->wq_empty.notify_all();
  } else {
    wq->no_block = false; // set circular queue to block if necessary
  }
}
