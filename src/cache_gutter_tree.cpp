#include "cache_gutter_tree.h"

void CacheGutterTree::CacheGutterTreeNode::flush(bool force) {
  if (leafNode) {
    for (const auto &update : buffer) {
      auto &outputBuffer = outputBuffers[update.first - updateSpan.first];
      outputBuffer.push_back(update.second);
      if (outputBuffer.size() == config.bufferSize) {
        int data_size = config.bufferSize * sizeof(node_id_t);
        config.wq.push(reinterpret_cast<char *>(outputBuffer.data()), data_size);
        outputBuffer.clear();
        outputBuffer.push_back(updateSpan.first + update.first);
      }
    }
    if (force) {
      const size_t numOutputBuffers = updateSpan.second - updateSpan.first + 1;
      for (size_t i = 0; i < numOutputBuffers; ++i) {
        std::vector<node_id_t> buffer = outputBuffers[i];
        if (buffer.size() > 0) {
          int data_size = buffer.size() * sizeof(node_id_t);
          config.wq.push(reinterpret_cast<char *>(buffer.data()), data_size);
          buffer.clear();
          buffer.push_back(updateSpan.first + i);
        }
      }      
    }
  } else {
    const size_t spanQuanta = calculateSpanQuanta(updateSpan);
    for (const auto &update : buffer) {
      const size_t bucket = std::min((update.first-updateSpan.first) / spanQuanta, numChildren - 1);
      childNodes[bucket].insert(update);
    }
  }
  buffer.clear();
}

bool CacheGutterTree::get_data(data_ret_t &data) {
  // make a request to the circular buffer for data
  std::pair<int, queue_ret_t> queue_data;
  bool got_data = wq.peek(queue_data);

  if (!got_data) return false;  // we got no data so return not valid
  
  int i                  = queue_data.first;
  uint32_t len           = queue_data.second.first;
  node_id_t *serial_data = reinterpret_cast<node_id_t *>(queue_data.second.second);
  assert(len % sizeof(node_id_t) == 0);


  if (len == 0) return false;  // we got no data so return not valid
  // assume the first key is correct so extract it
  node_id_t key = serial_data[0];
  data.first = key;

  data.second.clear();  // remove any old data from the vector
  uint32_t vec_len = len / sizeof(node_id_t);
  data.second.reserve(vec_len);  // reserve space for our updates
  for (uint32_t j = 1; j < vec_len; ++j) {
    data.second.push_back(serial_data[j]);
  }

  wq.pop(i);  // mark the wq entry as clean
  return true;
}

void CacheGutterTree::set_non_block(bool block){
  wq.no_block = block; // should work queue peek block on empty queue
}
