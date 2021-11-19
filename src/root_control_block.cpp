#include "../include/root_control_block.h"
#include "../include/gutter_tree.h"
#include <cstring>

RootControlBlock::RootControlBlock(buffer_id_t id, File_Pointer off, uint32_t buf_size) :
 id(id), buffer_size(buf_size) {
  buffers[0] = new BufferControlBlock(0, off, 0);
  buffers[1] = new BufferControlBlock(1, off + buffer_size, 0);
}

/*
 * Function to convert an update_t to a char array
 * @param   dst the memory location to put the serialized data
 * @param   src the edge update
 * @return  nothing
 */
inline static void serialize_update(char *dst, update_t src) {
  node_id_t node1 = src.first;
  node_id_t node2 = src.second;

  memcpy(dst, &node1, sizeof(node_id_t));
  memcpy(dst + sizeof(node_id_t), &node2, sizeof(node_id_t));
}

insert_ret_t RootControlBlock::insert(const GutterTree *gt, const update_t &upd) {
  if (just_switched) { // only get lock and check size on switching to new buffer
    while (true) {
      std::unique_lock<std::mutex> lk(BufferControlBlock::buffer_ready_lock);
      BufferControlBlock::buffer_ready.wait(lk, [this]{return buffers[which_buf]->size() < buffer_size;});
      if (buffers[which_buf]->size() < buffer_size) {
        lk.unlock();
        break;
      }
      lk.unlock();
    }
    just_switched = false;
  }
  char *write_loc = gt->get_cache() + buffers[which_buf]->size() + buffers[which_buf]->offset();
  serialize_update(write_loc, upd);

  // Advance the pointer and switch to the other buffer if necessary.
  // When we switch to the next buffer we add the current buffer
  // to the flush_queue
  buffers[which_buf]->set_size(buffers[which_buf]->size() + GutterTree::serial_update_size);
  if (buffers[which_buf]->size() == buffer_size) {
    // Add the BufferControlBlock to the flush queue
    BufferFlusher::queue_lock.lock();
    BufferFlusher::flush_queue.push({this, which_buf});
    BufferFlusher::queue_lock.unlock();
    BufferFlusher::flush_ready.notify_one();
    // Switch to the other BufferControlBlock
    which_buf = (which_buf + 1) % 2;
    just_switched = true;
  }
}
