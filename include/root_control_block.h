#pragma once

#include "buffer_control_block.h"
#include "buffer_flusher.h"

class RootControlBlock {
private:
  // id of the root
  const buffer_id_t id;

  // lock for controlling flushing this root and its sub-tree
  std::mutex flush_lock;

  // the two buffers which compose the root
  BufferControlBlock *buffers[2];

  // which buffer are we currently using to insert
  uint8_t which_buf = 0;

  // variable to control if we need to use a lock to check size
  // this happens when we switch from one buffer to the other
  bool just_switched = false;

  // keep a copy of the GutterTree's buffer size
  uint32_t buffer_size;
public:
  /** Constructor for a root control block
   * Manages two buffers of insertions at the root of a sub-tree
   * When one buffer fills up we switch to inserting to the other.
   * @param id         The id of the root
   * @param off        The offset in the cache that this root begins at
   * @param buf_size   The GutterTree this root is a component of
   * @return           A new RootControlBlock
   */
  RootControlBlock(buffer_id_t id, File_Pointer off, uint32_t buf_size) :
   id(id), buffer_size(buf_size) {
    buffers[0] = new BufferControlBlock(0, off, 0);
    buffers[1] = new BufferControlBlock(1, off + buffer_size, 0);
  }

  // The current buffer being inserted to
  inline uint8_t cur_which() { return which_buf; }

  // one of the buffers in particular
  inline BufferControlBlock *get_buf(uint8_t which) { return buffers[which]; }

  inline buffer_id_t get_id() { return id; }

  /** Synchronization functions. Should be called when root buffers are read or written to.
   * Other buffers should not require synchronization
   * We keep two locks to allow writes to root to happen while flushing
   * but need to enforce that flushes are done sequentially
   */
  inline void lock_flush()   {flush_lock.lock();}
  inline void unlock_flush() {flush_lock.unlock();}

  // call this function after constructing the GutterTree to copy
  // BufferControlBlock meta-data from the first buffer to the second
  inline void finish_setup() {
    buffers[1]->min_key = buffers[0]->min_key;
    buffers[1]->max_key = buffers[0]->max_key;
    buffers[1]->children_num = buffers[0]->children_num;
    buffers[1]->first_child  = buffers[0]->first_child;
  }

  inline void check_block() {
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
  }

  // switch to the other buffer if necessary.
  // When we switch to the next buffer we add the current buffer
  // to the flush_queue
  inline void check_cur_full() {
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

  inline void print() {
    printf("Root buffer %i:\n", id);
    printf("\t"); buffers[0]->print();
    printf("\t"); buffers[1]->print();
  }
};
