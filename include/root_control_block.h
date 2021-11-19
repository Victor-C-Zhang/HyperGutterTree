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
  RootControlBlock(buffer_id_t id, uint32_t off, uint32_t buf_size);

  // The current buffer being inserted to
  inline uint8_t cur_which() { return which_buf; }

  // one of the buffers in particular
  inline BufferControlBlock *get_buf(uint8_t which) { return buffers[which]; }

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

  /** Insert to this root and block if the root is completely full
   * @param gt    the GutterTree this buffer is a part of
   * @param upd   the update to write to this root
   * @return      nothing
   */
  insert_ret_t insert(const GutterTree *gt, const update_t &upd);

  inline void print() {
    printf("Root buffer %i:\n", id);
    printf("\t"); buffers[0]->print();
    printf("\t"); buffers[1]->print();
  }
};
