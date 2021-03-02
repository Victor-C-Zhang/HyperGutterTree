#ifndef FASTBUFFERTREE_BUFFER_TREE_H
#define FASTBUFFERTREE_BUFFER_TREE_H

#include <cstdint>
#include <string>
#include <vector>
#include <queue>
#include "update.h"
#include "buffer_control_block.h"
#include "sketch_write_manager.h"

typedef void insert_ret_t;
typedef void flush_ret_t;

/*
 * Quick and dirty buffer tree skeleton.
 * Metadata about buffers (buffer control blocks) will be stored in memory.
 * The root buffer will be stored in memory.
 * All other buffers will be primarily stored on disk.
 * The tree operates read-lazily, only reading (non-root) buffers into memory
 *    if they need to be flushed.
 * Flushing and flush-related work is handled by a dynamic thread pool.
 * A flush queue will be maintained, from which threads pick tasks.
 * DESIGN NOTE: Currently, a buffer cannot flush to another buffer that needs to
 * be flushed. This is to prevent starvation.
 */
class BufferTree {
private:
  // root directory of tree
  std::string dir;

  // minimum size of buffer. true buffer size is guaranteed to be between M
  // and 2M
  uint32_t M;

  // branching factor
  uint32_t B;

  // metadata control block(s)
  std::vector<BufferControlBlock> buffers;

  // TODO: DESIGN: evaluate whether this should be a priority queue or if a
  //  regular queue will suffice
  std::queue<BufferControlBlock*> flush_queue;

  // utility to handle batching and writing to sketches
  SketchWriteManager sketchWriteManager;

  /*
   * Flushes the buffer:
   * 0. Checks if the root buffer is busy. If it is, wait until not busy.
   *    Lock the root.
   * 1. If the buffer is not stored in memory, read into memory.
   * 2. If the root is a leaf buffer:
   *      a. Collate and write_updates().
   *      b. Unlock the root and RETURN.
   * 2. Run heavy-hitters algorithm.
   * 3. write_updates() to heavy hitters, leaving gaps in the buffer where
   *    the elements were.
   * 4. Find fixed pivots within buffer to split data on.
   * 5. Locks children that need to be written to.
   * 6. File-appends elements to children buffers. Updates storage pointers
   *    of children. If any children have over M elements, add them to the
   *    flush queue.
   * 7. Resets storage pointer of root buffer to 0 and free the auxiliary
   *    memory used to store the buffer.
   * 8. Unlock children and root (in that order, please) and RETURN.
   */
  flush_ret_t flush(BufferControlBlock& buffer);

public:
  /**
   * Generates a new homebrew buffer tree.
   * @param dir     file path of the data structure root directory, relative to
   *                the executing workspace.
   * @param size    the minimum length of one buffer, in updates. Should be
   *                larger than the branching factor. not guaranteed to be
   *                the true buffer size, which can be between size and 2*size.
   * @param b       branching factor.
   * @param levels  (optional) number of levels in the tree.
   */
  BufferTree(std::string dir, uint32_t size, uint32_t b, uint32_t levels);
  ~BufferTree();
  /**
   * Puts an update into the data structure.
   * @param upd the edge update.
   * @return nothing.
   */
  insert_ret_t insert(update_t upd);

  /**
   * Flushes the entire tree down to the leaves.
   * @return nothing.
   */
  flush_ret_t force_flush();
};



#endif //FASTBUFFERTREE_BUFFER_TREE_H
