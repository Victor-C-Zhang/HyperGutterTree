#ifndef FASTBUFFERTREE_BUFFER_TREE_H
#define FASTBUFFERTREE_BUFFER_TREE_H

#include <cstdint>
#include <string>
#include <vector>
#include <queue>
#include <mutex>
#include "update.h"
#include "buffer_control_block.h"

typedef void insert_ret_t;
typedef void flush_ret_t;
typedef std::pair<Node, std::vector<std::pair<Node, bool>>> data_ret_t;

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

  // number of nodes in the graph
  Node N;

  // metadata control block(s)
  // level 1 blocks take indices 0->(B-1). So on and so forth from there
  std::vector<BufferControlBlock*> buffers;

  // buffers which we will use when performing flushes
  char **flush_buffers;

  // check root first then level 1/2 queues and finally a queue of anything else
  std::queue<BufferControlBlock*> flush_queue1;     // level 1
  std::queue<BufferControlBlock*> flush_queue2;     // level 2
  std::queue<BufferControlBlock*> flush_queue_wild; // level > 2

  // utility to handle batching and writing to sketches
  // SketchWriteManager sketchWriteManager;

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

  /*
   * root node and functions for handling it
   */
  char *root_node;
  flush_ret_t flush_root();
  uint root_position;
  std::mutex root_lock;

  /*
   * File descriptor of backing file for storage
   */
  int backing_store;

public:
  /**
   * Generates a new homebrew buffer tree.
   * @param dir     file path of the data structure root directory, relative to
   *                the executing workspace.
   * @param size    the minimum length of one buffer, in updates. Should be
   *                larger than the branching factor. not guaranteed to be
   *                the true buffer size, which can be between size and 2*size.
   * @param b       branching factor.
   * @param nodes   number of nodes in the graph
   */
  BufferTree(std::string dir, uint32_t size, uint32_t b, Node nodes);
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

  /*
   * Function to convert an update_t to a char array
   * @param dst the memory location to put the serialized data
   * @param src the edge update
   * @return nothing
   */
  void serialize_update(char *dst, update_t src);

  /*
   * Function to covert char array to update_t
   * @param src the memory location to load serialized data from
   * @param dst the edge update to put stuff into
   * @return nothing
   */
  void deserialize_update(char *src, update_t dst);
 
  /*
   * Copy the serialized data from one location to another
   * @param src data to copy from
   * @param dst data to copy to
   * @return nothing
   */
  static void copy_serial(char *src, char *dst);

  /*
   * Load a key from serialized data
   * @param location data to pull from
   * @return the key pulled from the data
   */
  static Node load_key(char *location);

  data_ret_t get_data(uint32_t tag, Node key);
  /*
   * Size of a page (can vary from machine to machine but usually 4kb)
   */
  uint page_size;
  static const uint serial_update_size = sizeof(Node) + sizeof(Node) + sizeof(bool);
};

class BufferFullError : public std::exception {
private:
  int id;
public:
  BufferFullError(int id) : id(id) {};
  virtual const char* what() const throw() {
    if (id == -1)
      return "Root buffer is full";
    else
      return "Non-Root buffer is full";
  }
};


#endif //FASTBUFFERTREE_BUFFER_TREE_H
