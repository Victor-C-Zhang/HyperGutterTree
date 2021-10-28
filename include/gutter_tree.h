#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <queue>
#include <mutex>
#include <math.h>
#include "types.h"
#include "buffer_control_block.h"
#include "work_queue.h"

typedef void insert_ret_t;
typedef void flush_ret_t;
typedef std::pair<Node, std::vector<Node>> data_ret_t;

/*
 * Structure of the GutterTree
 */
class GutterTree {
private:
  // root directory of tree
  std::string dir;

  // metadata control block(s)
  // level 1 blocks take indices 0->(B-1). So on and so forth from there
  std::vector<BufferControlBlock*> buffers;

  // buffers which we will use when performing flushes
  // we maintain these for every level of the tree 
  // to handle recursive flushing.
  // TODO: a read_buffer per level is somewhat expensive
  // we could just read back from disk instead (more IOs though)
  char ***flush_buffers;
  char ***flush_positions; // pointers into the flush_buffers
  char **read_buffers;

  /*
   * root node and functions for handling it
   */
  char *root_node;
  flush_ret_t flush_root();
  uint32_t root_position;
  std::mutex root_lock;

  /**
  * Use buffering.conf configuration file to determine parameters of the GutterTree
  * Sets the following variables
  * Buffer_Size  :   The size of the root buffer
  * Fanout       :   The maximum number of children per internal node
  * Queue_Factor :   The number of queue slots per worker removing data from the queue
  * Page_Factor  :   Multiply system page size by this number to get our write granularity
  */
  void configure_tree();

  /*
   * function which actually carries out the flush. Designed to be
   * called either upon the root or upon a buffer at any level of the tree
   * @param data        the data to flush
   * @param size        the size of the data in bytes
   * @param begin       the smallest id of the node's children
   * @param min_key     the smalleset key this node is responsible for
   * @param max_key     the largest key this node is responsible for
   * @param options     the number of children this node has
   * @param level       the level of the buffer being flushed (0 is root)
   * @returns nothing
   */
  flush_ret_t do_flush(char *data, uint32_t size, uint32_t begin, 
    node_id_t min_key, node_id_t max_key, uint16_t options, uint8_t level);

  // Work queue in which we place leaves that fill up
  WorkQueue *wq;

  flush_ret_t flush_control_block(BufferControlBlock *bcb);
  flush_ret_t flush_internal_node(BufferControlBlock *bcb);
  flush_ret_t flush_leaf_node(BufferControlBlock *bcb);

  /*
   * Variables which track universal information about the buffer tree which
   * we would like to be accesible to all the bufferControlBlocks
   */
  uint32_t page_size;    // write granularity
  uint8_t  max_level;    // max depth of the tree
  uint32_t buffer_size;  // size of an internal node buffer
  uint32_t fanout;       // maximum number of children per node
  uint32_t num_nodes;    // number of unique ids to buffer
  uint64_t backing_EOF;  // file to write tree to
  uint64_t leaf_size;    // size of a leaf buffer
  uint32_t queue_factor; // number of elements in queue is this factor * num_workers

  //File descriptor of backing file for storage
  int backing_store;
  // a chunk of memory we reserve to cache the first level of the buffer tree
  char *cache;

public:
  /**
   * Generates a new homebrew buffer tree.
   * @param dir     file path of the data structure root directory, relative to
   *                the executing workspace.
   * @param nodes   number of nodes in the graph
   * @param workers the number of workers which will be using this buffer tree (defaults to 1)
   * @param reset   should truncate the file storage upon opening
   */
  GutterTree(std::string dir, node_id_t nodes, int workers, bool reset);
  ~GutterTree();

  /**
   * Puts an update into the data structure.
   * @param upd the edge update.
   * @return nothing.
   */
  insert_ret_t insert(update_t upd);

  /*
   * Ask the buffer tree for data and sleep if necessary until it is available.
   * @param data       this is where to the key and vector of updates associated with it
   * @return           true true if got valid data, false if unable to get data.
   */
  bool get_data(data_ret_t &data);

  /**
   * Flushes the entire tree down to the leaves.
   * @return nothing.
   */
  flush_ret_t force_flush();

  /*
   * Notifies all threads waiting on condition variables that 
   * they should check their wait condition again
   * Useful when switching from blocking to non-blocking calls
   * to the work queue
   * For example: we set this to true when shutting down workers
   * @param    block is true if we should turn on non-blocking operations
   *           and false if we should turn them off
   * @return   nothing
   */
  void set_non_block(bool block);

  /*
   * Function to convert an update_t to a char array
   * @param   dst the memory location to put the serialized data
   * @param   src the edge update
   * @return  nothing
   */
  void serialize_update(char *dst, update_t src);

  /*
   * Function to covert char array to update_t
   * @param src the memory location to load serialized data from
   * @param dst the edge update to put stuff into
   * @return nothing
   */
  update_t deserialize_update(char *src);
 
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
  static node_id_t load_key(char *location);

  /*
   * Creates the entire buffer tree to produce a tree of depth log_B(N)
   */
  void setup_tree();

  /*
   * A bunch of functions for accessing buffer tree variables
   */
  inline uint32_t get_page_size()    { return page_size; };
  inline uint8_t  get_max_level()    { return max_level; };
  inline uint32_t get_buffer_size()  { return buffer_size; };
  inline uint64_t get_leaf_size()    { return leaf_size; };
  inline uint32_t get_fanout()       { return fanout; };
  inline uint32_t get_num_nodes()    { return num_nodes; };
  inline uint64_t get_file_size()    { return backing_EOF; };
  inline uint32_t get_queue_factor() { return queue_factor; };

  inline int get_fd()       { return backing_store; };
  inline char * get_cache() { return cache; };

  static const uint32_t serial_update_size = sizeof(node_id_t) + sizeof(node_id_t); // size in bytes of an update
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

class KeyIncorrectError : public std::exception {
public:
  virtual const char * what() const throw() {
    return "The key was not correct for the associated buffer";
  }
};
