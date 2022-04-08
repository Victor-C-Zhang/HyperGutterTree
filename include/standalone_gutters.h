#pragma once
#include "work_queue.h"
#include "types.h"
#include "guttering_system.h"

/**
 * In-memory wrapper to offer the same interface as a buffer tree.
 */
class StandAloneGutters : public GutteringSystem {
private:
  struct Gutter
  { 
    std::mutex mux;
    std::vector<node_id_t> buffer;
  };
  static constexpr uint8_t local_buf_size = 8;
  struct LocalGutter
  {
		uint8_t count = 0;
    node_id_t buffer[local_buf_size];
  };
  uint32_t buffer_size; // size of a buffer (including metadata)
  std::vector<Gutter> gutters; // array dump of numbers for performance:
                                               // DO NOT try to access directly!
  const uint32_t inserters;
  std::vector<std::vector<LocalGutter>> local_buffers; // array dump of numbers for performance:
  
  
  /**
   * Puts an update into the data structure from the local buffer. Must hold both buffer locks
   * @param upd the edge update.
   * @return nothing.
   */
  insert_ret_t insert_batch(int which, node_id_t gutterid);

  /**
   * Use buffering.conf configuration file to determine parameters of the StandAloneGutters
   * Sets the following variables
   * Queue_Factor :   The number of queue slots per worker removing data from the queue
   * Size_Factor  :   Decrease the amount of bytes used per node by this multiplicative factor
   */
public:
  /**
   * Constructs a new .
   * @param size        the total length of a buffer, in updates.
   * @param nodes       number of nodes in the graph.
   * @param workers     the number of workers which will be removing batches
   * @param inserters     the number of inserter buffers
   */
  StandAloneGutters(node_id_t nodes, uint32_t workers, uint32_t inserters=1);

  /**
   * Puts an update into the data structure.
   * @param upd the edge update.
   * @return nothing.
   */
  insert_ret_t insert(const update_t &upd, int which);
  // pure virtual functions don't like default params
  insert_ret_t insert(const update_t &upd);

  
  /**
   * Flushes all pending buffers.
   * @return nothing.
   */
  flush_ret_t force_flush();
};
