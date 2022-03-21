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
  uint32_t buffer_size; // size of a buffer (including metadata)
  std::vector<Gutter> gutters; // array dump of numbers for performance:
                                               // DO NOT try to access directly!

  /**
   * Flushes the corresponding buffer to the queue.
   * @param gutter      a pointer to the head of the gutter to flush.
   * @param num_bytes   the number of bytes to flush.
   */
  void flush(node_id_t node_idx, std::vector<node_id_t> &buffer);
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
   */
  StandAloneGutters(node_id_t nodes, int workers);

  /**
   * Puts an update into the data structure.
   * @param upd the edge update.
   * @return nothing.
   */
  insert_ret_t insert(const update_t &upd);

  /**
   * Flushes all pending buffers.
   * @return nothing.
   */
  flush_ret_t force_flush();
};
