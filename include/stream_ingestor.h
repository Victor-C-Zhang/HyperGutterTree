//
// Created by victor on 5/13/21.
//

#ifndef FASTBUFFERTREE_STREAM_INGESTOR_H
#define FASTBUFFERTREE_STREAM_INGESTOR_H

#include "buffer_control_block.h"

typedef void flush_ret_t;

/**
 * Interface to solve co-dependency between FlushWorker and BufferTree.
 */
class StreamIngestor {
public:
  virtual flush_ret_t flush_control_block(BufferControlBlock* bcb) = 0;
};

#endif //FASTBUFFERTREE_STREAM_INGESTOR_H
