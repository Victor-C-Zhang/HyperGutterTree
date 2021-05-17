//
// Created by victor on 5/17/21.
//

#ifndef FASTBUFFERTREE_GRAPHWORKERMOCK_H
#define FASTBUFFERTREE_GRAPHWORKERMOCK_H

#include "../../include/buffer_tree.h"

class GraphWorker {
  BufferTree* bt;
public:
  std::atomic_bool done_proc;

  GraphWorker(BufferTree *bt);
  void listen();
  void alert_done();
};

#endif //FASTBUFFERTREE_GRAPHWORKERMOCK_H
