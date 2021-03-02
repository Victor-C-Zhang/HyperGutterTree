//
// Created by victor on 3/2/21.
//

#ifndef FASTBUFFERTREE_SKETCH_WRITE_MANAGER_H
#define FASTBUFFERTREE_SKETCH_WRITE_MANAGER_H

#include <queue>
#include <cstdint>
#include <mutex>
#include "update.h"
#include "sketch.h"

typedef uint64_t Node;
/**
 * Class to schedule and manage writes to sketches.
 */
class SketchWriteManager {
private:
  std::mutex* sketch_locks;
  std::queue<Sketch*> write_queue;
public:
  SketchWriteManager();
  ~SketchWriteManager();

  // TODO: how do we do this without introducing too much inefficiency from
  //  reading/writing to disk between updates to the same sketch?
  /**
   * Synchronously writes batched updates to the desired sketch.
   * @param sketch_num  the sketch to update.
   * @param updates     an iterable batch of updates.
   */
  void write_updates(Node sketch_num, std::vector<update_t>& updates);
};


#endif //FASTBUFFERTREE_SKETCH_WRITE_MANAGER_H
