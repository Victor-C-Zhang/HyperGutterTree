//
// Created by victor on 5/17/21.
//

#include "GraphWorkerMock.h"

GraphWorker::GraphWorker(BufferTree *bt) : bt(bt) {
  std::atomic_init(&done_proc, false);
}

void GraphWorker::listen() {
  while (!std::atomic_load(&done_proc)) {
    std::unique_lock<std::mutex> queue_lock(bt->queue_lock);
    bt->queue_cond.wait(queue_lock, [this]{return !bt->work_queue.empty();});
    auto unused = bt->get_data(bt->work_queue.front());
    bt->work_queue.pop();
    queue_lock.unlock();
  }
}

void GraphWorker::alert_done() {
  std::atomic_store(&done_proc, true);
}