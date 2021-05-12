//
// Created by victor on 5/11/21.
//

#include <omp.h>
#include "../include/flush_worker.h"

FlushWorker::FlushWorker(BufferTree* bufferTree) {
  bt = bufferTree;
  std::atomic_init(&can_exit, false);
}

void FlushWorker::do_flush() {
  std::unique_lock<std::mutex> pq_lock(pq_mutex);
  pq_lock.lock();
  BufferControlBlock* bcb = *flush_queue.begin();
  flush_queue.erase(flush_queue.begin());
  pq_lock.unlock();
  bt->flush_control_block(bcb);
}

void FlushWorker::listen() {
  while (!std::atomic_load(&can_exit)) {
    std::unique_lock<std::mutex> pq_lock(pq_mutex);
    pq_cv.wait(pq_lock, [this]{return !flush_queue.empty();});
    pq_lock.unlock();
#pragma omp task
    do_flush();
  }
}

void FlushWorker::request_flush(BufferControlBlock *bcb, int priority) {
  std::unique_lock<std::mutex> pq_lock(pq_mutex);
  auto buf_ptr = flush_queue.find(bcb);
  bool should_notify = true;
  if (buf_ptr != flush_queue.end() && (*buf_ptr)->priority == priority) {
    pq_lock.unlock();
    return;
  }
  if (buf_ptr != flush_queue.end()){ // replace
    flush_queue.erase(buf_ptr);
    should_notify = false;
  }
  bcb->priority = priority;
  bcb->timestamp = omp_get_wtime();
  flush_queue.insert(bcb);
  if (should_notify) pq_cv.notify_one();
  pq_lock.unlock();
}
