#include "../include/buffer_flusher.h"
#include "../include/gutter_tree.h"

bool BufferFlusher::shutdown    = false;
bool BufferFlusher::force_flush = false;
std::condition_variable BufferFlusher::flush_ready;
std::queue<buffer_id_t> BufferFlusher::flush_queue;
std::mutex BufferFlusher::queue_lock;

BufferFlusher::BufferFlusher(uint32_t id, GutterTree *gt) 
 : id(id), gt(gt) {
  shutdown    = false;
  force_flush = false;
  working     = false;

  flush_data = new flush_struct(gt);

  thr = std::thread(BufferFlusher::start_flusher, this);
}

BufferFlusher::~BufferFlusher() {
  shutdown = true;
  flush_ready.notify_all();
  thr.join();

  delete flush_data;
}

void BufferFlusher::do_work() {
  printf("Starting BufferFlusher thread %i\n", id);
  while(true) {
    std::unique_lock<std::mutex> queue_unique(queue_lock);
    flush_ready.wait(queue_unique, [this]{return (!flush_queue.empty() || shutdown);});
    if (!flush_queue.empty()) {
      working = true;
      buffer_id_t bcb_id = flush_queue.front();
      flush_queue.pop();
      // printf("BufferFlusher id=%i awoken processing buffer %u\n", id, bcb_id);
      queue_unique.unlock();
      if (bcb_id >= gt->buffers.size()) {
        fprintf(stderr, "ERROR: the id given in the flush_queue is too large! %u\n", bcb_id);
        exit(EXIT_FAILURE);
      }

      if (force_flush) {
        // flush the entire subtree of all updates
        BufferControlBlock *bcb = gt->buffers[bcb_id];
        gt->flush_subtree(*flush_data, bcb);
      }
      else {
        BufferControlBlock *bcb = gt->buffers[bcb_id];
        gt->flush_control_block(*flush_data, bcb); // flush and unlock the bcb
      }
      // printf("BufferFlusher id=%i done\n", id);
      working = false;
      BufferControlBlock::buffer_ready.notify_one();
    } else if (shutdown) {
      // printf("BufferFlusher %i shutting down\n", id);
      queue_unique.unlock();
      return;
    } else {
      // printf("spurious wake-up\n");
      queue_unique.unlock(); // spurious wake-up. Go back to sleep
    }
  }
}
