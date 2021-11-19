#ifndef BUFFER_FLUSHER_H
#define BUFFER_FLUSHER_H

#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>
#include <thread>

class GutterTree;
class flush_struct;
class RootControlBlock;

// The struct that goes in the flush queue
// With these two pieces of information we
// know exactly which root buffer to flush
struct flush_queue_elm {
  RootControlBlock *rcb; // pointer to the root meta-data
  uint8_t which_buf;     // out of the two buffers which should we flush
};

class BufferFlusher {
public:
  static std::condition_variable flush_ready;
  static bool shutdown;
  static bool force_flush;
  static std::queue<flush_queue_elm> flush_queue;
  static std::mutex queue_lock;

  BufferFlusher(uint32_t id, GutterTree *gt);
  ~BufferFlusher();

  inline bool get_working() {return working;}

private:
  static void *start_flusher(void *obj) {
    ((BufferFlusher *)obj)->do_work();
    return 0;
  }

  // memory for flushing
  flush_struct *flush_data;

  std::thread thr;
  uint32_t id;
  GutterTree *gt;
  std::atomic<bool> working; // is this thread actively working on something

  void do_work();
};

#endif