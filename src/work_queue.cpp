#include "../include/work_queue.h"
#include "../include/types.h"

#include <string.h>
#include <chrono>
#include <cassert>

WorkQueue::WorkQueue(int num_elements, int size_of_elm): 
  len(num_elements), elm_size(size_of_elm) {
  head = 0;
  tail = 0;
  no_block = false;

  // malloc the memory for the work queue
  queue_array = (queue_elm *) malloc(sizeof(queue_elm) * len);
  data_array = (char *) malloc(elm_size * len * sizeof(char));
  for (int i = 0; i < len; i++) {
    queue_array[i].data    = data_array + (elm_size * i);
    queue_array[i].dirty   = false;
    queue_array[i].touched = false;
    queue_array[i].size    = 0;
  }

  printf("WQ: created work queue with %i elements each of size %i\n", len, elm_size);
}

WorkQueue::~WorkQueue() {
  // free the queue
  free(data_array);
  free(queue_array);
}

void WorkQueue::push(char *elm, int size) {
  if(size > elm_size) {
    printf("WQ: write of size %i bytes greater than max of %i\n", size, elm_size);
    throw WriteTooBig();
  }

  while(true) {
    std::unique_lock<std::mutex> lk(write_lock);
    // printf("WQ: push: wait on not-full. full() = %s\n", (full())? "true" : "false");
    wq_full.wait_for(lk, std::chrono::milliseconds(500), [this]{return !full();});
    if(!full()) {
      memcpy(queue_array[head].data, elm, size);
      queue_array[head].size = size;
      queue_array[head].dirty = true;
      head = incr(head);
      lk.unlock();
      wq_empty.notify_one();
      break;
    }
    lk.unlock();
  }
}

bool WorkQueue::peek(std::pair<int, queue_ret_t> &ret) {
  do {
    std::unique_lock<std::mutex> lk(read_lock);
    wq_empty.wait_for(lk, std::chrono::milliseconds(500), [this]{return (!empty() || no_block);});
    if(!empty()) {
      int temp = tail;
      queue_array[tail].touched = true;
      tail = incr(tail);
      lk.unlock();

      ret.first = temp;
      ret.second = {queue_array[temp].size, queue_array[temp].data};
      return true;
    }
    lk.unlock();
  }while(!no_block);
  return false;
}

bool WorkQueue::peek_batch(std::vector<std::pair<int, queue_ret_t>> &ret, int batch_size) {
  // some asserts for sanity
  assert(ret.capacity() >= batch_size);
  assert(ret.size() == 0);
  assert(batch_size >= len);

  std::unique_lock<std::mutex> lk(read_lock);
  do {
    wq_empty.wait_for(lk, std::chrono::milliseconds(500), [this, batch_size]{return (size() >= batch_size || no_block);});
    // give batch_size queue elements to this thread
    int i = 0;
    while (!empty() && i < batch_size) {
      queue_array[tail].touched = true;
      ret[i] = {tail, {queue_array[tail].size, queue_array[tail].data}};
      tail = incr(tail);
      ++i;
    }
    lk.unlock();
  } while(!no_block);
  if (ret.size() > 0) return true;
  return false;
}

void WorkQueue::pop(int i) {
  queue_array[i].dirty   = false; // this data has been processed and this slot may now be overwritten
  queue_array[i].touched = false; // may read this slot
  wq_full.notify_one();
}

void WorkQueue::print() {
  printf("WQ: head=%i, tail=%i, is_full=%s, is_empty=%s\n", 
    head.load(), tail.load(), full()? "true" : "false", empty()? "true" : "false");
}
