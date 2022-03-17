#include "../include/work_queue.h"
#include "../include/types.h"

#include <string.h>
#include <chrono>
#include <thread>
#include <iostream>

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

  // Implement busy waiting until there is a slot to place the update in and we have the write lock
  while (true) {
    do {
      size_t i = 0; // how many times have we waited on full
      while(full()) {
        if(i > 1000000) std::this_thread::sleep_for(std::chrono::milliseconds(10));
        else ++i;
      }
    } while(!write_lock.try_lock()); // could potentially switch to using CAS on head for lock-less

    if (!full()) {
      //printf("WQ: PUSH: have lock and pushing\n");
      // perform the insertion
      memcpy(queue_array[head].data, elm, size);
      queue_array[head].size = size;
      queue_array[head].dirty = true;
      head = incr(head);
      write_lock.unlock();
      break;
    }
    write_lock.unlock(); // failed to write, yield lock
  }
}

bool WorkQueue::peek(std::pair<int, queue_ret_t> &ret) {
  // Ensure that there is data to read and that we have the lock
  // if many threads attempt to get data only one will exit the loop with the lock at a time
  // if there is data to get, then the threads will spin-lock if there is not
  // data to get then they will sleep in increments that double
  while (true) {
    do {
      size_t i = 0; // how many times have we waited on full
      while (empty() && !no_block) { 
        if(i > 1000000) std::this_thread::sleep_for(std::chrono::milliseconds(10));
        else ++i;
      }
    } while (!read_lock.try_lock()); // could potentially switch to using CAS on tail for lock-less
    
    // check if the guttering system is empty (return false if so)
    if (no_block && empty()) {
      read_lock.unlock();
      return false;
    }

    // printf("WQ: PEEK, have lock and reading\n");
    if (!empty()) {
      // actually read the data
      int temp = tail.load();
      queue_array[tail].touched = true;
      tail = incr(tail);
      read_lock.unlock();

      ret.first = temp;
      ret.second = {queue_array[temp].size, queue_array[temp].data};
      return true;
    }
    read_lock.unlock(); // failed to read, yield lock
  }
}

void WorkQueue::pop(int i) {
  queue_array[i].dirty   = false; // this data has been processed and this slot may now be overwritten
  queue_array[i].touched = false; // may read this slot
}

void WorkQueue::print() {
  printf("WQ: head=%i, tail=%i, is_full=%s, is_empty=%s\n", 
    head.load(), tail.load(), full()? "true" : "false", empty()? "true" : "false");
}
