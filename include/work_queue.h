#pragma once
#include <condition_variable>
#include <mutex>
#include <utility>
#include <atomic>

struct queue_elm {
  std::atomic<bool> dirty;    // is the queue element unprocessed (if so do not overwrite)
  std::atomic<bool> touched;  // have we peeked at this item (if so do not peek it again)
  std::atomic<uint32_t> size; // the size of this data element (in bytes)
  char *data;                 // a pointer to the data
};

typedef std::pair<uint32_t, char *> queue_ret_t;

/*
 * The Work Queue: A circular queue of data elements.
 * Used by the buffering systems to place data which is ready to be processed.
 * Has a finite size and will block operations which do not have what they
 * need need (either empty or full for peek and push respectively)
 */

class WorkQueue {
public:
  WorkQueue(int num_elements, int size_of_elm);
  ~WorkQueue();

  /* 
   * Add a data element to the queue
   * @param   elm the data to be placed into the queue
   * @param   size the number of bytes in elm
   */
  void push(char *elm, int size);
  
  /* 
   * Get data from the queue for processing
   * @param   ret where the data from the work queue should be placed
   * @return  true if we were able to get good data, false otherwise
   */
  bool peek(std::pair<int, queue_ret_t> &ret);
  
  /* 
   * Mark a queue element as ready to be overwritten.
   * Call pop after processing the data from peek.
   * @param   i is the position of the queue_elm which should be popped
   */
  void pop(int i);

  std::condition_variable wq_full;
  std::condition_variable wq_empty;
  std::mutex read_lock;
  std::mutex write_lock;

  // should WorkQueue peeks wait until they can succeed(false)
  // or return false on failure (true)
  std::atomic<bool> no_block;

  /*
   * Function which prints the work queue
   * Used for debugging
   */
  void print();

  // functions for checking if the queue is empty or full
  inline bool full()     {return queue_array[head].dirty;} // if the next data item is dirty then full
  // if place to read from is clean and has not been peeked already then queue is empty
  inline bool empty()    {return !queue_array[tail].dirty || queue_array[tail].touched;}
private:
  int32_t len;      // maximum number of data elements to be stored in the queue
  int32_t elm_size; // size of an individual element in bytes

  std::atomic<int> head;     // where to push (starts at 0, write pointer)
  std::atomic<int> tail;     // where to peek (starts at 0, read pointer)
  
  queue_elm *queue_array; // array queue_elm metadata
  char *data_array;       // the actual data

  // increment the head or tail pointer
  inline int incr(int p) {return (p + 1) % len;}
};

class WriteTooBig : public std::exception {
public:
  virtual const char * what() const throw() {
    return "Write to work queue is too big";
  }
};
