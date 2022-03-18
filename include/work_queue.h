#pragma once
#include <condition_variable>
#include <mutex>
#include <utility>
#include <atomic>
#include <vector>

struct WQLinkedList {
  // LL next pointer
  WQLinkedList *next;

  uint32_t size; // number of elements in data vector
  vector<node_id_t> &data; // vector of data for processing
}

class WorkQueue {
public:
  /*
   * Construct a work queue
   * @param num_elements  the number of queue slots
   * @param max_elm_size  the maximum size of a data element
   */
  WorkQueue(int num_elements, int max_elm_size);
  ~WorkQueue();

  /* 
   * Add a data element to the queue
   * @param   elm the data to be placed into the queue
   * @param   size the number of bytes in elm
   */
  void push(char *elm, int size);

  /* 
   * Get data from the queue for processing
   * @param valid   did we successfully find data
   * @return  true if we were able to get good data, false otherwise
   */
  std::vector<node_id_t> &data peek(bool &valid);

  /*
   * Wait until the work queue has enough items in it to satisfy the request and then
   * return batch_size number of work units
   */
  bool peek_batch(std::vector<std::pair<int, queue_ret_t>> &ret, int batch_size);
  
  /* 
   * After processing data taken from the work queue call this function
   * to mark the node as ready to be overwritten
   * @param node   the LL node that we have finished processing
   */
  void peek_callback(std::vector<node_id_t> &data);

  // should WorkQueue peeks wait until they can succeed(false)
  // or return false on failure (true)
  std::atomic<bool> no_block;

  /*
   * Function which prints the work queue
   * Used for debugging
   */
  void print();

  // functions for checking if the queue is empty or full
  inline bool full()    {return producer_list == nullptr;} // if producer queue empty, wq full
  inline bool empty()   {return consumer_list == nullptr;} // if consumer queue empty, wq empty
  inline int get_size() {return q_size;}

private:
  WQLinkedList *producer_list = nullptr; // list of nodes ready to be written to
  WQLinkedList *consumer_list = nullptr; // list of nodes with data for reading

  const int max_elm_size;
  std::atomic<int> q_size;

  // locks and condition variables for producer list
  std::condition_variable produce_condition;
  std::mutex produce_list_lock;

  // locks and condition variables for consumer list
  std::condition_variable consume_condition;
  std::mutex consume_list_lock;
}

class WriteTooBig : public std::exception {
private:
  int max_size;
  int elm_size;

public:
  WriteTooBig(elm_size, max_size) : elm_size(elm_size), max_size(max_size) {}

  virtual const char *what() const throw() {
    return "WQ: Write is too big " + std::to_string(elm_size) + " > " + std::to_string(max_size);
  }
}
