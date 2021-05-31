#ifndef QUEUE_GUARD
#define QUEUE_GUARD

#include <condition_variable>
#include <mutex>
#include <utility>

struct queue_elm {
	bool dirty;    // is this queue element yet to be processed by sketching (if so do not overwrite)
	uint32_t size; // the size of this data element
	char *data;    // a pointer to the data
};

/*
 * A circular queue of data elements.
 * Used in the bufferTree to place leaf data which is ready to be processed.
 * Has a finite size and will block operations which do not have what they
 * need need (either empty or full for peek and push respectively)
 */

class CircularQueue {
public:
	CircularQueue(int num_elements, int size_of_elm);
	~CircularQueue();

	// add a data element to the queue
	void push(char *elm, int size);              
	
	// get data from the queue for processing
	bool peek(std::pair<int, queue_elm> &ret);
	
	// mark the queue element i as ready to be overwritten.
	// Call pop after processing the data from peek
	void pop(int i);                             

	std::condition_variable cirq_full;
	std::mutex write_lock;

	std::condition_variable cirq_empty;
	std::mutex read_lock;

	// should CircularQueue peeks wait until they can succeed(false)
	// or return false on failure (true)
	bool no_block;
private:
	int len;      // maximum number of data elements to be stored in the queue
	int elm_size; // size of an individual element in bytes

	int head;     // where to push (starts at 0, write pointer)
	int tail;     // where to peek (starts at 0, read pointer)
	
	queue_elm *queue_array; // array queue_elm metadata
	char *data_array;
	inline int incr(int p) {return (p + 1) % len;}
	inline bool full()     {return queue_array[head].dirty;} // if the next data item is dirty then full
	inline bool empty()    {return (head == tail && !full());}
};
#endif
