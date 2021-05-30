#ifndef QUEUE_GUARD
#define QUEUE_GUARD

#include <condition_variable>
#include <mutex>
#include <utility>

struct queue_elm {
	bool dirty;    // is this queue element yet to be processed by sketching (if so do not overwrite)
	uint32_t size; // the size of this data element
	char *data;    // the data element which goes in the queue
};

/*
 * A circular queue of data elements.
 * Used in the bufferTree to place leaf data which is ready to be processed.
 * Has a finite size and will block operations which would either empty or
 * fill up the queue until they can proceed
 */

class CircularQueue {
public:
	CircularQueue(int num_elements, int size_of_elm);
	~CircularQueue();

	// add a data element to the queue
	void push(char *elm, int size);              
	
	// get data from the queue for processing
	bool peek(bool noBlock, std::pair<int, queue_elm> &ret);
	
	// mark the queue element i as ready to be overwritten.
	// Call pop after processing the data from peek
	void pop(int i);                             

	std::condition_variable cirq_full;
	std::mutex write_lock;

	std::condition_variable cirq_empty;
	std::mutex read_lock;
private:
	int len;      // maximum number of data elements to be stored in the queue
	int elm_size; // size of an individual element in bytes

	int head;     // where to push (starts at 0, write pointer)
	int tail;     // where to peek (starts at 0, read pointer)
	
	queue_elm *queue_array; // array of char * which holds all the data

	inline int incr(int p) {return (p + 1) % len;}
	inline bool full()     {return queue_array[head].dirty;} // if the next data item is dirty then full
	inline bool empty()    {return (head == tail && !full());}
};
#endif
