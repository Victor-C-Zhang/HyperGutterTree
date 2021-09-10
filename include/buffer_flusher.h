#ifndef BUFFER_FLUSHER_H
#define BUFFER_FLUSHER_H

#include <mutex>
#include <condition_variable>
#include <queue>
#include <pthread.h>

class BufferTree;
class flush_struct;

class BufferFlusher {
public:
	static std::condition_variable flush_ready;
	static bool shutdown;
	static bool force_flush;
	static std::queue<uint32_t> flush_queue;
	static std::mutex queue_lock;

	// memory for flushing
  	flush_struct *flush_data;

	BufferFlusher(uint32_t id, BufferTree *bt);
	~BufferFlusher();

	inline bool get_working() {return working;}

private:
	static void *start_flusher(void *obj) {
		((BufferFlusher *)obj)->do_work();
		return 0;
	}

	uint32_t id;
	BufferTree *bt;
	pthread_t thr;
	bool working = false; // is this thread actively working on something

	void do_work();
};

#endif