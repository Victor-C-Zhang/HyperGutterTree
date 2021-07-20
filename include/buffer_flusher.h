#ifndef BUFFER_FLUSHER_H
#define BUFFER_FLUSHER_H

#include <mutex>
#include <condition_variable>
#include <queue>
#include <pthread.h>

class BufferTree;

class BufferFlusher {
public:
	static std::condition_variable flush_ready;
	static bool shutdown;
	static std::queue<uint32_t> flush_queue;
	static std::mutex queue_lock;

	BufferFlusher(uint32_t id, BufferTree *bt);
	~BufferFlusher();

private:
	static void *start_flusher(void *obj) {
		((BufferFlusher *)obj)->do_work();
		return 0;
	}

	uint32_t id;
	BufferTree *bt;
	pthread_t thr;

	void do_work();
};

#endif