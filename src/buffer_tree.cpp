#include "../include/buffer_tree.h"
#include "../include/buffer_flusher.h"

#include <utility>
#include <unistd.h> //sysconf
#include <string.h> //memcpy
#include <fcntl.h>  //posix_fallocate
#include <errno.h>

/*
 * Static "global" BufferTree variables
 */
uint32_t BufferTree::page_size;
uint8_t  BufferTree::max_level;
uint32_t BufferTree::buffer_size;
uint32_t BufferTree::branch_factor;
uint64_t BufferTree::backing_EOF;
uint64_t BufferTree::leaf_size;
int      BufferTree::backing_store;
char *   BufferTree::cache;

/*
 * Constructor
 * Sets up the buffer tree given the storage directory, buffer size, number of children
 * and the number of nodes we will insert(N)
 * We assume that node indices begin at 0 and increase to N-1
 */
BufferTree::BufferTree(std::string dir, uint32_t size, uint32_t b, node_id_t
nodes, int nf, int workers, int queue_factor, bool reset=false) : dir(dir), M(size), B(b), N(nodes), num_flushers(nf) {
	page_size = 5 * sysconf(_SC_PAGE_SIZE); // works on POSIX systems (alternative is boost)
	page_size = (page_size % serial_update_size == 0)? page_size : page_size + serial_update_size - page_size % serial_update_size;

	int file_flags = O_RDWR | O_CREAT; // direct memory O_DIRECT may or may not be good
	if (reset) {
		file_flags |= O_TRUNC;
	}

	if (M < page_size) {
		printf("WARNING: requested buffer size smaller than page_size. Set to page_size.\n");
		M = page_size;
	}
	
	// setup static variables
	max_level       = ceil(log(N) / log(B));
	buffer_size     = M; // probably figure out a better solution than this
	branch_factor   = B; // this too (best would probably be to just use these static variables)
	backing_EOF     = 0;
	leaf_size       = floor(24 * pow(log2(N), 3)); // size of leaf proportional to size of sketch
	leaf_size       = (leaf_size % serial_update_size == 0)? leaf_size : leaf_size + serial_update_size - leaf_size % serial_update_size;

	// create memory
	flush_data = new flush_struct(); // must be done after setting up statics
	cache = (char *) malloc(B * ((uint64_t)buffer_size + page_size));

	// open the file which will be our backing store for the non-root nodes
	// create it if it does not already exist
	std::string file_name = dir + "buffer_tree_v0.2.data";
	printf("opening file %s\n", file_name.c_str());
	backing_store = open(file_name.c_str(), file_flags, S_IRUSR | S_IWUSR);
	if (backing_store == -1) {
		fprintf(stderr, "Failed to open backing storage file! error=%s\n", strerror(errno));
		exit(1);
	}

	setup_tree(); // setup the buffer tree

	// create the circular queue in which we will place ripe fruit (full leaves)
	cq = new CircularQueue(queue_factor*workers, leaf_size + page_size);

	// start the buffer flushers
	// TODO: make the number of buffer flushers a parameter
	flushers = (BufferFlusher **) malloc(sizeof(BufferFlusher *) * num_flushers);
	for (int i = 0; i < num_flushers; i++) {
		flushers[i] = new BufferFlusher(i, this);
	}
	// use pwrite/read so that the IOs are thread safe and all threads can share a single file descriptor

	printf("Successfully created buffer tree\n");
}

BufferTree::~BufferTree() {
	printf("Closing BufferTree\n");
	cq->get_stats();
	// force_flush(); // flush everything to leaves (could just flush to files in higher levels)

	// free malloc'd memory
	delete flush_data;
	free(cache);
	for(uint32_t i = 0; i < buffers.size(); i++) {
		if (buffers[i] != nullptr)
			delete buffers[i];
	}
	delete cq;

	free(flushers);

	close(backing_store);
}

void print_tree(std::vector<BufferControlBlock *>bcb_list) {
	for(uint32_t i = 0; i < bcb_list.size(); i++) {
		if (bcb_list[i] != nullptr) 
			bcb_list[i]->print();
	}
}

// TODO: clean up this function
void BufferTree::setup_tree() {
	printf("Creating a tree of depth %i\n", max_level);
	File_Pointer size = 0;

	// create the BufferControlBlocks
	for (uint32_t l = 0; l < max_level; l++) { // loop through all levels
		if(l == 1) size = 0; // reset the size because the first level is held in cache

		uint32_t level_size    = pow(B, l+1); // number of blocks in this level
		uint32_t plevel_size   = pow(B, l);
		uint32_t start         = buffers.size();
		node_id_t key          = 0;

		double parent_keys = N;
		uint32_t options       = B;
		bool skip          = false;
		uint32_t parent        = 0;
		File_Pointer index = 0;

		buffers.reserve(start + level_size);
		for (uint32_t i = 0; i < level_size; i++) { // loop through all blocks in the level
			// get the parent of this node if not level 1 and if we have a new parent
			if (l > 0 && (i-start) % B == 0) {
				parent      = start + i/B - plevel_size; // this logic should check out because only the last level is ever not full
				parent_keys = buffers[parent]->max_key - buffers[parent]->min_key + 1;
				key         = buffers[parent]->min_key;
				options     = B;
				skip        = (parent_keys == 1)? true : false; // if parent leaf then skip
			}
			if (skip || parent_keys == 0) {
				continue;
			}

			BufferControlBlock *bcb = new BufferControlBlock(start + index, size, l);
	 		bcb->min_key     = key;
	 		key              += ceil(parent_keys/options);
			bcb->max_key     = key - 1;

			if (l != 0)
				buffers[parent]->add_child(start + index);
			
			parent_keys -= ceil(parent_keys/options);
			options--;
			buffers.push_back(bcb);
			index++; // seperate variable because sometimes we skip stuff
			if(bcb->is_leaf())
				size += leaf_size + page_size; // leaves are of size == sketch
			else 
				size += buffer_size + page_size;
		}
	}

    // allocate file space for all the nodes to prevent fragmentation
    #ifdef HAVE_FALLOCATE
	fallocate(backing_store, 0, 0, size); // linux only but fast
	#else
	posix_fallocate(backing_store, 0, size); // portable but much slower
    #endif
    
    backing_EOF = size;
    // print_tree(buffers);
}

// serialize an update to a data location (should only be used for root I think)
inline void BufferTree::serialize_update(char *dst, update_t src) {
	node_id_t node1 = src.first;
	node_id_t node2 = src.second;

	memcpy(dst, &node1, sizeof(node_id_t));
	memcpy(dst + sizeof(node_id_t), &node2, sizeof(node_id_t));
}

inline update_t BufferTree::deserialize_update(char *src) {
	update_t dst;
	dst.first  = *((node_id_t *) src);
	dst.second = *((node_id_t *) (src + sizeof(node_id_t)));

	return dst;
}

// copy two serailized updates between two locations
inline void BufferTree::copy_serial(char *src, char *dst) {
	memcpy(dst, src, serial_update_size);
}

/*
 * Load a key from a given location
 */
inline node_id_t BufferTree::load_key(char *location) {
	node_id_t key;
	memcpy(&key, location, sizeof(node_id_t));
	return key;
}

/*
 * Helper function which determines which child we should flush to
 */
inline uint32_t which_child(node_id_t key, node_id_t min_key, node_id_t max_key, uint16_t options) {
	node_id_t total = max_key - min_key + 1;
	double div = (double)total / options;

	uint32_t larger_kids = total % options;
	uint32_t larger_count = larger_kids * ceil(div);
	node_id_t idx = key - min_key;

	if (idx >= larger_count)
		return ((idx - larger_count) / (int)div) + larger_kids;
	else
		return idx / ceil(div);
}

/*
 * Perform an insertion to the buffer-tree
 * Insertions always go to the root
 */
insert_ret_t BufferTree::insert(update_t upd) {
	// printf("inserting to buffer tree . . . \n");
	
	// first calculate which of the roots we're inserting to
	Node key = upd.first;
	buffer_id_t r_id = which_child(key, 0, N-1, B); // TODO: Here we are assuming that N >= B
	BufferControlBlock *root = buffers[r_id];
	// printf("Insertion to buffer %i of size %llu\n", r_id, root->size());

	// perform the write and block if necessary
	while (true) {
		std::unique_lock<std::mutex> lk(BufferControlBlock::buffer_ready_lock);
		BufferControlBlock::buffer_ready.wait(lk, [root]{return (root->size() < buffer_size + page_size);});
		if (root->size() < buffer_size + page_size) {
			lk.unlock();
			root->lock(); // ensure that a worker isn't flushing this node.
			serialize_update(BufferTree::cache + root->size() + root->offset(), upd);
			root->set_size(root->size() + serial_update_size);
			// printf("Did an insertion. Root %i size now %lu\n", r_id, root->size());
			break;
		}
		lk.unlock();
	}

	// if the buffer is full enough, push it to the flush_queue
	if (root->size() > buffer_size && root->size() - serial_update_size <= buffer_size) {
		BufferFlusher::queue_lock.lock();
		BufferFlusher::flush_queue.push(r_id);
		BufferFlusher::queue_lock.unlock();
		BufferFlusher::flush_ready.notify_one();
		// printf("Added to %i to flush queue\n", r_id);
	}
	root->unlock(); // no longer need root lock
}

/*
 * Function for perfoming a flush anywhere in the tree agnostic to position.
 * this function should perform correctly so long as the parameters are correct.
 *
 * IMPORTANT: after perfoming the flush it is the caller's responsibility to reset
 * the number of elements in the buffer and associated pointers.
 *
 * IMPORTANT: Unless we add more flush_buffers only a single flush at each level may occur 
 * at once otherwise the data will clash
 */
flush_ret_t BufferTree::do_flush(flush_struct &flush_from, uint32_t data_size, uint32_t begin, 
	node_id_t min_key, node_id_t max_key, uint16_t options, uint8_t level) {
	// setup
	uint32_t full_flush = page_size - (page_size % serial_update_size);

	char **flush_pos = flush_from.flush_positions[level];
	char **flush_buf = flush_from.flush_buffers[level];

	char *data_start = flush_from.read_buffers[level];
	char *data       = data_start;
	for (uint32_t i = 0; i < B; i++) {
		flush_pos[i] = flush_buf[i];
	}

	while (data - data_start < data_size) {
		node_id_t key = load_key(data);
		uint32_t child  = which_child(key, min_key, max_key, options);
		if (child > B - 1) {
			printf("ERROR: incorrect child %u abandoning insert key=%u min=%u max=%u\n", child, key, min_key, max_key);
			printf("first child = %u\n", buffers[begin]->get_id());
			printf("data pointer = %lu data_start=%lu data_size=%u\n", (uint64_t) data, (uint64_t) data_start, data_size);
			throw KeyIncorrectError();
		}
		if (buffers[child+begin]->min_key > key || buffers[child+begin]->max_key < key) {
			printf("ERROR: bad key %u for child %u, child min = %u, max = %u\n", 
				key, child, buffers[child+begin]->min_key, buffers[child+begin]->max_key);
			throw KeyIncorrectError();
		}
 
		copy_serial(data, flush_pos[child]);
		flush_pos[child] += serial_update_size;

		if (flush_pos[child] - flush_buf[child] >= full_flush) {
			// write to our child, return value indicates if it needs to be flushed
			uint32_t size = flush_pos[child] - flush_buf[child];
			if(buffers[begin+child]->write(flush_buf[child], size)) {
				flush_control_block(flush_from, buffers[begin+child]);
			}
			flush_pos[child] = flush_buf[child]; // reset the flush_position
		}
		data += serial_update_size; // go to next thing to flush
	}

	// loop through the flush buffers and write out any non-empty ones
	for (uint32_t i = 0; i < B; i++) {
		if (flush_pos[i] - flush_buf[i] > 0) {
			// write to child i, return value indicates if it needs to be flushed
			uint32_t size = flush_pos[i] - flush_buf[i];
			if(buffers[begin+i]->write(flush_buf[i], size)) {
				flush_control_block(flush_from, buffers[begin+i]);
			}
		}
	}
}

flush_ret_t BufferTree::flush_control_block(flush_struct &flush_from, BufferControlBlock *bcb) {
	// printf("flushing "); bcb->print();
	if(bcb->size() == 0) {
		return; // don't flush empty control blocks
	}

	if (bcb->is_leaf()) {
		return flush_leaf_node(flush_from, bcb);
	}
	return flush_internal_node(flush_from, bcb);
}

flush_ret_t inline BufferTree::flush_internal_node(flush_struct &flush_from, BufferControlBlock *bcb) {
	uint8_t level = bcb->level;
	if (level == 0) { // we have this in cache
		uint32_t data_size = bcb->size();
		memcpy(flush_from.read_buffers[level], cache+bcb->offset(), data_size); // move the data over
		bcb->set_size(); // data is copied out so no need to save it
		bcb->unlock();

		// now do_flush which doesn't require locking the top buffer! yay!
		do_flush(flush_from, data_size, bcb->first_child, bcb->min_key, bcb->max_key, bcb->children_num, bcb->level);
		return;
	} 

	// sub level 0 flush
	bcb->lock(); // for these sub-level buffers we lock them the entire time they're being flushed
	uint32_t data_to_read = bcb->size();
	uint32_t offset = 0;
	while(data_to_read > 0) {
		int len = pread(backing_store, flush_from.read_buffers[level] + offset, data_to_read, bcb->offset() + offset);
		if (len == -1) {
			printf("ERROR flush failed to read from buffer %i, %s\n", bcb->get_id(), strerror(errno));
			exit(EXIT_FAILURE);
		}
		data_to_read -= len;
		offset += len;
	}
	do_flush(flush_from, bcb->size(), bcb->first_child, bcb->min_key, bcb->max_key, bcb->children_num, bcb->level);
	bcb->set_size(); // set size if sub level 0 flush
	bcb->unlock();
}

flush_ret_t inline BufferTree::flush_leaf_node(flush_struct &flush_from, BufferControlBlock *bcb) {
	uint8_t level = bcb->level;
	if (level == 0) {
		cq->push(cache + bcb->offset(), bcb->size());
		bcb->set_size();
		bcb->unlock();
		return;
	}

	// sub level flush
	bcb->lock(); // for these sub-level buffers we lock them the entire time they're being flushed
	uint32_t data_to_read = bcb->size();
	uint32_t offset = 0;
	while(data_to_read > 0) {
		int len = pread(backing_store, flush_from.read_buffers[level] + offset, data_to_read, bcb->offset() + offset);
		if (len == -1) {
			printf("ERROR flush failed to read from buffer %i, %s\n", bcb->get_id(), strerror(errno));
			exit(EXIT_FAILURE);
		}
		data_to_read -= len;
		offset += len;
	}
	cq->push(flush_from.read_buffers[level], bcb->size()); // add the data we read to the circular queue
		
	// reset the BufferControlBlock
	bcb->set_size();
	bcb->unlock();
}

// ask the buffer tree for data
// this function may sleep until data is available
bool BufferTree::get_data(data_ret_t &data) {
	File_Pointer idx = 0;

	// make a request to the circular buffer for data
	std::pair<int, queue_elm> queue_data;
	bool got_data = cq->peek(queue_data);

	if (!got_data)
		return false; // we got no data so return not valid

	int i         = queue_data.first;
	queue_elm elm = queue_data.second;
	char *serial_data = elm.data;
	uint32_t len      = elm.size;

	if (len == 0) {
		cq->pop(i);
		return false; // we got no data so return not valid
        }

	data.second.clear(); // remove any old data from the vector
	uint32_t vec_len  = len / serial_update_size;
	data.second.reserve(vec_len); // reserve space for our updates

	// assume the first key is correct so extract it
	node_id_t key = load_key(serial_data);
	data.first = key;

	while(idx < (uint64_t) len) {
		update_t upd = deserialize_update(serial_data + idx);
		// printf("got update: %lu %lu\n", upd.first, upd.second);
		if (upd.first == 0 && upd.second == 0) {
			break; // got a null entry so done
		}

		if (upd.first != key) {
			// error to handle some weird unlikely buffer tree shenanigans
			printf("source node %u and key %u do not match in get_data()\n", upd.first, key);
			printf("idx = %lu  len = %u\n", idx, len);
			throw KeyIncorrectError();
		}

		// printf("query to node %lu got edge to node %lu\n", key, upd.second);
		data.second.push_back(upd.second);
		idx += serial_update_size;
	}

	cq->pop(i); // mark the cq entry as clean
	return true;
}

// Helper function for force flush. This function flushes an entire subtree rooted at one
// of our cached root buffers
flush_ret_t BufferTree::flush_subtree(flush_struct &flush_from, buffer_id_t first_child) {
	BufferControlBlock *root = buffers[first_child];
	root->lock();

	buffer_id_t num_children = 1;
	for(int l = 0; l < max_level; l++) {
		buffer_id_t new_first_child  = 0;
		buffer_id_t new_num_children = 0;
		for (buffer_id_t idx = 0; idx < num_children; idx++) {
			BufferControlBlock *cur = buffers[idx + first_child];
			if (idx == 0) new_first_child = cur->first_child;
			new_num_children += cur->children_num;
			flush_control_block(flush_from, cur);
		}
		first_child  = new_first_child;
		num_children = new_num_children;
	}
	root->unlock();
}

flush_ret_t BufferTree::force_flush() {
	// Tell the BufferFlushers to flush the entire subtrees
	BufferFlusher::force_flush = true;

	// add each of the roots to the flush_queue
	BufferFlusher::queue_lock.lock();
	for (buffer_id_t idx = 0; idx < B && idx < buffers.size(); idx++) {
		BufferFlusher::flush_queue.push(idx);
	}
	BufferFlusher::queue_lock.unlock();
	BufferFlusher::flush_ready.notify_all();

	// help flush the subtrees by pulling from the queue
	while(true) {
		BufferFlusher::queue_lock.lock();
		if(!BufferFlusher::flush_queue.empty()) {
			buffer_id_t idx = BufferFlusher::flush_queue.front();
			BufferFlusher::flush_queue.pop();
			BufferFlusher::queue_lock.unlock();
			// printf("main thread flushing buffer %u\n", idx);
			flush_subtree(*flush_data, idx);
			// printf("main thread done\n");
		} else {
			BufferFlusher::queue_lock.unlock();
			break;
		}
	}

	// Here we delete the buffer flushers to assert that they are done with their work
	// TODO: make the number of buffer flushers a parameter
	for(int i = 0; i < num_flushers; i++) {
		delete flushers[i];
	}
}

void BufferTree::set_non_block(bool block) {
	if (block) {
		cq->no_block = true; // circular queue operations should no longer block
		cq->cirq_empty.notify_all();
		cq->cirq_full.notify_all();
	}
	else {
		cq->no_block = false; // set circular queue to block if necessary
	}
}
