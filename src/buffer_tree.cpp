#include "../include/buffer_tree.h"

#include <utility>
#include <unistd.h> //sysconf
#include <string.h> //memcpy
#include <fcntl.h>  //posix_fallocate
#include <errno.h>

/*
 * Static "global" BufferTree variables
 */
uint     BufferTree::page_size;
uint8_t  BufferTree::max_level;
uint32_t BufferTree::max_buffer_size;
uint32_t BufferTree::buffer_size;
uint64_t BufferTree::backing_EOF;
int      BufferTree::backing_store;


/*
 * Constructor
 * Sets up the buffer tree given the storage directory, buffer size, number of children
 * and the number of nodes we will insert(N)
 * We assume that node indices begin at 0 and increase to N-1
 */
BufferTree::BufferTree(std::string dir, uint32_t size, uint32_t b, Node
nodes, int workers, bool reset=false) : dir(dir), M(size), B(b), N(nodes) {
	page_size = sysconf(_SC_PAGE_SIZE); // works on POSIX systems (alternative is boost)
	int file_flags = O_RDWR | O_CREAT; // direct memory O_DIRECT may or may not be good
	if (reset) {
		file_flags |= O_TRUNC;
	}

	if (M < page_size) {
		printf("WARNING: requested buffer size smaller than page_size. Set to page_size.\n");
		M = page_size;
	}
	
	// malloc the memory for the flush buffers
	flush_buffers = (char **) malloc(sizeof(char *) * B);
	for (uint i = 0; i < B; i++) {
		flush_buffers[i] = (char *) calloc(page_size, sizeof(char));
	}
	
	// setup static variables
	max_level       = 0;
	buffer_size     = M; // probably figure out a better solution than this
	max_buffer_size = 2 * M;
	backing_EOF     = 0;

	// malloc the memory for the root node
	root_node = (char *) malloc(max_buffer_size);
	root_position = 0;

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
	// make space for full updates of size 2 times the number of workers
	cq = new CircularQueue(2*workers, M);
	
	// will want to use mmap instead? - how much is in RAM after allocation (none?)
	// can't use mmap instead might use it as well. (Still need to create the file to be a given size)
	// will want to use pwrite/read so that the IOs are thread safe and all threads can share a single file descriptor
	// if we don't use mmap that is

	// printf("Successfully created buffer tree\n");
}

BufferTree::~BufferTree() {
	printf("Closing BufferTree\n");
	// force_flush(); // flush everything to leaves (could just flush to files in higher levels)

	// free malloc'd memory
	for (uint i = 0; i < B; i++) {
		free(flush_buffers[i]);
	}
	free(flush_buffers);
	free(root_node);

	for(uint i = 0; i < buffers.size(); i++) {
		if (buffers[i] != nullptr)
			delete buffers[i];
	}

	close(backing_store);
}

void print_tree(std::vector<BufferControlBlock *>bcb_list) {
	for(uint i = 0; i < bcb_list.size(); i++) {
		if (bcb_list[i] != nullptr) 
			bcb_list[i]->print();
	}
}

// TODO: clean up this function
void BufferTree::setup_tree() {
	max_level = ceil(log(N) / log(B));
	printf("Creating a tree of depth %i\n", max_level);
	File_Pointer size = 0;

	// create the BufferControlBlocks
	for (uint l = 1; l <= max_level; l++) {
		uint level_size  = pow(B, l); // number of blocks in this level
		uint plevel_size = pow(B, l-1);
		uint start = buffers.size();
		buffers.reserve(start + level_size);
		Node key = 0;
		double parent_keys = N;
		uint options = B;
		bool skip = false;
		uint prev = 0;
		File_Pointer index = 0;
		for (uint i = 0; i < level_size; i++) {
			// get the parent of this node if not level 1 and if we have a new parent
			if (l > 1 && (i-start) % B == 0) {
				prev = start + i/B - plevel_size; // this logic should check out because only the last level is ever not full
				parent_keys = buffers[prev]->max_key - buffers[prev]->min_key + 1;
				key = buffers[prev]->min_key;
				options = B;
				skip = (parent_keys == 1)? true : false;
			}
			if (skip || parent_keys == 0) {
				continue;
			}

			BufferControlBlock *bcb = new BufferControlBlock(start + index, size + max_buffer_size*index, l);
	 		bcb->min_key            = key;
	 		key                     += ceil(parent_keys/options);
			bcb->max_key            = key - 1;
			if (l != 1)
				buffers[prev]->add_child(start + index);
			
			parent_keys -= ceil(parent_keys/options);
			options--;
			buffers.push_back(bcb);
			index++; // seperate variable because sometimes we skip stuff
		}
		size += max_buffer_size * index;
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
	Node node1 = src.first;
	Node node2 = src.second;

	memcpy(dst, &node1, sizeof(Node));
	memcpy(dst + sizeof(Node), &node2, sizeof(Node));
}

inline update_t BufferTree::deserialize_update(char *src) {
	update_t dst;
	memcpy(&dst.first, src, sizeof(Node));
	memcpy(&dst.second, src + sizeof(Node), sizeof(Node));

	return dst;
}

// copy two serailized updates between two locations
inline void BufferTree::copy_serial(char *src, char *dst) {
	memcpy(dst, src, serial_update_size);
}

/*
 * Load a key from a given location
 */
inline Node BufferTree::load_key(char *location) {
	Node key;
	memcpy(&key, location, sizeof(Node));
	return key;
}

/*
 * Perform an insertion to the buffer-tree
 * Insertions always go to the root
 */
insert_ret_t BufferTree::insert(update_t upd) {
	// printf("inserting to buffer tree . . . ");
	root_lock.lock();
	if (root_position + serial_update_size > 2 * M) {
		flush_root(); // synchronous approach for testing
		// throw BufferFullError(-1); // TODO maybe replace with synchronous flush
	}

	serialize_update(root_node + root_position, upd);
	root_position += serial_update_size;
	root_lock.unlock();
	// printf("done insert\n");
}

/*
 * Helper function which determines which child we should flush to
 */
inline uint32_t which_child(Node key, Node min_key, Node max_key, uint8_t options) {
	Node total = max_key - min_key + 1;
	double div = (double)total / options;
	uint larger_kids = total % options;
	uint larger_count = larger_kids * ceil(div);
	Node idx = key - min_key;

	if (idx >= larger_count)
		return ((idx - larger_count) / (int)div) + larger_kids;
	else
		return idx / ceil(div);
}

/*
 * Function for perfoming a flush anywhere in the tree agnostic to position.
 * this function should perform correctly so long as the parameters are correct.
 * 
 * IMPORTANT: after perfoming the flush it is the caller's responsibility to reset
 * the number of elements in the buffer and associated pointers.
 *
 * IMPORTANT: Unless we add more flush_buffers only a single flush may occur at once
 * otherwise the data will clash
 */
flush_ret_t BufferTree::do_flush(char *data, uint32_t data_size, uint32_t begin, Node min_key, Node max_key, uint8_t options, std::queue<BufferControlBlock *> &fq) {
	// setup
	uint32_t full_flush = page_size - (page_size % serial_update_size);

	char *data_start = data;
	char **flush_positions = (char **) malloc(sizeof(char *) * B); // TODO move malloc out of this function
	for (uint i = 0; i < B; i++) {
		flush_positions[i] = flush_buffers[i];
	}

	while (data - data_start < data_size) {
		Node key = load_key(data);
		uint32_t child  = which_child(key, min_key, max_key, options);
		if (child > B - 1) {
			printf("ERROR: incorrect child %u abandoning insert key=%lu min=%lu max=%lu\n", child, key, min_key, max_key);
			printf("first child = %u\n", buffers[begin]->get_id());
			printf("data pointer = %lu data_start=%lu data_size=%u\n", (uint64_t) data, (uint64_t) data_start, data_size);
			data += serial_update_size;
			exit(EXIT_SUCCESS);
			continue;
		}
		if (buffers[child+begin]->min_key > key || buffers[child+begin]->max_key < key) {
			printf("ERROR: bad key %lu for child %u, child min = %lu, max = %lu abandoning insert\n", 
				key, child, buffers[child+begin]->min_key, buffers[child+begin]->max_key);
			data += serial_update_size;
			continue;
		}
 
		copy_serial(data, flush_positions[child]);
		flush_positions[child] += serial_update_size;

		if (flush_positions[child] - flush_buffers[child] >= full_flush) {
			// write to our child, return value indicates if it needs to be flushed
			uint size = flush_positions[child] - flush_buffers[child];
			if(buffers[begin+child]->write(flush_buffers[child], size)) {
				fq.push(buffers[begin+child]);
			}

			flush_positions[child] = flush_buffers[child]; // reset the flush_position
		}
		data += serial_update_size; // go to next thing to flush
	}

	// loop through the flush_buffers and write out any non-empty ones
	for (uint i = 0; i < B; i++) {
		if (flush_positions[i] - flush_buffers[i] > 0) {
			// write to child i, return value indicates if it needs to be flushed
			uint size = flush_positions[i] - flush_buffers[i];
			if(buffers[begin+i]->write(flush_buffers[i], size)) {
				fq.push(buffers[begin+i]);
			}
		}
	}
	free(flush_positions);
}

flush_ret_t inline BufferTree::flush_root() {
	//printf("Flushing root\n");
	// root_lock.lock(); // TODO - we can probably reduce this locking to only the last page
	do_flush(root_node, root_position, 0, 0, N-1, B, flush_queue1);
	root_position = 0;
	// root_lock.unlock();

	while (!flush_queue1.empty()) { // REMOVE later ... synchronous approach
		BufferControlBlock *to_flush = flush_queue1.front();
		flush_queue1.pop();
		flush_control_block(to_flush);
	}
}

flush_ret_t inline BufferTree::flush_control_block(BufferControlBlock *bcb) {
	//printf("flushing "); bcb->print();

	char *data = (char *) malloc(max_buffer_size); // TODO malloc only once instead of per call
	uint32_t data_to_read = bcb->size();
	uint32_t offset = 0;
	while(data_to_read > 0) {
		int len = pread(backing_store, data + offset, data_to_read, bcb->offset() + offset);
		if (len == -1) {
			printf("ERROR flush failed to read from buffer %i, %s\n", bcb->get_id(), strerror(errno));
			return;
		}
		data_to_read -= len;
		offset += len;
	}

	if (bcb->min_key == bcb->max_key) { // this is a leaf node
		// printf("adding key %i from buffer %i to circular queue\n", bcb->min_key, bcb->get_id());
		cq->push(data, bcb->size()); // add data to the circular queue
		
		// reset the BufferControlBlock (we have emptied it of data)
		bcb->reset();
		return;
	}

	// printf("read %lu bytes\n", len);

	do_flush(data, bcb->size(), bcb->first_child, bcb->min_key, bcb->max_key, bcb->children_num, flush_queue_wild);
	bcb->reset();

	free(data);

	while (!flush_queue_wild.empty()) { // REMOVE later ... synchronous approach
		BufferControlBlock *to_flush = flush_queue_wild.front();
		flush_queue_wild.pop();
		flush_control_block(to_flush);
	}
}

// ask the buffer tree for data
// this function may sleep until data is available
bool BufferTree::get_data(data_ret_t &data) {
	File_Pointer idx = 0;

	// make a request to the circular buffer for data
	std::pair<int, queue_elm> queue_data;
	bool got_data = cq->peek(queue_data);

	if (!got_data)
		return false; // we got no data so return empty

	int i         = queue_data.first;
	queue_elm elm = queue_data.second;
	char *serial_data = elm.data;
	uint32_t len      = elm.size;

	data.second.clear(); // remove any old data from the vector
	uint32_t vec_len  = len / serial_update_size;
	data.second.reserve(vec_len); // reserve space for our updates

	// assume the first key is correct so extract it
	Node key = deserialize_update(serial_data).first;
	data.first = key;

	while(idx < (uint64_t) len) {
		update_t upd = deserialize_update(serial_data + idx);
		// printf("got update: %lu %lu\n", upd.first, upd.second);
		if (upd.first == 0 && upd.second == 0) {
			break; // got a null entry so done
		}

		if (upd.first == key) {
			// printf("query to node %lu got edge to node %lu\n", key, upd.second);
			data.second.push_back(upd.second);
		}
		idx += serial_update_size;
	}

	cq->pop(i); // mark the cq entry as clean
	return true;
}

flush_ret_t BufferTree::force_flush() {
	// printf("Force flush\n");
	flush_root();
	// loop through each of the bufferControlBlocks and flush it
	// looping from 0 on should force a top to bottom flush (if we do this right)
	
	for (BufferControlBlock *bcb : buffers) {
		if (bcb != nullptr) {
			flush_control_block(bcb);
		}
	}
}

void BufferTree::bypass_wait() {
	cq->no_block = true; // circular queue operations should no longer block
	cq->cirq_empty.notify_all();
}
