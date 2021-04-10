#include "../include/buffer_tree.h"

#include <utility>
#include <unistd.h> //sysconf
#include <fcntl.h>  //posix_fallocate
#include <string.h> //memcpy
#include <errno.h>

/*
 * Constructor
 * Sets up the buffer tree given the storage directory, buffer size, number of children
 * and the number of nodes we will insert(N)
 * We assume that node indices begin at 0 and increase to N-1
 */
BufferTree::BufferTree(std::string dir, uint32_t size, uint32_t b, uint64_t
nodes) : dir(dir), M(size), B(b), N(nodes) {
	page_size = sysconf(_SC_PAGE_SIZE); // works on POSIX systems (alternative is boost)
	
	// malloc the memory for the flush buffers
	flush_buffers = (char **) malloc(sizeof(char *) * B);
	for (uint i = 0; i < B; i++) {
		flush_buffers[i] = (char *) malloc(page_size);
	}

	// malloc the memory for the root node
	root_node = (char *) malloc(2 * M);
	root_position = 0;

	// open the file which will be our backing store for the non-root nodes
	// create it if it does not already exist
	printf("directory = %s\n", dir.c_str());
	std::string file_name = dir + "buffer_tree_v0.1.data";
	printf("opening file %s\n", file_name.c_str());
	backing_store = open(file_name.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR); //O_DIRECT ?
	if (backing_store == -1) {
		fprintf(stderr, "Failed to open backing storage file! error=%s\n", strerror(errno));
		exit(1);
	}

	// allocate space in the file for the level 1 nodes
	// this should prevent intra-level fragmentation
	// posix_fallocate(fd, o, n) to allocate space for n bytes at offset o in file fd
	posix_fallocate(backing_store, 0, 2 * B * M); // on non-linux systems this might be very slow
	
	// will want to use mmap instead? - how much is in RAM after allocation (none?)
	// can't use mmap instead might use it as well. (Still need to create the file to be a given size)
	// will want to use pwrite/read so that the IOs are thread safe and all threads can share a single file descriptor
	// if we don't use mmap that is

	// create first level of tree buffers.
	buffers.reserve(B);
	for (uint i = 0; i < B; i++) {
		// create a buffer control block of size 2*M at offset 2*M*i in the backing_store
		BufferControlBlock *bcb = new BufferControlBlock(i, backing_store, 2*M*i, 2*M);
		buffers.push_back(bcb);
	}

	printf("Successfully created buffer tree\n");
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

// serialize an update to a data location (should only be used for root I think)
inline void BufferTree::serialize_update(char *dst, update_t src) {
	uint32_t node1 = src.first.first;
	uint32_t node2 = src.first.second;
	bool value = src.second;

	memcpy(dst, &node1, sizeof(uint32_t));
	memcpy(dst + sizeof(uint32_t), &node2, sizeof(uint32_t));
	memcpy(dst + sizeof(uint32_t)*2, &value, sizeof(bool));
}

inline void BufferTree::deserialize_update(char *src, update_t dst) {
	memcpy(&dst.first.first, src, sizeof(uint32_t));
	memcpy(&dst.first.second, src + sizeof(uint32_t), sizeof(uint32_t));
	memcpy(&dst.second, src + sizeof(uint32_t)*2, sizeof(bool));
}

// copy two serailized updates between two locations
inline void BufferTree::copy_serial(char *src, char *dst) {
	memcpy(dst, src, serial_update_size);
}

/*
 * Load a key from a given location
 */
inline key_type BufferTree::load_key(char *location) {
	key_type key;
	memcpy(&key, location, sizeof(key_type));
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
inline uint64_t which_child(node_type key, uint64_t total, uint32_t options) {
	return key / (total / options); // can go in one of options places and there are total things
}

flush_ret_t BufferTree::flush_root() {
	printf("Flushing root\n");
	// setup
	char **flush_positions = (char **) malloc(sizeof(char *) * B); // TODO move malloc out of this function
	for (uint i = 0; i < B; i++) {
		flush_positions[i] = flush_buffers[i]; // TODO this casting is annoying (just convert everything to update_t type?)
	}

	// root_lock.lock(); // TODO - we can probably reduce this locking to only the last page
	char *data = root_node;
	while (data - root_node < root_position) {
		uint64_t key = load_key(data);
		short child  = which_child(key, N, B);
		copy_serial(data, flush_positions[child]);
		flush_positions[child] += serial_update_size;

		if (flush_positions[child] - flush_buffers[child] >= M) {
			// write to our child, return value indicates if it needs to be flushed
			uint size = flush_positions[child] - flush_buffers[child];
			if(buffers[child]->write(flush_buffers[child], size))
				flush_queue1.push(buffers[child]);

			flush_positions[child] = flush_buffers[child]; // reset the flush_position
		}
		data += serial_update_size; // go to next thing to flush
	}

	// loop through the flush_buffers and write out any non-empty ones
	for (uint i = 0; i < B; i++) {
		if (flush_positions[i] - flush_buffers[i] > 0) {
			// write to child i, return value indicates if it needs to be flushed
			uint size = flush_positions[i] - flush_buffers[i];
			if(buffers[i]->write(flush_buffers[i], size))
				flush_queue1.push(buffers[i]);
		}
	}

	root_position = 0;
	// root_lock.unlock();

	free(flush_positions);
}

// load data from buffer memory location so long as the key matches
// what we expect
data_ret_t BufferTree::get_data(uint32_t tag, uint32_t key) {
	data_ret_t data;
	data.first = key;
	uint32_t pos = tag;

	char *serial_data = (char *) malloc(2*M);
	pread(backing_store, serial_data, tag, 2*M);

	while(pos - tag < 2*M) {
		update_t upd;
		deserialize_update(serial_data + pos, upd);
		if (upd.first.first == key)
			data.second.push_back(std::pair<uint32_t, bool>(upd.first.second,upd.second));
		else
			break;
		pos += serial_update_size;
	}

	// TODO we need some way of marking this data as cleared so that this space
	// can be reused by future flushes

	return data;
}

flush_ret_t BufferTree::flush(BufferControlBlock &/*buffer*/) {
// TODO
//sketchWriteManager.write_updates(if necessary);
}

flush_ret_t force_flush() {
	// TODO: loop through each of the bufferControlBlocks and flush it
	// looping from 0 on should force a top to bottom flush (if we do this right)
}
