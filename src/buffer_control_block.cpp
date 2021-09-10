//
// Created by victor on 3/2/21.
//

#include "../include/buffer_control_block.h"
#include "../include/buffer_tree.h"

#include <unistd.h>
#include <errno.h>
#include <string.h>

std::condition_variable BufferControlBlock::buffer_ready;
std::mutex BufferControlBlock::buffer_ready_lock;

BufferControlBlock::BufferControlBlock(buffer_id_t id, File_Pointer off, uint8_t level)
  : id(id), file_offset(off), level(level){
  storage_ptr = 0;
}

inline bool BufferControlBlock::needs_flush(uint32_t size) {
	if(is_leaf())
		return storage_ptr >= BufferTree::leaf_size && storage_ptr - size < BufferTree::leaf_size;
	else
		return storage_ptr >= BufferTree::buffer_size;
}

bool BufferControlBlock::write(char *data, uint32_t size) {
	// printf("Writing to buffer %d data pointer = %p with size %i\n", id, data, size);
	if(storage_ptr + size > BufferTree::buffer_size + BufferTree::page_size) {
		printf("buffer %i too full write size %u, storage_ptr = %lu, max = %lu\n", id, size, storage_ptr, BufferTree::buffer_size + BufferTree::page_size);
		throw BufferFullError(id);
	}
	
	if(is_leaf() && storage_ptr + size > BufferTree::leaf_size + BufferTree::page_size) {
		printf("leaf %i too full write size %u storage_ptr = %lu, max = %lu\n", id, size, storage_ptr, BufferTree::leaf_size + BufferTree::page_size);
		throw BufferFullError(id);
	}

	lock();
	int len = pwrite(BufferTree::backing_store, data, size, file_offset + storage_ptr);
	int w = 0;
	while(len < (int32_t)size) {
		if (len == -1) {
			printf("ERROR: write to buffer %i failed %s\n", id, strerror(errno));
			exit(EXIT_FAILURE);
		}
		w += len;
		size -= len;
		len = pwrite(BufferTree::backing_store, data + w, size, file_offset + storage_ptr + w);
	}
	storage_ptr += size;
	bool ret_flush = needs_flush(size);
	unlock();

	// return if this buffer should be added to the flush queue
	return ret_flush;
}

// loop through everything we're being asked to write and verify that it falls within the
// bounds of our min_key to max_key
//
// Used only when testing!
void BufferControlBlock::validate_write(char *data, uint32_t size) {
	char *data_start = data;
	printf("Warning: Validating write should only be used for testing\n");

	while(data - data_start < size) {
		Node key = *((Node *) data);
		if (key < min_key || key > max_key) {
			printf("ERROR: incorrect key %lu --> ", key); print();
			throw BufferFullError(id);
		}
		data += BufferTree::serial_update_size;
	}
}
