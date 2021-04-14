//
// Created by victor on 3/2/21.
//

#include "../include/buffer_control_block.h"
#include "../include/buffer_tree.h"

#include <unistd.h>
#include <errno.h>
#include <string.h>

BufferControlBlock::BufferControlBlock(buffer_id_t id, uint32_t off, uint8_t level)
  : id(id), file_offset(off), level(level){
  storage_ptr = 0;
}

void BufferControlBlock::lock() {
// TODO
}

void BufferControlBlock::unlock() {
// TODO
}

bool BufferControlBlock::busy() {
  // TODO
	return true;
}

inline bool BufferControlBlock::needs_flush(uint32_t size_written) {
	return storage_ptr > BufferTree::buffer_size && 
		storage_ptr - size_written < BufferTree::buffer_size;
}


bool BufferControlBlock::write(char *data, uint32_t size) {
	// printf("Writing to buffer %d data pointer = %p with size %i\n", id, data, size);
	if (storage_ptr + size > BufferTree::max_buffer_size) {
		printf("buffer %i error\n", id);
		throw BufferFullError(id);
	}


	int len = pwrite(BufferTree::backing_store, data, size, file_offset + storage_ptr);
	if (len == -1) {
		printf("ERROR: write to buffer %i failed %s\n", id, strerror(errno));
	}

	storage_ptr += size; // some null bytes at end

	// return if this buffer should be added to the flush queue
	return needs_flush(size);
}
