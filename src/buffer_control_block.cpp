//
// Created by victor on 3/2/21.
//

#include "../include/buffer_control_block.h"
#include "../include/buffer_tree.h"

#include <unistd.h>

BufferControlBlock::BufferControlBlock(buffer_id_t id, int fd, uint32_t off, uint32_t M)
  : id(id), f_ext(fd), file_offset(off), data_max_size(2*M){
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


bool BufferControlBlock::write(char *data, uint32_t size) {
	printf("Writing to buffer %d data pointer = %p with size %i\n", id, data, size);
	if (storage_ptr + size > data_max_size)
		throw BufferFullError(id);
	pwrite(f_ext, data, size, file_offset + storage_ptr);
	storage_ptr += BufferTree::serial_update_size;
	return needs_flush(size);
}
