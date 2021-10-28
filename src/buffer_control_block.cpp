#include "../include/buffer_control_block.h"
#include "../include/gutter_tree.h"

#include <unistd.h>
#include <errno.h>
#include <string.h>

BufferControlBlock::BufferControlBlock(buffer_id_t id, File_Pointer off, uint8_t level)
  : id(id), file_offset(off), level(level){
  storage_ptr = 0;
}

inline bool BufferControlBlock::check_size_limit(uint32_t size, uint32_t flush_size, uint32_t max_size) {
  if (storage_ptr + size > max_size) {
    printf("buffer %i too full write size %u, storage_ptr = %lu, max = %u\n", id, size, storage_ptr, max_size);
    throw BufferFullError(id);
  }
  return storage_ptr + size >= flush_size;
}

bool BufferControlBlock::write(GutterTree *gt, char *data, uint32_t size) {
  // printf("Writing to buffer %d data pointer = %p with size %i\n", id, data, size);
  uint32_t flush_size = is_leaf()? gt->get_leaf_size() : gt->get_buffer_size();
  bool need_flush = check_size_limit(size, flush_size, flush_size + gt->get_page_size());

  if (level == 1) { // we cache the first level of the buffer tree
    memcpy(gt->get_cache() + file_offset + storage_ptr, data, size);
    storage_ptr += size;
    return need_flush;
  }

  int len = pwrite(gt->get_fd(), data, size, file_offset + storage_ptr);
  int w = 0;
  while(len < (int32_t)size) {
    if (len == -1) {
      printf("ERROR: write to buffer %i failed %s\n", id, strerror(errno));
      exit(EXIT_FAILURE);
    }
    w += len;
    size -= len;
    len = pwrite(gt->get_fd(), data + w, size, file_offset + storage_ptr + w);
  }
  storage_ptr += size;

  // return if this buffer should be added to the flush queue
  return need_flush;
}
