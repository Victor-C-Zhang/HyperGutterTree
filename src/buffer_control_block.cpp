#include "../include/buffer_control_block.h"
#include "../include/gutter_tree.h"
#include "../include/gt_file_errors.h"

#include <unistd.h>
#include <errno.h>
#include <string.h>

std::condition_variable BufferControlBlock::buffer_ready;
std::mutex BufferControlBlock::buffer_ready_lock;

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

  int len = pwrite(gt->get_fd(), data, size, file_offset + storage_ptr);
  int w = 0;
  while(len < (int32_t)size) {
    if (len == -1) {
      throw GTFileWriteError(strerror(errno), id);
    }
    w    += len;
    size -= len;
    len = pwrite(gt->get_fd(), data + w, size, file_offset + storage_ptr + w);
  }
  storage_ptr += size;

  // return if this buffer should be added to the flush queue
  return need_flush;
}

/* loop through everything we're being asked to write and verify that it falls within the
 * bounds of our min_key to max_key
 *
 * Use only when testing!
 */
void BufferControlBlock::validate_write(char *data, uint32_t size) {
  char *data_start = data;
  printf("Warning: Validating write should only be used for testing\n");

  while(data - data_start < size) {
    node_id_t key = *((node_id_t *) data);
    if (key < min_key || key > max_key) {
      printf("ERROR: Validate Write: incorrect key %u --> ", key); print();
      throw KeyIncorrectError();
    }
    data += GutterTree::serial_update_size;
  }
}
