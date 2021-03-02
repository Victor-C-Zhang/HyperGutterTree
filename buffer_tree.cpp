#include "include/buffer_tree.h"

#include <utility>

insert_ret_t BufferTree::insert(update_t upd) {

}

BufferTree::BufferTree(std::string dir, uint32_t size, uint32_t b, uint32_t
levels) : dir(std::move(dir)), M(size), B(b) {

}

flush_ret_t BufferTree::flush(BufferControlBlock &buffer) {
// TODO
//sketchWriteManager.write_updates(if necessary);
}

BufferTree::~BufferTree() = default;
