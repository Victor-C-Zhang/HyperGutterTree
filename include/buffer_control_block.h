//
// Created by victor on 3/2/21.
//

#ifndef FASTBUFFERTREE_BUFFER_CONTROL_BLOCK_H
#define FASTBUFFERTREE_BUFFER_CONTROL_BLOCK_H


#include <cstdint>
#include <string>
#include <mutex>
#include <condition_variable>

typedef uint32_t buffer_id_t;

/**
 * Buffer metadata class. Care should be taken to synchronize access to the
 * *entire* data structure.
 */
class BufferControlBlock {
private:
  buffer_id_t id;

  // busy lock
  std::mutex mtx;

  // condition variable to determine if the buffer needs to be flushed
  // std::condition_variable needs_flushing;

  // how many items are currently in the buffer
  uint32_t storage_ptr;

  // where in the file is our data stored
  uint32_t file_offset;

  /*
   * Check if this buffer needs a flush
   * @return true if buffer needs a flush and it didn't need it before the most recent write
   */
  bool needs_flush(uint32_t size);

public:
  // this node's level in the tree. 0 is root, 1 is it's children, etc
  uint8_t level;
  
  /**
   * Generates metadata and file handle for a new buffer.
   * @param id an integer identifier for the buffer.
   * @param off the offset into the file at which this buffer's data begins
   * @param level the level in the tree this buffer resides at
   */
  BufferControlBlock(buffer_id_t id, uint32_t off, uint8_t level);

  /**
   * Lock the buffer for data transfer. Blocks the calling context if the
   * lock is unavailable.
   */
  void lock();

  /**
   * Unlocks the buffer once data transfer is complete. Throws an error if
   * the buffer isn't locked.
   * @throw BufferNotLockedError if the buffer isn't locked.
   */
  void unlock();

  /**
   * Atomic method to check if the buffer is "busy", i.e. flushing or being
   * flushed to.
   * @return true if the buffer is busy.
   */
  bool busy();

  /*
   * Write to the buffer managed by this metadata.
   * @param data the data to write
   * @param size the size of the data written
   * @return true if buffer needs flush for the first time and false otherwise
   */
  bool write(char * data, uint32_t size);

  /*
   * Flush the buffer this block controls
   * @return nothing
   */
  void flush();
};

class BufferNotLockedError : public std::exception {
private:
  buffer_id_t id;
public:
  explicit BufferNotLockedError(buffer_id_t id) : id(id){}
  const char* what() const noexcept override {
    std::string temp = "Buffer not locked! id: " + std::to_string(id);
    // ok, since this will terminate the program anyways
    return temp.c_str();
  }
};

#endif //FASTBUFFERTREE_BUFFER_CONTROL_BLOCK_H
