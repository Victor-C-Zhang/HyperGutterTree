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

  // file descriptor of backing store, less directory information
  int f_ext;

  // how many items are currently in the buffer
  uint32_t storage_ptr;

  // where in the file is our data stored
  uint32_t file_offset;

  // maximum amount of data this buffer can store
  uint32_t data_max_size;

  /*
   * Check if this buffer needs a flush
   * @return true if buffer needs a flush and it didn't need it before the most recent write
   */
  inline bool needs_flush(uint32_t size_written) {
    return storage_ptr > data_max_size && storage_ptr - size_written < data_max_size;
  }

public:
  /**
   * Generates metadata and file handle for a new buffer.
   * @param id an integer identifier for the buffer.
   */
  BufferControlBlock(buffer_id_t id, int fd, uint32_t off, uint32_t M);

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
