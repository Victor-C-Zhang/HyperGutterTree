#pragma once
#include <cassert>
#include <cmath>
#include <variant>

#include "work_queue.h"
#include "types.h"
#include "buffering_system.h"

constexpr size_t M = 32768; // The size in bytes of the L1 cache
constexpr size_t B = 64;    // The size of a cache line
static_assert(M % B == 0);

/*
 * CacheGutterTree is a version of the StandaloneGutters buffering system
 * It is designed to be highly efficient when the buffering system is entirely in memory
 * and when the number of nodes in the graph very high by maintaining a gutter tree like
 * structure but for cache rather than disk.
 */
class CacheGutterTree : public BufferingSystem {
 private:
  static size_t getNumLevels(size_t numLeaves) {
    const double fractionalNumLevels = std::log(numLeaves) / std::log(M / B);
    const bool dropTopLevel = (fractionalNumLevels - std::floor(fractionalNumLevels)) < epsilon;
    const size_t numLevels = dropTopLevel? static_cast<size_t>(fractionalNumLevels) - 1 : 
                                           static_cast<size_t>(fractionalNumLevels);
    return numLevels;
  }

  struct CacheGutterTreeConfig {
    const size_t numLevels;
    const size_t bufferSize;
    WorkQueue &wq;
  };

  class CacheGutterTreeNode {
   private:
    static constexpr size_t maxOccupancy = M / sizeof(update_t);
    static constexpr size_t numChildren = M / B;

    const CacheGutterTreeConfig &config;
    const std::pair<size_t, size_t> updateSpan;
    std::vector<update_t> buffer;
    const bool leafNode;

    // I'd like to use variant here, but I can't because we use too old a
    // version of C++, and union isn't worth the trouble.
    std::vector<CacheGutterTreeNode> childNodes;
    std::vector<std::vector<node_id_t>> outputBuffers;

   public:
    static constexpr size_t calculateSpanQuanta(std::pair<size_t, size_t> updateSpan) {
      return (updateSpan.second - updateSpan.first) / (M / B);
    }

    std::pair<size_t, size_t> calculateSubspan (std::pair<size_t, size_t> updateSpan, size_t i) {
      const size_t spanQuanta = calculateSpanQuanta(updateSpan);
      const size_t spanBegin = updateSpan.first + i * spanQuanta;
      const size_t spanEnd = i + 1 == numChildren ? updateSpan.second : spanBegin + spanQuanta - 1;
      return std::pair<size_t, size_t>(spanBegin, spanEnd);
    }

    CacheGutterTreeNode(std::pair<size_t, size_t> updateSpan, size_t level,
                             const CacheGutterTreeConfig &config) : config(config),
                             updateSpan(updateSpan), buffer(), leafNode(level == config.numLevels) {
      buffer.reserve(maxOccupancy);
      if (leafNode) {
        const size_t numOutputBuffers = updateSpan.second - updateSpan.first + 1;
        for (size_t i = 0; i < numOutputBuffers; ++i) {
          outputBuffers.emplace_back();
          outputBuffers.back().reserve(config.bufferSize);
          outputBuffers.back().push_back(updateSpan.first + i);
        }
      } else {
        for (size_t i = 0; i < numChildren; ++i) {
          childNodes.emplace_back(calculateSubspan(updateSpan, i), level + 1, config);
        }
      }
    }

    CacheGutterTreeNode(const CacheGutterTreeNode &) = default;

    void insert(update_t upd) {
      buffer.push_back(upd);
      if (buffer.size() == maxOccupancy) {
        flush();
      }
    }

    void flush() {
      if (leafNode) {
        for (const auto &update : buffer) {
          auto &outputBuffer = outputBuffers[update.first - updateSpan.first];
          outputBuffer.push_back(update.second);
          if (outputBuffer.size() == config.bufferSize) {
            config.wq.push(reinterpret_cast<char *>(outputBuffer.data()),
                           config.bufferSize * sizeof(node_id_t));
            outputBuffer.clear();
            outputBuffer.push_back(updateSpan.first + update.first);
          }
        }
      } else {
        const size_t spanQuanta = calculateSpanQuanta(updateSpan);
        for (const auto &update : buffer) {
          const size_t bucket = std::min(static_cast<size_t>((update.first - updateSpan.first) 
                                / spanQuanta), numChildren - 1);
          childNodes[bucket].insert(update);
        }
      }
      buffer.clear();
    }
  };
  uint32_t bytes_size; // size of leaf in bytes

  static constexpr double epsilon = 0.1;
  WorkQueue wq;
  CacheGutterTreeConfig config;
  CacheGutterTreeNode root;

 public:
  /*
   * Constructs a new CacheGutterTree
   * @param nodes     the number of nodes in the graph
   * @param workers   the number of workers that will be removing batches
   * Note: using comma operator in a hacky way to configure the system before initializing
   */
  CacheGutterTree(node_id_t nodes, int workers) :
    bytes_size((configure_system(), gutter_factor * sketch_size(nodes))),
    wq{workers * (int) queue_factor, (int) bytes_size},
    config{getNumLevels(nodes), (size_t)(gutter_factor*sketch_size(nodes)/sizeof(node_id_t)), wq},
    root{std::pair<size_t, size_t>(0, nodes - 1), 0, config} {}

  /**
   * Puts an update into the data structure.
   * @param upd the edge update.
   * @return nothing.
   */
  insert_ret_t insert(const update_t &upd) { root.insert(upd); }

  /**
   * Ask the buffer queue for data and sleep if necessary until it is available.
   * @param data        to store the fetched data.
   * @return            true if got valid data, false if unable to get data.
   */
  bool get_data(data_ret_t &data) {
    // make a request to the circular buffer for data
    std::pair<int, queue_elm> queue_data;
    bool got_data = wq.peek(queue_data);

    if (!got_data) return false;  // we got no data so return not valid
    int i = queue_data.first;
    queue_elm elm = queue_data.second;
    auto *serial_data = reinterpret_cast<node_id_t *>(elm.data);
    uint32_t len = elm.size;
    assert(len % sizeof(node_id_t) == 0);

    if (len == 0) return false;  // we got no data so return not valid
    // assume the first key is correct so extract it
    node_id_t key = serial_data[0];
    data.first = key;

    data.second.clear();  // remove any old data from the vector
    uint32_t vec_len = len / sizeof(node_id_t);
    data.second.reserve(vec_len);  // reserve space for our updates
    for (uint32_t j = 1; j < vec_len; ++j) {
      data.second.push_back(serial_data[j]);
    }

    wq.pop(i);  // mark the wq entry as clean
    return true;
  }

  /**
   * Flushes all pending buffers.
   * @return nothing.
   */
  flush_ret_t force_flush() { root.flush(); }

  /**
   * Notifies all threads waiting on condition variables that they should
   * check their wait condition again. Useful when switching from blocking to
   * non-blocking calls to the circular queue.
   * For example: we set this to true when shutting down the graph_workers.
   * @param    block is true if we should turn on non-blocking operations
   *           and false if we should turn them off.
   */
  void set_non_block(bool block) {
    if (block) {
      wq.no_block = true;  // circular queue operations should no longer block
      wq.wq_empty.notify_all();
    } else {
      wq.no_block = false;  // set circular queue to block if necessary
    }
  }
};
