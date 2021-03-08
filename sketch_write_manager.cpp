//
// Created by victor on 3/2/21.
//

#include "include/sketch_write_manager.h"

SketchWriteManager::SketchWriteManager() {
// TODO
}

SketchWriteManager::~SketchWriteManager() {
// TODO
}

void SketchWriteManager::write_updates(Node sketch_num,
                                       std::vector<update_t> &updates) {
// TODO
}

std::string SketchWriteManager::serialize(const update_t &update) {
  // TODO
  return std::__cxx11::string();
}

update_t SketchWriteManager::deserialize(const std::string &str) {
  // TODO
  return update_t();
}

