#pragma once
#include <utility>
#include <vector>
typedef uint32_t node_id_t;
typedef std::pair<node_id_t, node_id_t> update_t;
typedef void insert_ret_t;
typedef void flush_ret_t;
typedef std::pair<node_id_t, std::vector<node_id_t>> data_ret_t;
