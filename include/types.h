#pragma once
#include <utility>
#include <vector>
#include <graph_zeppelin_common.h>

typedef uint128_t index_t;
typedef std::pair<int8_t, index_t> delta_update_t;
typedef std::pair<node_id_t, delta_update_t> update_tt; // node, delta, sketch index

typedef void insert_ret_t;
typedef void flush_ret_t;
