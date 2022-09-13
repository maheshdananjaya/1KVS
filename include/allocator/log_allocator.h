// Author: Ming Zhang
// Copyright (c) 2021

#pragma once

#include "common/common.h"


//DAM chanage both when ever memory configuration change.
const offset_t HASH_BUFFER_SIZE = 1024 * 1024 * 1024*8;

const offset_t LOG_BUFFER_SIZE = 1024 * 1024 * 1024;
const node_id_t NUM_MEMORY_NODES = BACKUP_DEGREE + 1;
const coro_id_t MAX_NUM_COROS = 16; //DAM max variables

// Remote offset to write log
class LogOffsetAllocator {
 public:
  LogOffsetAllocator(t_id_t tid, t_id_t num_thread) {
    auto per_thread_remote_log_buffer_size = LOG_BUFFER_SIZE / num_thread;
    for (node_id_t i = 0; i < NUM_MEMORY_NODES; i++) {
      start_log_offsets[i] = tid * per_thread_remote_log_buffer_size;
      end_log_offsets[i] = (tid + 1) * per_thread_remote_log_buffer_size;
      current_log_offsets[i] = 0;
    }
  }

  LogOffsetAllocator(t_id_t tid, t_id_t num_thread, coro_id_t num_coro) {

    assert(num_coro <= MAX_NUM_COROS);
    num_coros_per_thread = num_coro;
    auto per_thread_remote_log_buffer_size = LOG_BUFFER_SIZE / num_thread;
    auto per_coro_remote_log_buffer_size = per_thread_remote_log_buffer_size/num_coro;

    for (node_id_t i = 0; i < NUM_MEMORY_NODES; i++) {
      start_log_offsets[i] = tid * per_thread_remote_log_buffer_size;
      end_log_offsets[i] = (tid + 1) * per_thread_remote_log_buffer_size;
      current_log_offsets[i] = 0;

      //init coro offset start, end and current. offset is set to start once the truncate message is sent. cyclic buffer.
      for (coro_id_t j=0; j< num_coro ; j++){
        coro_start_log_offsets[i][j] =  HASH_BUFFER_SIZE + (tid * per_thread_remote_log_buffer_size) + (j*per_coro_remote_log_buffer_size);
        coro_end_log_offsets[i][j] = HASH_BUFFER_SIZE + (tid * per_thread_remote_log_buffer_size) + ((j+1)*per_coro_remote_log_buffer_size);
        //coro_current_log_offsets[i][j] = (tid * per_thread_remote_log_buffer_size) + (j*per_coro_remote_log_buffer_size);;
        coro_current_log_offsets[i][j] = 0; // this is the offset
      }
    }
  }


  offset_t GetNextLogOffset(node_id_t node_id, size_t log_entry_size) {
    if (unlikely(start_log_offsets[node_id] + current_log_offsets[node_id] + log_entry_size > end_log_offsets[node_id])) {
      current_log_offsets[node_id] = 0;
    }
    offset_t offset = start_log_offsets[node_id] + current_log_offsets[node_id];
    current_log_offsets[node_id] += log_entry_size;
    return offset;
  }

  //DAM - For recovery
  offset_t GetStartLogOffset(node_id_t node_id, coro_id_t coro_id) {
      return coro_start_log_offsets[node_id][coro_id];
  }

  //SAM- For coroutines
  offset_t GetNextLogOffset(node_id_t node_id, coro_id_t coro_id, size_t log_entry_size) {

    //current coro log pointer. fixed sized logs always
    assert(coro_id < MAX_NUM_COROS);
    if (unlikely(coro_start_log_offsets[node_id][coro_id] + coro_current_log_offsets[node_id][coro_id] + log_entry_size > coro_end_log_offsets[node_id][coro_id])) {
      coro_current_log_offsets[node_id][coro_id] = 0;
    }
    offset_t offset = coro_start_log_offsets[node_id][coro_id] + coro_current_log_offsets[node_id][coro_id];
    coro_current_log_offsets[node_id][coro_id] += log_entry_size;
    return offset;
  }

  void ResetAllLogOffsetCoro(node_id_t node_id, coro_id_t coro_id) {
      //cleaning all previous logs in this coro

      coro_current_log_offsets[node_id][coro_id] = 0;
  }


 private:
  offset_t start_log_offsets[NUM_MEMORY_NODES];
  offset_t end_log_offsets[NUM_MEMORY_NODES];
  offset_t current_log_offsets[NUM_MEMORY_NODES];

  offset_t coro_start_log_offsets[NUM_MEMORY_NODES][MAX_NUM_COROS];
  offset_t coro_end_log_offsets[NUM_MEMORY_NODES][MAX_NUM_COROS];
  offset_t coro_current_log_offsets[NUM_MEMORY_NODES][MAX_NUM_COROS];

  coro_id_t num_coros_per_thread;

};