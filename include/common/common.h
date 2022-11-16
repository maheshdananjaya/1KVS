// Author: Ming Zhang
// Copyright (c) 2021

#pragma once

#include <cstddef>  // For size_t
#include <cstdint>  // For uintxx_t

// Global specification
using tx_id_t = uint64_t;     // Transaction id type
using t_id_t = uint32_t;      // Thread id type
using coro_id_t = int;        // Coroutine id type
using node_id_t = int;        // Machine id type
using mr_id_t = int;          // Memory region id type
using table_id_t = uint64_t;  // Table id type
using itemkey_t = uint64_t;   // Data item key type, used in DB tables
using offset_t = int64_t;     // Offset type. Usually used in remote offset for RDMA
using version_t = uint64_t;   // Version type, used in version checking
using lock_t = uint64_t;      // Lock type, used in remote locking

// Memory region ids for server's hash store buffer and undo log buffer
const mr_id_t SERVER_HASH_BUFF_ID = 97;
const mr_id_t SERVER_LOG_BUFF_ID = 98;

// Memory region ids for client's local_mr
const mr_id_t CLIENT_MR_ID = 100;

// Indicating that memory store metas have been transmitted
const uint64_t MEM_STORE_META_END = 0xE0FF0E0F;

// Max data item size.
// 8: smallbank
// 40: tatp
// 664: tpcc
// 40: micro-benchmark
const size_t MAX_ITEM_SIZE = 664;

// Node and thread conf
#define BACKUP_DEGREE 1          // Backup memory node number. MUST **NOT** BE SET TO 0
#define MAX_REMOTE_NODE_NUM 100  // Max remote memory node number

// Data state
#define STATE_INVISIBLE 0x8000000000000000  // Data cannot be read
#define STATE_LOCKED 1                      // Data cannot be written. Used for serializing transactions
#define STATE_CLEAN 0

// Alias
#define Aligned8 __attribute__((aligned(8)))
#define ALWAYS_INLINE inline __attribute__((always_inline))
#define TID (std::this_thread::get_id())

// Helpful for improving condition prediction hit rate
#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x) __builtin_expect(!!(x), 1)


#define STATS
typedef struct atomic_record{
  uint64_t txs;
  double usecs;
} REC;