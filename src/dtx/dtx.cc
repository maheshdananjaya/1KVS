// Author: Ming Zhang
// Copyright (c) 2021

#include "dtx/dtx.h"

DTX::DTX(MetaManager* meta_man,
         QPManager* qp_man,
         t_id_t tid,
         coro_id_t coroid,
         CoroutineScheduler* sched,
         RDMABufferAllocator* rdma_buffer_allocator,
         LogOffsetAllocator* remote_log_offset_allocator,
         AddrCache* addr_buf) {
  // Transaction setup
  tx_id = 0;
  t_id = tid;
  coro_id = coroid;
  coro_sched = sched;
  global_meta_man = meta_man;
  thread_qp_man = qp_man;
  thread_rdma_buffer_alloc = rdma_buffer_allocator;
  tx_status = TXStatus::TX_INIT;

  select_backup = 0;
  thread_remote_log_offset_alloc = remote_log_offset_allocator;
  addr_cache = addr_buf;

  hit_local_cache_times = 0;
  miss_local_cache_times = 0;
}


#ifdef RECOVERY 
bool DTX::TxRecovery(coro_yield_t& yield){
    std::vector<DirectRead> pending_direct_ro; 

    //for all keys/hashnodes of all tables, check the lock value.
    //for all tablese

    //const TPCCTableType all_table_types[]= {kWarehouseTable, kDistrictTable, kCustomerTable, kHistoryTable, 
    //                                        kNewOrderTable, kOrderTable, kOrderLineTable, kItemTable, kStockTable, kCustomerIndexTable, kOrderIndexTable}
   //for tpcc
    
    const uint64_t all_table_types[]= {48076, 48077, 48078, 48079, 48080, 48081, 48082, 48083, 48084, 48085, 48086};

    for (const auto table_id_ : all_table_types){
        table_id_t  table_id = (table_id_t)table_id_;
        HashMeta meta = global_meta_man->GetPrimaryHashMetaWithTableID(table_id);

        //this is without batching or parallelism
        for (int bucket_id=0; bucket_id< meta.bucket_num; bucket_id++){
            // key=lock_id/tx_id(key to search). this could be multiple id
            auto obj = std::make_shared<DataItem>(table_id_, 0xffffffff); 
            DataSetItem data_set_item{.item_ptr = std::move(obj), .is_fetched = false, .is_logged = false, .read_which_node = -1};

            std::vector<HashRead> pending_hash_reads;
            if(!IssueLockRecoveryRead(table_id, bucket_id, &data_set_item, pending_hash_reads)) return false;
            //one-by-one 
            coro_sched->Yield(yield, coro_id);
            // if the tx_id found or multiple tc ids found. then stop the search.
            if(!CheckLockRecoveryRead(pending_hash_reads)) continue ; 
        }
    //
    }
}
#endif

bool DTX::ExeRO(coro_yield_t& yield) {
  // You can read from primary or backup
  std::vector<DirectRead> pending_direct_ro;
  std::vector<HashRead> pending_hash_ro;

  // Issue reads
  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " issue read ro";
  if (!IssueReadOnly(pending_direct_ro, pending_hash_ro)) return false;

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  // Receive data
  std::list<InvisibleRead> pending_invisible_ro;
  std::list<HashRead> pending_next_hash_ro;
  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " check read ro";
  auto res = CheckReadRO(pending_direct_ro, pending_hash_ro, pending_invisible_ro, pending_next_hash_ro, yield);
  return res;
}

bool DTX::ExeRW(coro_yield_t& yield) {
  is_ro_tx = false;
  // You can read from primary or backup

  // For read-only data
  std::vector<DirectRead> pending_direct_ro;
  std::vector<HashRead> pending_hash_ro;

  // For read-write data
  std::vector<CasRead> pending_cas_rw;
  std::vector<HashRead> pending_hash_rw;
  std::vector<InsertOffRead> pending_insert_off_rw;

  std::list<InvisibleRead> pending_invisible_ro;
  std::list<HashRead> pending_next_hash_ro;
  std::list<HashRead> pending_next_hash_rw;
  std::list<InsertOffRead> pending_next_off_rw;

  if (!IssueReadOnly(pending_direct_ro, pending_hash_ro)) return false;  // RW transactions may also have RO data
  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " issue read rorw";

  //DAM: changed  this to avoid read+locks?
  if (!IssueReadLock(pending_cas_rw, pending_hash_rw, pending_insert_off_rw)) return false;

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " check read rorw";
  auto res = CheckReadRORW(pending_direct_ro, pending_hash_ro, pending_hash_rw, pending_insert_off_rw, pending_cas_rw,
                           pending_invisible_ro, pending_next_hash_ro, pending_next_hash_rw, pending_next_off_rw, yield);
  
  ParallelUndoLog();

  //DAM. redo

  return res;

}


bool DTX::Validate(coro_yield_t& yield) {
  // The transaction is write-only, and the data are locked before
  if (not_eager_locked_rw_set.empty() && read_only_set.empty()) return true;


  std::vector<ValidateRead> pending_validate;

  if (!IssueValidate(pending_validate)) return false;

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  auto res = CheckValidate(pending_validate);
  return res;
}

// Invisible + write primary and backups
bool DTX::CoalescentCommit(coro_yield_t& yield) {
  tx_status = TXStatus::TX_COMMIT;
  char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
  *(lock_t*)cas_buf = STATE_LOCKED | STATE_INVISIBLE;

  std::vector<CommitWrite> pending_commit_write;

  // Check whether all the log ACKs have returned
  while (!coro_sched->CheckLogAck(coro_id)) {
    ;
  }

  if (!IssueCommitAll(pending_commit_write, cas_buf)) return false;

  coro_sched->Yield(yield, coro_id);

  *((lock_t*)cas_buf) = 0;

  auto res = CheckCommitAll(pending_commit_write, cas_buf);

  return res;
}

void DTX::ParallelUndoLog() {
  // Write the old data from read write set
  size_t log_size = sizeof(tx_id) + sizeof(t_id);
  for (auto& set_it : read_write_set) {
    if (!set_it.is_logged && !set_it.item_ptr->user_insert) {
      // For the newly inserted data, the old data are not needed to be recorded
      log_size += DataItemSize;
      set_it.is_logged = true;
    }
  }
  char* written_log_buf = thread_rdma_buffer_alloc->Alloc(log_size);

  offset_t cur = 0;
  std::memcpy(written_log_buf + cur, &tx_id, sizeof(
    tx_id));
  cur += sizeof(tx_id);
  std::memcpy(written_log_buf + cur, &t_id, sizeof(t_id));
  cur += sizeof(t_id);

  for (auto& set_it : read_write_set) {
    if (!set_it.is_logged && !set_it.item_ptr->user_insert) {
      std::memcpy(written_log_buf + cur, set_it.item_ptr.get(), DataItemSize);
      cur += DataItemSize;
      set_it.is_logged = true;
    }
  }

  // Write undo logs to all memory nodes
  for (int i = 0; i < global_meta_man->remote_nodes.size(); i++) {
    offset_t log_offset = thread_remote_log_offset_alloc->GetNextLogOffset(i, log_size);
    RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(i);
    coro_sched->RDMALog(coro_id, tx_id, qp, (char*)written_log_buf, log_offset, log_size);
  }
}

void DTX::Abort(coro_yield_t& yield) {
  // When failures occur, transactions need to be aborted.
  // In general, the transaction will not abort during committing replicas if no hardware failure occurs
  char* unlock_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
  *((lock_t*)unlock_buf) = 0;
  for (auto& index : locked_rw_set) {
    auto& it = read_write_set[index].item_ptr;
    node_id_t primary_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
    RCQP* primary_qp = thread_qp_man->GetRemoteDataQPWithNodeID(primary_node_id);
    auto rc = primary_qp->post_send(IBV_WR_RDMA_WRITE, unlock_buf, sizeof(lock_t), it->GetRemoteLockAddr(), 0);
    if (rc != SUCC) {
      RDMA_LOG(FATAL) << "Thread " << t_id << " , Coroutine " << coro_id << " unlock fails during abortion";
    }
  }
  tx_status = TXStatus::TX_ABORT;
}