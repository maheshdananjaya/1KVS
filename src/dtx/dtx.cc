// Author: Ming Zhang
// Copyright (c) 2021

#include "dtx/dtx.h"
//#define LATCH_LOG
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

  //DAM- if latch lock is enabled. using log qps
  #ifdef LATCH_LOG
    if(LatchLog()) {

      RDMA_LOG(DBG) << "coro: " << coro_id << " waiting for latch logs";

      coro_sched->Yield(yield, coro_id, true); 
      //Critical error when scheduling at scheduler with append coroutines.+ can overlaps with undo logs. deprecated. 
      while (!coro_sched->CheckLogAck(coro_id));     

      RDMA_LOG(DBG) << "coro: " << coro_id << " latch log -- SUCCESSFUL";
    }         

  #endif


  //if(LatchLogDataQP()){
  //  coro_sched->Yield(yield, coro_id);
  //}

  //new latch logging using data qps

  if (!IssueReadOnly(pending_direct_ro, pending_hash_ro)) return false;  // RW transactions may also have RO data
  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " issue read rorw";

  //DAM: changed  this to avoid read+locks?
  if (!IssueReadLock(pending_cas_rw, pending_hash_rw, pending_insert_off_rw)) return false;

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " check read rorw";
  auto res = CheckReadRORW(pending_direct_ro, pending_hash_ro, pending_hash_rw, pending_insert_off_rw, pending_cas_rw,
                           pending_invisible_ro, pending_next_hash_ro, pending_next_hash_rw, pending_next_off_rw, yield);
  
  
  //ParallelUndoLog();
  //DAM - per coroutine log buffer.

  //UndoLog();  

  //DAM. redo

  return res;


}


bool DTX::Validate(coro_yield_t& yield) {
  // The transaction is write-only, and the data are locked before
  if (not_eager_locked_rw_set.empty() && read_only_set.empty()) return true;


  std::vector<ValidateRead> pending_validate;

  if (!IssueValidate(pending_validate)) return false;

  //DAM-missing inserts logging after locking non-eager write set (i.e. inserts).
  
  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  auto res = CheckValidate(pending_validate);

  //taking undo logs: table, key, version for inserts as well. TODO- avoid logging inserts.
  //TODO: this has to be done only if the validations is sucessful.
  
  //UndoLog();

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


void DTX::ParallelUndoLogIncludingInserts() {

  // Write the old data from read write set
  size_t log_size = sizeof(tx_id) + sizeof(t_id);
  for (auto& set_it : read_write_set) {
    if (!set_it.is_logged) {
      // For the newly inserted data, the old data are not needed to be recorded
      log_size += DataItemSize;
      //set_it.is_logged = true;
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


//DAM Log API- undo log before locking. log-> log. we can use data qp to write logs. 
//TODO: input pending logs. isolating failures.
//logging inserts included
bool DTX::UndoLog() {

  // Write the old data from read write set
  //DAM- t_id is unnecssary. it is used to flag the last log entry. and /or first log entry.
  // if we can give the number of potential log entries at the beggining, size can be written to the first tid
  
  size_t log_record_size = sizeof(tx_id)+sizeof(t_id)+DataItemSize;
  // this does not include inserts as they get locked suring the validation phase. 
  // This works with seperate locking as well.

  size_t num_log_entries = 0;
   //DAM- TODO craefull all the inserts are logged without locking. wait for the validation.
  for (auto& set_it : read_write_set) { 

    if (!set_it.is_logged) num_log_entries++;
  }

  if(num_log_entries == 0) return false;

  size_t log_size = log_record_size*num_log_entries;
  char* written_log_buf = thread_rdma_buffer_alloc->Alloc(log_size);
  offset_t cur = 0;

  for (auto& set_it : read_write_set) { 

    if (!set_it.is_logged) {           
      std::memcpy(written_log_buf + cur, &tx_id, sizeof(tx_id));
      cur += sizeof(tx_id);
      std::memcpy(written_log_buf + cur, &t_id, sizeof(t_id));
      cur += sizeof(t_id);
      std::memcpy(written_log_buf + cur, set_it.item_ptr.get(), DataItemSize);
      cur += DataItemSize;

      set_it.is_logged = true;   

    }
  }
  // Write undo logs to all memory nodes. ibv send send the offset relative to the memory region.
  for (int i = 0; i < global_meta_man->remote_nodes.size(); i++) {
    offset_t log_offset = thread_remote_log_offset_alloc->GetNextLogOffset(i, coro_id, log_size);
    RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(i);

    //TODO- log records are without Acks.
    coro_sched->RDMALog(coro_id, tx_id, qp, (char*)written_log_buf, log_offset, log_size);
  }

  return true;

}

//DAM - without inserts
bool DTX::UndoLogWithoutInserts() {

  // Write the old data from read write set
  //DAM- t_id is unnecssary. it is used to flag the last log entry. and /or first log entry.
  // if we can give the number of potential log entries at the beggining, size can be written to the first tid
  
  size_t log_record_size = sizeof(tx_id)+sizeof(t_id)+DataItemSize;
  // this does not include inserts as they get locked suring the validation phase. 
  // This works with seperate locking as well.

  size_t num_log_entries = 0;
   //DAM- TODO craefull all the inserts are logged without locking. wait for the validation.
  for (auto& set_it : read_write_set) { 

    if (!set_it.is_logged  && !set_it.item_ptr->user_insert ) num_log_entries++;
  }

  if(num_log_entries == 0) return false;

  size_t log_size = log_record_size*num_log_entries;
  char* written_log_buf = thread_rdma_buffer_alloc->Alloc(log_size);
  offset_t cur = 0;

  for (auto& set_it : read_write_set) { 

    if (!set_it.is_logged && !set_it.item_ptr->user_insert) {           
      std::memcpy(written_log_buf + cur, &tx_id, sizeof(tx_id));
      cur += sizeof(tx_id);
      std::memcpy(written_log_buf + cur, &t_id, sizeof(t_id));
      cur += sizeof(t_id);
      std::memcpy(written_log_buf + cur, set_it.item_ptr.get(), DataItemSize);
      cur += DataItemSize;

      set_it.is_logged = true;   

    }
  }

  // Write undo logs to all memory nodes. ibv send send the offset relative to the memory region.
  for (int i = 0; i < global_meta_man->remote_nodes.size(); i++) {
    offset_t log_offset = thread_remote_log_offset_alloc->GetNextLogOffset(i, coro_id, log_size);
    RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(i);

    //TODO- log records are without Acks.
    coro_sched->RDMALog(coro_id, tx_id, qp, (char*)written_log_buf, log_offset, log_size);
  }

  return true;

}

//DDAM Inserts only
bool DTX::UndoLogInsertsOnly() {

  // Write the old data from read write set
  //DAM- t_id is unnecssary. it is used to flag the last log entry. and /or first log entry.
  // if we can give the number of potential log entries at the beggining, size can be written to the first tid
  
  size_t log_record_size = sizeof(tx_id)+sizeof(t_id)+DataItemSize;
  // this does not include inserts as they get locked suring the validation phase. 
  // This works with seperate locking as well.

  size_t num_log_entries = 0;
   //DAM- TODO craefull all the inserts are logged without locking. wait for the validation.
  for (auto& set_it : read_write_set) { 

    if (!set_it.is_logged  && set_it.item_ptr->user_insert ) num_log_entries++;
  }

  if(num_log_entries == 0) return false;

  size_t log_size = log_record_size*num_log_entries;
  char* written_log_buf = thread_rdma_buffer_alloc->Alloc(log_size);
  offset_t cur = 0;

  for (auto& set_it : read_write_set) { 

    if (!set_it.is_logged && set_it.item_ptr->user_insert) {           
      std::memcpy(written_log_buf + cur, &tx_id, sizeof(tx_id));
      cur += sizeof(tx_id);
      std::memcpy(written_log_buf + cur, &t_id, sizeof(t_id));
      cur += sizeof(t_id);
      std::memcpy(written_log_buf + cur, set_it.item_ptr.get(), DataItemSize);
      cur += DataItemSize;

      set_it.is_logged = true;   

    }
  }

  // Write undo logs to all memory nodes. ibv send send the offset relative to the memory region.
  for (int i = 0; i < global_meta_man->remote_nodes.size(); i++) {
    offset_t log_offset = thread_remote_log_offset_alloc->GetNextLogOffset(i, coro_id, log_size);
    RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(i);

    //TODO- log records are without Acks.
    coro_sched->RDMALog(coro_id, tx_id, qp, (char*)written_log_buf, log_offset, log_size);
  }

  return true;

}

//DAM  - latch logging with logQPs
bool DTX::LatchLog() {

  size_t log_record_size = sizeof(tx_id)+sizeof(t_id)+sizeof(itemkey_t);
  // this does not include inserts as they get locked suring the validation phase. 
  // This works with seperate locking as well.

  size_t num_log_entries = 0;
  for (auto& set_it : read_write_set) { 

    if (!set_it.is_logged) num_log_entries++;
  }

  if(num_log_entries == 0) return false;

  size_t log_size = log_record_size*num_log_entries;
  char* written_log_buf = thread_rdma_buffer_alloc->Alloc(log_size);
  offset_t cur = 0;

  for (auto& set_it : read_write_set) { 

    if (!set_it.is_logged) {           
      std::memcpy(written_log_buf + cur, &tx_id, sizeof(tx_id));
      cur += sizeof(tx_id);
      std::memcpy(written_log_buf + cur, &t_id, sizeof(t_id));
      cur += sizeof(t_id);
      //logging the key
      std::memcpy(written_log_buf + cur, &((set_it.item_ptr.get())->key), sizeof(itemkey_t));
      cur +=  sizeof(itemkey_t);

      //DAM- fix this prevents taking undo later
      //set_it.is_logged = true;   

    }
  }

  // Write undo logs to all memory nodes. ibv send send the offset relative to the memory region.
  for (int i = 0; i < global_meta_man->remote_nodes.size(); i++) {
    offset_t log_offset = thread_remote_log_offset_alloc->GetNextLogOffset(i, coro_id, log_size);
    RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(i);

    //TODO- log records are without Acks.
    coro_sched->RDMALog(coro_id, tx_id, qp, (char*)written_log_buf, log_offset, log_size, true);
  }

   return true;

}


//DAM -latch logs with Data QPs
bool DTX::LatchLogDataQP() {

  size_t log_record_size = sizeof(tx_id)+sizeof(t_id)+sizeof(itemkey_t);
  // this does not include inserts as they get locked suring the validation phase. 
  // This works with seperate locking as well.

  size_t num_log_entries = 0;
  for (auto& set_it : read_write_set) { 

    if (!set_it.is_logged) num_log_entries++;
  }

  if(num_log_entries == 0) return false;

  size_t log_size = log_record_size*num_log_entries;
  char* written_log_buf = thread_rdma_buffer_alloc->Alloc(log_size);
  offset_t cur = 0;

  for (auto& set_it : read_write_set) { 

    if (!set_it.is_logged) {           
      std::memcpy(written_log_buf + cur, &tx_id, sizeof(tx_id));
      cur += sizeof(tx_id);
      std::memcpy(written_log_buf + cur, &t_id, sizeof(t_id));
      cur += sizeof(t_id);
      //logging the key
      std::memcpy(written_log_buf + cur, &((set_it.item_ptr.get())->key), sizeof(itemkey_t));
      cur +=  sizeof(itemkey_t);

      //DAM- fix this prevents taking undo later
      //set_it.is_logged = true;   
    }
  }

  // Write undo logs to all memory nodes. ibv send send the offset relative to the memory region.
  for (int i = 0; i < global_meta_man->remote_nodes.size(); i++) {
    offset_t log_offset = thread_remote_log_offset_alloc->GetNextLatchLogOffset(i, coro_id, log_size);
    //DAM -using data qps to write latch logs.
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(i);

    //TODO- log records are without Acks.
    //coro_sched->RDMALog(coro_id, tx_id, qp, (char*)written_log_buf, log_offset, log_size, true);
    coro_sched->RDMAWrite(coro_id, qp, (char*)written_log_buf, log_offset, log_size);
  }

   return true;

}

//writing and clearning the old log entry.
void DTX::PruneLog() {

  //wait for the acks of last trsnaction unlocks. then truncate.
  for (int i = 0; i < global_meta_man->remote_nodes.size(); i++) {
    thread_remote_log_offset_alloc->ResetAllLogOffsetCoro(i, coro_id);  
  }

  //TODO: should we wait for the next transaction to overide it or should we proactively truncate logs.
  //later log can be partially logged. so cant only check the first entry.

}

void DTX:: Recovery(){
  //Assuming that coompute fails/
  //two conditions: if the tx id is non -1. or if the tx_id is non incremental. 
  //new data strutcures to read the log buffer. and search it and recover.

  //lets recover one coroutine firts.then we can loop.
  
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