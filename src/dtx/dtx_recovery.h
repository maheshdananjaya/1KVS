#include "dtx/dtx.h"
//Recovery program
#ifdef RECOVERY 
bool DTX::TxRecovery(coro_yield_t& yield){
    std::vector<DirectRead> pending_direct_ro; 

    //for all keys/hashnodes of all tables, check the lock value.
    //for all tablese

    //const TPCCTableType all_table_types[]= {kWarehouseTable, kDistrictTable, kCustomerTable, kHistoryTable, 
    //                                        kNewOrderTable, kOrderTable, kOrderLineTable, kItemTable, kStockTable, kCustomerIndexTable, kOrderIndexTable}
   //for tpcc
    int total_keys_searched=0;

    const uint64_t all_table_types[]= {48076, 48077, 48078, 48079, 48080, 48081, 48082, 48083, 48084, 48085, 48086};

    struct timespec scan_start_time, scan_end_time;
    clock_gettime(CLOCK_REALTIME, &scan_start_time);

    for (const auto table_id_ : all_table_types){
      int tot_keys=0;
        table_id_t  table_id = (table_id_t)table_id_;
        HashMeta meta = global_meta_man->GetPrimaryHashMetaWithTableID(table_id);

        //this is without batching or parallelism
        for (int bucket_id=0; bucket_id< meta.bucket_num; bucket_id++){
            // key=lock_id/tx_id(key to search). this could be multiple id
            auto obj = std::make_shared<DataItem>(table_id_, 0xffffffff); 
            DataSetItem data_set_item{.item_ptr = std::move(obj), .is_fetched = false, .is_logged = false, .read_which_node = -1};

            std::vector<HashRead> pending_hash_reads;
            
            //Instead of single buckets, we read multiple buckets.
            if(!IssueLockRecoveryReadMultiple(table_id, bucket_id, &data_set_item, pending_hash_reads)) return false;
            
            //one-by-one 
            coro_sched->Yield(yield, coro_id);
            // if the tx_id found or multiple tc ids found. then stop the search.
            //if(!CheckLockRecoveryRead(pending_hash_reads)) continue; 
            int depth=0;

            while(!pending_hash_reads.empty()){
              coro_sched->Yield(yield, coro_id);
              depth++;  
              CheckLockRecoveryRead2(pending_hash_reads);
            }
            
            //while(!CheckLockRecoveryRead2(pending_hash_reads)) {
            //  if(pending_hash_reads.empty()){  
            //    depth++;          

            //    break;
            //  }
            //    coro_sched->Yield(yield, coro_id);
            //} 

            tot_keys+=depth*2;
            //else RDMA_LOG(INFO) << "failed transaction found";
            //if(!CheckLockRecoveryRead(pending_hash_reads)) continue ; 
        }

        //RDMA_LOG(INFO) << "table - keys scanned : " << tot_keys;
        total_keys_searched+=tot_keys;
    //
    }
    clock_gettime(CLOCK_REALTIME, &scan_end_time);
    double scan_time = (scan_end_time.tv_sec - scan_start_time.tv_sec) * 1000000 + (double)(scan_end_time.tv_nsec - scan_start_time.tv_nsec) / 1000;
    RDMA_LOG(INFO) << "total keys scanned : " << total_keys_searched << "with total time spent us=" << scan_time ;
}


//read all hashbuckets from table id. 
bool DTX::IssueLockRecoveryRead(table_id_t table_id, uint64_t bucket_id, DataSetItem* item, std::vector<HashRead>& pending_hash_reads){

    //this could be for loop with recovery set or batching.   

    node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(table_id);
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    //auto offset = addr_cache->Search(remote_node_id, it->table_id, it->key);
    HashMeta meta = global_meta_man->GetPrimaryHashMetaWithTableID(table_id);
    //uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
    offset_t node_off = bucket_id * meta.node_size + meta.base_off;
    char* local_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(HashNode));   
   
    //has to change
    pending_hash_reads.emplace_back(HashRead{.qp = qp, .item = item, .buf = local_hash_node, .remote_node = remote_node_id, .meta = meta});
    

    if (!coro_sched->RDMARead(coro_id, qp, local_hash_node, node_off, sizeof(HashNode))) {
      return false;
    }

}

bool DTX::IssueLockRecoveryReadMultiple(table_id_t table_id, uint64_t bucket_id, DataSetItem* item, std::vector<HashRead>& pending_hash_reads){

    // this could be for loop with recovery set or batching.   
    const int num_buckets_ = 4;
    node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(table_id);
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    //auto offset = addr_cache->Search(remote_node_id, it->table_id, it->key);
    HashMeta meta = global_meta_man->GetPrimaryHashMetaWithTableID(table_id);
    //uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
    offset_t node_off = bucket_id * meta.node_size + meta.base_off;
    char* local_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(HashNode)*num_buckets_);   
   
    //has to change
    pending_hash_reads.emplace_back(HashRead{.qp = qp, .item = item, .buf = local_hash_node, .remote_node = remote_node_id, .meta = meta});
    

    if (!coro_sched->RDMARead(coro_id, qp, local_hash_node, node_off, sizeof(HashNode)*num_buckets_)) {
      return false;
    }
}



//For Latch recovery
bool DTX::IssueLatchLogRecoveryRead(std::vector<HashRead>& pending_hash_reads){

    size_t latch_log_size = sizeof(LatchLogRecord)*100;

    //we need a array here
    char** latch_log;
    for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){
        char* latch_log = thread_rdma_buffer_alloc->Alloc(latch_log_size); // or 512 bytes
    }    

    for (int i = 0; i < global_meta_man->remote_nodes.size(); i++) {
      offset_t log_offset = thread_remote_log_offset_alloc->GetStartLogOffset(i, coro_id);
      RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(i);
    
        //RDMA Read
        if (!coro_sched->RDMARead(coro_id, qp, latch_log, log_offset, latch_log_size)) return false;
    }

    //compare

}

//End Latch Recovery


bool DTX::CheckLockRecoveryRead(std::vector<HashRead>& pending_hash_reads){
    for (auto& res : pending_hash_reads) {
      auto* local_hash_node = (HashNode*)res.buf;
      auto* it = res.item->item_ptr.get(); //original item. key=tx_id
      bool find = false;
      for (auto& item : local_hash_node->data_items) {
        if(item.lock == it->key){ // to find the correct tx id. worse case. 
          find=true;
          return true;
        }

      }
    }

    return false;
}


bool DTX::CheckLockRecoveryRead2(std::vector<HashRead>& pending_hash_reads){
    
    for (auto iter = pending_hash_reads.begin(); iter != pending_hash_reads.end();) {
      auto res = *iter;
      auto* local_hash_node = (HashNode*)res.buf;
      auto* it = res.item->item_ptr.get(); //original item. key=tx_id
      bool find = false;
      for (auto& item : local_hash_node->data_items) {
        if(item.lock == it->key){ // to find the correct tx id. worse case. 
          find=true;
          return true;
        }

      }
      //check for the next pointer. //trim the pending_hash_read vector. add new. 

      if(local_hash_node->next == nullptr){
          //more nodes.
          //pending_hash_reads.emplace_back(HashRead{.qp = qp, .item = item, .buf = local_hash_node, .remote_node = remote_node_id, .meta = meta});
          iter = pending_hash_reads.erase(iter);
          
      }else{
        //i can use the saem buffer spcae. only need to calculate the offset.
        //auto pp = std::addressof(local_hash_node->next);
        auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr + res.meta.base_off; 
        if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off, sizeof(HashNode))) {
          return false; //error
        }       
      }
      iter++;
    }

    return false;
}

bool DTX::CheckLockRecoveryReadMultiple(std::vector<HashRead>& pending_hash_reads){
    
    for (auto iter = pending_hash_reads.begin(); iter != pending_hash_reads.end();) {
      auto res = *iter;
      auto* local_hash_node = (HashNode*)res.buf;

      for(int i =0; i<1;i++){ // for all hashbuckets we ask in the first level.

        auto* it = res.item->item_ptr.get(); //original item. key=tx_id
        bool find = false;
        for (auto& item : local_hash_node->data_items) {
          if(item.lock == it->key){ // to find the correct tx id. worse case. 
            find=true;
            return true;
          }
  
        }
        //check for the next pointer. //trim the pending_hash_read vector. add new.   
        if(local_hash_node->next == nullptr){
            //more nodes.
            //pending_hash_reads.emplace_back(HashRead{.qp = qp, .item = item, .buf = local_hash_node, .remote_node = remote_node_id, .meta = meta});
            iter = pending_hash_reads.erase(iter);
            
        }else{
          //i can use the saem buffer spcae. only need to calculate the offset.
          //auto pp = std::addressof(local_hash_node->next);
          auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr + res.meta.base_off; 
          if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off, sizeof(HashNode))) {
            return false; //error
          }
          iter++;       
        }

      }//hashbucket end
      //iter++;
    }

    return false;
}
#endif

