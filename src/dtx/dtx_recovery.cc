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
        const HashMeta& meta = global_meta_man->GetPrimaryHashMetaWithTableID(table_id);

        //this is without batching or parallelism
        for (int bucket_id=0; bucket_id < meta.bucket_num; bucket_id++){
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
    const HashMeta& meta = global_meta_man->GetPrimaryHashMetaWithTableID(table_id);
    //uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
    offset_t node_off = bucket_id * meta.node_size + meta.base_off;
    char* local_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(HashNode));   
   
    //has to change - 
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
    const HashMeta& meta = global_meta_man->GetPrimaryHashMetaWithTableID(table_id);
    //uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
    offset_t node_off = bucket_id * meta.node_size + meta.base_off;
    char* local_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(HashNode)*num_buckets_);   
   
    //has to change
    pending_hash_reads.emplace_back(HashRead{.qp = qp, .item = item, .buf = local_hash_node, .remote_node = remote_node_id, .meta = meta});
    

    if (!coro_sched->RDMARead(coro_id, qp, local_hash_node, node_off, sizeof(HashNode)*num_buckets_)) {
      return false;
    }
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
        iter++;  
      }
      
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



#ifdef LATCH_RECOVERY

bool DTX::TxLatchRecovery(coro_yield_t& yield){
    IssueUndoLogRecovery(yield);
    //IssueLatchLogRecoveryRead(yield);
}


//DAM - For Latch recovery for all pending transactions.
bool DTX::IssueLatchLogRecoveryRead(coro_yield_t& yield){

    coro_id_t num_coro = thread_remote_log_offset_alloc->GetNumCoro();

    const int MAX_LATCH_LOG_RECORDS = 16;

    size_t latch_log_size = sizeof(LatchLogRecord) * MAX_LATCH_LOG_RECORDS;


    //need to store: all nodes: all coroutines.
    //Log coro [coror] -> node [i]
    char* latch_logs[num_coro][global_meta_man->remote_nodes.size()]; // log buffer
    int num_valid_logs[num_coro]; //filter out last
    int last_valid_log[num_coro]; //filter out last
    bool tx_done [num_coro]; // finished transactions

    //DAM
    //1 record for coro refetch.
    //2. all the keys need to be unlocked. 

    //if not done (i.e. not pruned) we can go through logs actually
    //we need a array here

    char* latch_log;

    for (int c=0 ; c < num_coro ; c++){   
        for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){
            char* latch_log = thread_rdma_buffer_alloc->Alloc(latch_log_size); // or 512 bytes

            latch_logs [c][i] = latch_log; //allocating space;
            offset_t log_offset = thread_remote_log_offset_alloc->GetStartLatchLogOffset(i, coro_id);
            RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(i);
    
            //RDMA Read
            if (!coro_sched->RDMARead(coro_id, qp, latch_log, log_offset, latch_log_size)) return false;

            //+ we can read the undo logs at the same time. or do that later 
            //RDMAREAD - logs.
        }    
    }

    coro_sched->Yield(yield, coro_id); //wait for logs to arrive.

    //we have all latch log records of all nodes //for each coroutine
    for (int c = 0 ; c < num_coro ; c++){   

        //every node //see first two logs and check if they are not negative. //check all logs.
        bool has_started_commit = false; //not useful here.
        uint64_t coro_agreed_tx_id = 0; 

        bool tx_mismatched=false;
        uint64_t index_at_curr_tx_mismatch=0; 
        uint64_t last_commited_tx_id=0; 

        bool last_flag_reached=false;
        bool is_log_records_non_decreasing=false;

        //For each log record
        for(int r=0; r < MAX_LATCH_LOG_RECORDS; r++ ){    

            bool log_received=true;             
            bool tx_id_agreed=true; //same as log_received
            uint64_t curr_agreed_tx_id = 0; // largest of all agreed id. 
            bool last_flagged=true; //in this log record. 
            bool curr_log_matched=true;

            //Each machine
            for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){      

                LatchLogRecord* record =  (LatchLogRecord *)latch_logs [c][i];                  
                //to check if the transaction has been commited or partioal log locks are held. still need to unlock. 
                //Latch log will never be written out of order, assuing truncation
                if(record->tx_id_ < 0){
                    if(r==0){
                        //not even started
                        log_received &= false;  
                        tx_id_agreed &= false;
                        break;            
                    }
                    else{
                        tx_id_agreed &= false;
                        break;
                    }                    
                }
                          
                    //Dam log record                
                
                if(i==0){
                    curr_agreed_tx_id = record->tx_id_;                    
                    continue;
                }

                //TODO - handle (-1) in uncompleted last transactions.unflagged logs. 

                if(curr_agreed_tx_id != record->tx_id_){

                        //TODO- check if the tx if is -1;
                        tx_mismatched = true;
                        curr_log_matched &= false;
                        tx_id_agreed &= false;
                        index_at_curr_tx_mismatch = r; // index mismatch without reaching the last flag can be considered as failed. 
                        curr_agreed_tx_id = (curr_agreed_tx_id > record->tx_id_)? curr_agreed_tx_id : record->tx_id_;
                        
                        //last_commited_tx_id = curr_agreed_tx_id;  
                        break;
                }
                //if tx matched. if the last flag is set. tx has staretd commiting. 
                last_flagged &= (bool)record->tx_id_;  
            }


            if(!tx_id_agreed){
                // simply unlock places and continue. 
                //coro_agreed_tx_id = curr_tx_id;

                //TODO- check if no transactions have started.
                has_started_commit = false;
                break;
            }
            //all last tx_ids agreed. 
            else{

                coro_agreed_tx_id = curr_agreed_tx_id;                

                if(!last_flagged){
                    //continue with the next log record if if
                    assert((r+1) <= MAX_LATCH_LOG_RECORDS);
                    continue;
                }else{

                    //then only we need to read undo
                    //if last flagged, tx ids agrred and last tx_id is not complete. then only undo logs are needed. 

                    //I know all the keys here. i can check directly whether all logs have been recoved.  
                    //I can do this seperately after reading all the latch logs while logs being loaded/prefetched in the background.
                    //then later I will check if all the logs havebeen recoved.
                    has_started_commit = true;
                }

            }          
           //asserts if the log is larges than MAX LOG Size and if so fetch again. 


        } //For each log record 

        //TODO- check negative records.

        if(!has_started_commit); //TODO- unlock al the places and reconfigure.

        //everything is ok;

    }
   

}

//UNdo log recovery: read all logs and in-place updates to see if there is a mismatch.
bool DTX::IssueUndoLogRecovery(coro_yield_t& yield){

    coro_id_t num_coro = thread_remote_log_offset_alloc->GetNumCoro();

    const int MAX_DATA_LOG_RECORDS = 16;

    size_t undo_log_size = sizeof(UndoLogRecord) * MAX_DATA_LOG_RECORDS;
    size_t inplace_update_size = sizeof(DataItem) * MAX_DATA_LOG_RECORDS;

    //need to store: all nodes: all coroutines.
    //Log coro [coror] -> node [i]
    char* undo_logs[num_coro][global_meta_man->remote_nodes.size()]; // log buffer

    char* inplace_updates[num_coro][global_meta_man->remote_nodes.size()]; // log buffer

    int coro_num_valid_logs[num_coro]; //filter out last
    bool coro_has_started_commit [num_coro]; // finished transactions

    int last_valid_log[num_coro]; //filter out last
    
    bool tx_done [num_coro]; // finished transactions


    //read all logs.

    char* undo_log;

    for (int c=0 ; c < num_coro ; c++){   
        for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){
            char* undo_log = thread_rdma_buffer_alloc->Alloc(undo_log_size); // or 512 bytes

            undo_logs [c][i] = undo_log; //allocating space;
            offset_t log_offset = thread_remote_log_offset_alloc->GetStartLogOffset(i, coro_id);
            RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(i);
    
            //RDMA Read
            if (!coro_sched->RDMARead(coro_id, qp, undo_log, log_offset, undo_log_size)) return false;

            //+ we can read the undo logs at the same time. or do that later 
            //RDMAREAD - logs.
        }    
    }

    coro_sched->Yield(yield, coro_id); //wait for logs to arrive.

    for (int c = 0 ; c < num_coro ; c++){   

        //every node //see first two logs and check if they are not negative. //check all logs.
        bool has_started_commit = false; //not useful here.
        uint64_t coro_agreed_tx_id = 0; 
        uint64_t num_valid_logs = 0;

        bool tx_mismatched=false;
        uint64_t index_at_curr_tx_mismatch=0; 
        uint64_t last_commited_tx_id=0; 

        bool last_flag_reached=false;
        bool is_log_records_non_decreasing=false;

        //For each log record
        for(int r=0; r < MAX_DATA_LOG_RECORDS; r++ ){    

            bool log_received=true;             
            bool tx_id_agreed=true; //same as log_received
            uint64_t curr_agreed_tx_id = 0; // largest of all agreed id. 
            bool last_flagged=true; //in this log record. 
            bool curr_log_matched=true;

            //Each machine
            for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){      

                UndoLogRecord* record =  (UndoLogRecord *)undo_logs [c][i];                  
                //to check if the transaction has been commited or partioal log locks are held. still need to unlock. 
                //Latch log will never be written out of order, assuing truncation
                if(record[r].tx_id_ < 0){
                    if(r==0){
                        //not even started
                        log_received &= false;  
                        tx_id_agreed &= false;
                        break;            
                    }
                    else{
                        tx_id_agreed &= false;
                        break;
                    }                    
                }
                          
                    //Dam log record                
                
                if(i==0){
                    curr_agreed_tx_id = record->tx_id_;                    
                    continue;
                }

                //TODO - handle (-1) in uncompleted last transactions.unflagged logs. 

                if(curr_agreed_tx_id != record[r].tx_id_){

                        //TODO- check if the tx if is -1;
                        tx_mismatched = true;
                        curr_log_matched &= false;
                        tx_id_agreed &= false;
                        index_at_curr_tx_mismatch = r; // index mismatch without reaching the last flag can be considered as failed. 
                        curr_agreed_tx_id = (curr_agreed_tx_id > record->tx_id_)? curr_agreed_tx_id : record->tx_id_;
                        
                        //last_commited_tx_id = curr_agreed_tx_id;  
                        break;
                }
                //if tx matched. if the last flag is set. tx has staretd commiting. 
                last_flagged &= (bool)record->tx_id_;  
            }


            if(!tx_id_agreed){
                // simply unlock places and continue. 
                //coro_agreed_tx_id = curr_tx_id;

                //TODO- check if no transactions have started.
                has_started_commit = false;

                //TODO - call lock recovery

                break;
            }
            //all last tx_ids agreed. 
            else{

                coro_agreed_tx_id = curr_agreed_tx_id;                

                if(!last_flagged){
                    //continue with the next log record if if
                    assert((r+1) <= MAX_LATCH_LOG_RECORDS);
                    continue;
                }else{

                    //then only we need to read undo
                    //if last flagged, tx ids agrred and last tx_id is not complete. then only undo logs are needed. 

                    //I know all the keys here. i can check directly whether all logs have been recoved.  
                    //I can do this seperately after reading all the latch logs while logs being loaded/prefetched in the background.
                    //then later I will check if all the logs havebeen recoved.
                    has_started_commit = true;
                    num_valid_logs = r+1; 
                    break;

                }

            }          
           //asserts if the log is larges than MAX LOG Size and if so fetch again. 


        } //For each log record 

        //TODO- check negative records.

        //r is the index

        if(!has_started_commit){
           coro_has_started_commit[c] = has_started_commit; ; //TODO- iinvoke unlocks
        }

        else{

            //TODO - check in-place updates
            coro_num_valid_logs[c] = (int)num_valid_logs;
            coro_has_started_commit[c] = has_started_commit;

            //Iterate through logs (table, key) and read in-place values.

            UndoLogRecord* record_node_0 =  (UndoLogRecord *)undo_logs [c][0]; 

            for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){
                
                char* inplace_update = thread_rdma_buffer_alloc->Alloc(inplace_update_size); // or 512 bytes        
                inplace_updates [c][i] = inplace_update; //allocating space; 

                for (int rc =0 ; rc < num_valid_logs; rc++){  //recorc c

                        DataItem* logged_item= &record_node_0[rc].data_ ; 

                        table_id_t logged_table_id = logged_item->table_id;
                        itemkey_t logged_key = logged_item->key;
                        node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(logged_item->table_id);
                        
                        RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
                        auto offset = addr_cache->Search(remote_node_id, logged_item->table_id, logged_item->key);

                        //assume all logs keys are a cache hit.
                        if (offset != NOT_FOUND) {
                            if (!coro_sched->RDMARead(coro_id, qp, &inplace_update[rc], offset, DataItemSize)) {
                                return false;
                            }

                        } 
                        else{
                            //TODO- report back to me
                            assert(false);
                        }           
                        //+ we can read the undo logs at the same time. or do that later 
                        //RDMAREAD - logs.
                }   

            }

            //check in-place values.
            coro_sched->Yield(yield, coro_id); //wait for all in-place to arrive.

            for (int c = 0 ; c < num_coro ; c++){ 
                
                if(coro_has_started_commit[c]){
                   
                   for(int j=0; j < coro_num_valid_logs[c]; j++){
                        //
                    }

                }
                
            }


        }

    } //everything is ok;

}


//DAM - For Latch recovery for all pending transactions.
//bool DTX::IssueDataLogRecoveryRead(coro_yield_t& yield){
//    //recoering undo logs.


//}

#endif