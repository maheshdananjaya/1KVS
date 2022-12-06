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



#if defined(LATCH_RECOVERY) || defined(UNDO_RECOVERY)

bool DTX::TxLatchRecovery(coro_yield_t& yield, AddrCache ** addr_caches){
    //IssueUndoLogRecovery(yield);
    //IssueLatchLogRecoveryRead(yield);
    IssueLatchLogRecoveryReadForAllThreads(yield, addr_caches);
}

bool DTX::TxUndoRecovery(coro_yield_t& yield, AddrCache ** addr_caches){
    //IssueUndoLogRecovery(yield);
    UpdatedIssueUndoLogRecoveryForAllThreads(yield, addr_caches);
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

    for (int c=1 ; c < num_coro ; c++){   
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
    for (int c = 1 ; c < num_coro ; c++){   

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
                    curr_agreed_tx_id = record[r].tx_id_;                    
                    continue;
                }

                //TODO - handle (-1) in uncompleted last transactions.unflagged logs. 

                if(curr_agreed_tx_id != record[r].tx_id_){

                        //TODO- check if the tx if is -1;
                        tx_mismatched = true;
                        curr_log_matched &= false;
                        tx_id_agreed &= false;
                        index_at_curr_tx_mismatch = r; // index mismatch without reaching the last flag can be considered as failed. 
                        curr_agreed_tx_id = (curr_agreed_tx_id > record[r].tx_id_)? curr_agreed_tx_id : record[r].tx_id_;
                        
                        //last_commited_tx_id = curr_agreed_tx_id;  
                        break;
                }
                //if tx matched. if the last flag is set. tx has staretd commiting. 
                last_flagged &= (bool)record[r].t_id_;  
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
    bool coro_has_started_commit[num_coro]; // finished transactions

    int last_valid_log[num_coro]; //filter out last
    
    bool tx_done[num_coro]; // finished transactions


    //read all logs.

    //char* undo_log;
    
       for (int c=1 ; c < num_coro ; c++){   
           for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){
               char* undo_log = thread_rdma_buffer_alloc->Alloc(undo_log_size); // or 512 bytes

               undo_logs [c][i] = undo_log; //allocating space;
               offset_t log_offset = thread_remote_log_offset_alloc->GetStartLogOffset(i, coro_id); // 
               RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(i);
       
               //RDMA Read
               if (!coro_sched->RDMARead(coro_id, qp, undo_log, log_offset, undo_log_size)) return false;

               //+ we can read the undo logs at the same time. or do that later 
               //RDMAREAD - logs.
           }    
       }
    

        coro_sched->Yield(yield, coro_id); //wait for logs to arrive.
        while (!coro_sched->CheckLogAck(coro_id)){
            //wait for a 1 micro second
        }    

    for (int c = 1 ; c < num_coro ; c++){   

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
                    curr_agreed_tx_id = record[r].tx_id_;                    
                    continue;
                }

                //TODO - handle (-1) in uncompleted last transactions.unflagged logs. 

                if(curr_agreed_tx_id != record[r].tx_id_){

                        //TODO- check if the tx if is -1;
                        tx_mismatched = true;
                        curr_log_matched &= false;
                        tx_id_agreed &= false;
                        index_at_curr_tx_mismatch = r; // index mismatch without reaching the last flag can be considered as failed. 
                        curr_agreed_tx_id = (curr_agreed_tx_id > record[r].tx_id_)? curr_agreed_tx_id : record[r].tx_id_;
                        
                        //last_commited_tx_id = curr_agreed_tx_id;  
                        break;
                }
                //if tx matched. if the last flag is set. tx has staretd commiting. 
                last_flagged &= (bool)record[r].t_id_;  
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
                    assert((r+1) <= MAX_DATA_LOG_RECORDS);
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
            //coro_sched->Yield(yield, coro_id); //wait for all in-place to arrive.
            //
            //for (int c = 1 ; c < num_coro ; c++){ 
            //    
            //    if(coro_has_started_commit[c]){
            //       
            //       for(int j=0; j < coro_num_valid_logs[c]; j++){
            //            RDMA_LOG(INFO) << "recovery coro  - " << c;  
            //        }
            //
            //    }
            //    
            //}


        }

    } //everything is ok; for every cortutine/

    //check in-place values.
    coro_sched->Yield(yield, coro_id); //wait for all in-place to arrive.

            for (int c = 1 ; c < num_coro ; c++){ 
                
                if(coro_has_started_commit[c]){
                   
                   for(int j=0; j < coro_num_valid_logs[c]; j++){
                        RDMA_LOG(INFO) << "recovery coro  - " << c;  
                        //Check versions ids for the moment. 
                        //versions checks. 
                        //
                    }

                }
                
            }


}





//DAM - For Latch recovery for all pending transactions.
bool DTX::IssueLatchLogRecoveryReadForAllThreads(coro_yield_t& yield, AddrCache  ** addr_caches){

    coro_id_t num_coro = thread_remote_log_offset_alloc->GetNumCoro();
    t_id_t num_thread = thread_remote_log_offset_alloc->GetNumThreadsPerMachine();

    const int MAX_LATCH_LOG_RECORDS = 16;

    size_t latch_log_size = sizeof(LatchLogRecord) * MAX_LATCH_LOG_RECORDS;


    //need to store: all nodes: all coroutines.
    //Log coro [coror] -> node [i]
    char* latch_logs[num_thread][num_coro][global_meta_man->remote_nodes.size()]; // log buffer
    int coro_num_valid_logs[num_thread][num_coro]; //filter out last
    int last_valid_log[num_thread][num_coro]; //filter out last
    bool tx_done [num_thread][num_coro]; // finished transactions

    //DAM
    //1 record for coro refetch.
    //2. all the keys need to be unlocked. 

    //if not done (i.e. not pruned) we can go through logs actually
    //we need a array here

    //char* latch_log;

    for(int t=0; t<num_thread;t++){    
    for (int c=1 ; c < num_coro ; c++){   
        for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){
            char* latch_log = thread_rdma_buffer_alloc->Alloc(latch_log_size); // or 512 bytes

            latch_logs [t][c][i] = latch_log; //allocating space;

            //FIXED
            offset_t log_offset = thread_remote_log_offset_alloc->GetStartLatchLogOffsetForThread(i, c, t); //error c not coro_id
            RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(i);
    
            //RDMA Read
            if (!coro_sched->RDMARead(coro_id, qp, latch_log, log_offset, latch_log_size)) return false;

            //+ we can read the undo logs at the same time. or do that later 
            //RDMAREAD - logs.
            }    
        }
    }

    //coro_sched->Yield(yield, coro_id); //wait for logs to arrive.
    while(!coro_sched->CheckRecoveryDataAck(coro_id));

    //we have all latch log records of all nodes //for each coroutine
    for(int t=0; t<num_thread;t++){ 
    for (int c = 1 ; c < num_coro ; c++){   

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
        for(int r=0; r < MAX_LATCH_LOG_RECORDS; r++ ){    

            bool log_received=true;             
            bool tx_id_agreed=true; //same as log_received
            uint64_t curr_agreed_tx_id = 0; // largest of all agreed id. 
            bool last_flagged=true; //in this log record. 
            bool curr_log_matched=true;

            //Each machine.
            //TODO- only primary is enough if logs are recorded in the same memory node.
            for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){      

                LatchLogRecord* record =  (LatchLogRecord *)latch_logs [t][c][i];                  
                //to check if the transaction has been commited or partioal log locks are held. still need to unlock. 
                //Latch log will never be written out of order, assuing truncation
                if(record[r].tx_id_ <= 0){
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
                          
                    //Dam log record.                
                
                if(i==0){
                    curr_agreed_tx_id = record[r].tx_id_;                    
                    continue;
                }

                //TODO - handle (-1) in uncompleted last transactions.unflagged logs. 

                if(curr_agreed_tx_id != record[r].tx_id_){

                        //TODO- check if the tx if is -1;
                        tx_mismatched = true;
                        curr_log_matched &= false;
                        tx_id_agreed &= false;
                        index_at_curr_tx_mismatch = r; // index mismatch without reaching the last flag can be considered as failed. 
                        curr_agreed_tx_id = (curr_agreed_tx_id > record[r].tx_id_)? curr_agreed_tx_id : record[r].tx_id_;
                        
                        //last_commited_tx_id = curr_agreed_tx_id;  
                        break;
                }
                //if tx matched. if the last flag is set. tx has staretd commiting. 
                last_flagged &= (bool)record[r].t_id_;  
            }


            if(!tx_id_agreed){
                // simply unlock places and continue. 
                //coro_agreed_tx_id = curr_tx_id;

                //TODO- check if no transactions have started.
                //has_started_commit = false;
                //break;

                //tight implementation where all locks of the last transaction is set regardless of the last flagged. 
                if(r==0){
                    has_started_commit = false;
                }
                else
                {
                    //do not care about the last flagged. do recovery regardles of last falgged. no harm here. a bit conservative.
                    has_started_commit = true;
                    num_valid_logs = r;
                    //coro_agreed_tx_id is same as the last one.
                } 
                break;
            }
            
            //all last tx_ids agreed. 

            else{


                 if(curr_agreed_tx_id < coro_agreed_tx_id){
                        //canot be larger. assert. we write logs in order. cannt get reordered.
                        has_started_commit = true;
                        num_valid_logs = r;  // remove the last log recored. which is old.
                        break;
                 }

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

                //Send unlock messages here.  send CAS to check the process ids and unlock if the lock value matches the failed IDs.
                LatchLogRecord* rec =  (LatchLogRecord *)latch_logs [t][c][0];
                table_id_t logged_table_id =  rec[r].table_id_;
                itemkey_t logged_key = rec[r].key_;

                node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(logged_table_id);                            
                RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
                

                //FIXED auto offset = addr_cache->Search(remote_node_id, logged_table_id, logged_key);
                auto offset = addr_caches[t]->Search(remote_node_id, logged_table_id, logged_key);

                char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));

                //check the current lock value. if ots equal to the failed tx id or node id. then kill.
    
                            //assume all logs keys are a cache hit.
                   if (offset != NOT_FOUND) {
                       //TODO - pointers are 
                    offset += sizeof(table_id_t)+sizeof(size_t) + sizeof(itemkey_t)+ sizeof(offset_t)+sizeof(version_t)+sizeof(lock_t); 

                    //RDMACAS(coro_id_t coro_id, RCQP* qp, char* local_buf, uint64_t remote_offset, uint64_t compare, uint64_t swap)
                    //TODO - STATE_LOCKED must be 
                    if (!coro_sched->RDMACAS(coro_id, qp, cas_buf, offset, STATE_LOCKED, STATE_CLEAN)) {
                       //if (!coro_sched->RDMARead(coro_id, qp, &inplace_update[rc], offset, DataItemSize)) {
                           return false;
                       }
                    } 
                    else{
                       //TODO- report back to me
                       

                       //assert(false);
                       //report  back issues.
                   }            
                
               
                //

            }          
           //asserts if the log is larges than MAX LOG Size and if so fetch again. 


        } //For each log record 

        //TODO- check negative records.

        if(!has_started_commit){

        } //TODO- unlock al the places and reconfigure.

        else{
            //coro_num_valid_logs
            //send unlock operations for all log records. 
            
            //or we can send unlock aftr each record. its also fine.
        }

        //everything is ok;

    } // coros



    
    }// therads

    while(!coro_sched->CheckRecoveryDataAck(coro_id));

    /*  for (int t=0; t<num_thread; t++){           

            for (int c = 1 ; c < num_coro ; c++){ 
                RDMA_LOG(INFO) << "recovery coro  - " << c;  
                
                if(coro_has_started_commit[t][c]){
                    RDMA_LOG(INFO) << " --> TX has started commit  - " << c; 


                }
            }
    }
    */


   

}

//UNdo log recovery: read all logs and in-place updates to see if there is a mismatch.
bool DTX::IssueUndoLogRecoveryForAllThreads(coro_yield_t& yield){

    coro_id_t num_coro = thread_remote_log_offset_alloc->GetNumCoro();
    t_id_t num_thread = thread_remote_log_offset_alloc->GetNumThreadsPerMachine();

    const int MAX_DATA_LOG_RECORDS = 16;

    size_t undo_log_size = sizeof(UndoLogRecord) * MAX_DATA_LOG_RECORDS;
    size_t inplace_update_size = sizeof(DataItem) * MAX_DATA_LOG_RECORDS;

    //need to store: all nodes: all coroutines.
    //Log coro [coror] -> node [i]
    char* undo_logs[num_thread][num_coro][global_meta_man->remote_nodes.size()]; // log buffer

    char* inplace_updates[num_thread][num_coro][global_meta_man->remote_nodes.size()]; // log buffer

    int coro_num_valid_logs[num_thread][num_coro]; //filter out last
    bool coro_has_started_commit [num_thread][num_coro]; // finished transactions

    int last_valid_log [num_thread][num_coro]; //filter out last
    
    bool tx_done [num_thread][num_coro]; // finished transactions


    //read all logs.

    //char* undo_log;

    for (int t=0; t<num_thread; t++){
        for (int c=1 ; c < num_coro ; c++){   
            for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){
                char* undo_log = thread_rdma_buffer_alloc->Alloc(undo_log_size); // or 512 bytes
    
                undo_logs [t][c][i] = undo_log; //allocating space;

                //FIXED 
                offset_t log_offset = thread_remote_log_offset_alloc->GetStartLogOffsetForThread(i, c, t); // c not coro_id
                RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(i);
        
                //RDMA Read
                if (!coro_sched->RDMARead(coro_id, qp, undo_log, log_offset, undo_log_size)) return false;
    
                //+ we can read the undo logs at the same time. or do that later 
                //RDMAREAD - logs.
            }    
        }
    }

        //coro_sched->Yield(yield, coro_id); //wait for logs to arrive.
        //while (!coro_sched->CheckLogAck(coro_id)){
        while (!coro_sched->CheckRecoveryLogAck(coro_id)){
            //wait for a 1 micro second

        }

    for (int t=0; t<num_thread; t++){

        for (int c = 1 ; c < num_coro ; c++){   
    
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
    
                    UndoLogRecord* record =  (UndoLogRecord *)undo_logs [t][c][i];                  
                    //to check if the transaction has been commited or partioal log locks are held. still need to unlock. 
                    //Latch log will never be written out of order, assuing truncation
                    if(record[r].tx_id_ <= 0){
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
                        curr_agreed_tx_id = record[r].tx_id_;                    
                        continue;
                    }
    
                    //TODO - handle (-1) in uncompleted last transactions.unflagged logs. 
    
                    if(curr_agreed_tx_id != record[r].tx_id_){
    
                            //TODO- check if the tx if is -1;
                            tx_mismatched = true;
                            curr_log_matched &= false;
                            tx_id_agreed &= false;
                            index_at_curr_tx_mismatch = r; // index mismatch without reaching the last flag can be considered as failed. 
                            curr_agreed_tx_id = (curr_agreed_tx_id > record[r].tx_id_)? curr_agreed_tx_id : record[r].tx_id_;
                            
                            //last_commited_tx_id = curr_agreed_tx_id;  
                            break;
                    }
                    //if tx matched. if the last flag is set. tx has staretd commiting. 
                    last_flagged &= (bool)record[r].t_id_;  

                }// NODES
    
    
                if(!tx_id_agreed){

                    // simply unlock places and continue. 
                    //coro_agreed_tx_id = curr_tx_id;
    
                    //TODO- check if no transactions have started.

                    //modifying for non last flagged. 
                    if(r==0){
                        has_started_commit = false;
                    }else{

                        //do not care about the last flagged. do recovery regardles of last falgged. no harm here. a bit conservative.
                        has_started_commit = true;
                        num_valid_logs = r;
                        //coro_agreed_tx_id is same as the last one.

                    }            
    
                    //TODO - call lock recovery
    
                    break;
                }
                //all last tx_ids agreed. 
                else{
    
                    coro_agreed_tx_id = curr_agreed_tx_id;                
    
                    if(!last_flagged){
                        //continue with the next log record if if
                        assert((r+1) <= MAX_DATA_LOG_RECORDS);
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
               coro_has_started_commit[t][c] = has_started_commit; ; //TODO- iinvoke unlocks
            }
    
            else{
    
                // TODO - check in-place updates 
                coro_num_valid_logs[t][c] = (int)num_valid_logs;
                coro_has_started_commit[t][c] = has_started_commit;
    
                //Iterate through logs (table, key) and read in-place values.
    
                UndoLogRecord* record_node_0 =  (UndoLogRecord *)undo_logs [t][c][0]; 
    
                //Is this replicas?. Is this same as replicas.
                for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){
                    
                    char* inplace_update = thread_rdma_buffer_alloc->Alloc(inplace_update_size); // or 512 bytes        
                    inplace_updates [t][c][i] = inplace_update; //allocating space; 
    
                    for (int rc =0 ; rc < num_valid_logs; rc++){  //recorc c
    
                            DataItem* logged_item= &record_node_0[rc].data_ ; 
    
                            table_id_t logged_table_id = logged_item->table_id;
                            itemkey_t logged_key = logged_item->key;

                            node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(logged_item->table_id);
                            
                            RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
                            auto offset = addr_cache->Search(remote_node_id, logged_item->table_id, logged_item->key);
    
                            //assume all logs keys are a cache hit.
                            if (offset != NOT_FOUND) {

                                //TODO - pointers are 
                                if (!coro_sched->RDMARead(coro_id, qp, &inplace_updates [t][c][i][rc*DataItemSize], offset, DataItemSize)) {
                                //if (!coro_sched->RDMARead(coro_id, qp, &inplace_update[rc], offset, DataItemSize)) {
                                    return false;
                                }
    
                            } 
                            else{
                                //TODO- report back to me
                                assert(false);
                            }           
                            //+ we can read the undo logs at the same time. or do that later 
                            //RDMAREAD - logs.
                    }//Valid log entries per transactions.    
    
                }
    
                //check in-place values.
                //coro_sched->Yield(yield, coro_id); //wait for all in-place to arrive.
                //
                //for (int c = 1 ; c < num_coro ; c++){ 
                //    
                //    if(coro_has_started_commit[t][c]){
                //       
                //       for(int j=0; j < coro_num_valid_logs[c]; j++){
                //            RDMA_LOG(INFO) << "recovery coro  - " << c;  
                //        }
                //
                //    }
                //    
                //}
    
    
            }// read all in place updates

            //Freening pointer and threads.
            //TODO. 

    
        } //every coro

    }// for every thread

    //check in-place values.

    //coro_sched->Yield(yield, coro_id); //wait for all in-place to arrive.
    //Replacing yield with just a while loop.
    //Complexity if a upper bound of nuber of cncurrent transactions running on  single compute node. 

    while(!coro_sched->CheckRecoveryDataAck(coro_id));

        for (int t=0; t<num_thread; t++){           

            for (int c = 1 ; c < num_coro ; c++){ 
                
                if(coro_has_started_commit[t][c]){
                   
                   for(int j=0; j < coro_num_valid_logs[t][c]; j++){
                        RDMA_LOG(INFO) << "recovery coro  - " << c;  
                        #ifdef INPLACE_RECOVERY

                        #endif
                        //
                    }

                }
                
            }
        }



}
//DAM - For Latch recovery for all pending transactions.
//bool DTX::IssueDataLogRecoveryRead(coro_yield_t& yield){
//    //recoering undo logs.


//}



//Newwest one with reading data from all replicas. //
//1. in our setup all machines are logging machine sand replicas. f+2 replicas.
//2. 
bool DTX::UpdatedIssueUndoLogRecoveryForAllThreads(coro_yield_t& yield, AddrCache ** addr_caches){

    coro_id_t num_coro = thread_remote_log_offset_alloc->GetNumCoro();
    t_id_t num_thread = thread_remote_log_offset_alloc->GetNumThreadsPerMachine();

    const int MAX_DATA_LOG_RECORDS = 16;

    size_t undo_log_size = sizeof(UndoLogRecord) * MAX_DATA_LOG_RECORDS;
    size_t inplace_update_size = sizeof(DataItem) * MAX_DATA_LOG_RECORDS;

    //need to store: all nodes: all coroutines.
    //Log coro [coror] -> node [i].

    int num_replicas_assumed = global_meta_man->remote_nodes.size();

    char* undo_logs[num_thread][num_coro][global_meta_man->remote_nodes.size()]; // log buffer

    char* inplace_updates[num_thread][num_coro][global_meta_man->remote_nodes.size()]; // log buffer

    int coro_num_valid_logs[num_thread][num_coro]; //filter out last
    bool coro_has_started_commit [num_thread][num_coro]; // finished transactions

    int last_valid_log [num_thread][num_coro]; //filter out last
    
    bool tx_done [num_thread][num_coro]; // finished transactions

    //read all logs.
    //char* undo_log;
    for (int t=0; t<num_thread; t++){
        for (int c=1 ; c < num_coro ; c++){   
            for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){
                char* undo_log = thread_rdma_buffer_alloc->Alloc(undo_log_size); // or 512 bytes
    
                undo_logs [t][c][i] = undo_log; //allocating space;
                
                //FIXED
                offset_t log_offset = thread_remote_log_offset_alloc->GetStartLogOffsetForThread(i, c, t); // bit issue coro_id is wrong. c
                RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(i);
        
                //RDMA Read
                if (!coro_sched->RDMARead(coro_id, qp, undo_log, log_offset, undo_log_size)) return false;
    
                //+ we can read the undo logs at the same time. or do that later 
                //RDMAREAD - logs.
            }    
        }
    }

        //coro_sched->Yield(yield, coro_id); //wait for logs to arrive.
        //while (!coro_sched->CheckLogAck(coro_id)){
        while (!coro_sched->CheckRecoveryLogAck(coro_id)){
            //wait for a 1 micro second

        }

    for (int t=0; t<num_thread; t++){

        for (int c = 1 ; c < num_coro ; c++){   
    
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
    
                    UndoLogRecord* record =  (UndoLogRecord *)undo_logs [t][c][i];                  
                    //to check if the transaction has been commited or partioal log locks are held. still need to unlock. 
                    //Latch log will never be written out of order, assuing truncation

                    //FIX- check if the curretn agrred tx id is.
                    if(record[r].tx_id_ <= 0){
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
                        curr_agreed_tx_id = record[r].tx_id_;                    
                        continue;
                    }
    
                    //TODO - handle (-1) in uncompleted last transactions.unflagged logs. 
    
                    if(curr_agreed_tx_id != record[r].tx_id_){
    
                            //TODO- check if the tx if is -1;
                            tx_mismatched = true;
                            curr_log_matched &= false;
                            tx_id_agreed &= false;
                            index_at_curr_tx_mismatch = r; // index mismatch without reaching the last flag can be considered as failed. 
                            curr_agreed_tx_id = (curr_agreed_tx_id > record[r].tx_id_)? curr_agreed_tx_id : record[r].tx_id_;
                            
                            //last_commited_tx_id = curr_agreed_tx_id;  
                            break;
                    }
                    //if tx matched. if the last flag is set. tx has staretd commiting. 
                    last_flagged &= (bool)record[r].t_id_;  

                }// NODES    
    
                if(!tx_id_agreed){

                    // simply unlock places and continue. 
                    //coro_agreed_tx_id = curr_tx_id;
    
                    //TODO- check if no transactions have started.

                    //modifying for non last flagged. 
                    if(r==0){
                        has_started_commit = false;
                    }else{

                        //do not care about the last flagged. do recovery regardles of last falgged. no harm here. a bit conservative.
                        
                        //TODO - This is wrong. 
                        //coro_agreed_tx_id = curr_agreed_tx_id; 


                        has_started_commit = true;
                        num_valid_logs = r;
                        //coro_agreed_tx_id is same as the last one.

                    }            
    
                    //TODO - call lock recovery
    
                    break;
                }
                //all last tx_ids agreed. 
                else{
                
                    if(curr_agreed_tx_id < coro_agreed_tx_id){
                        //canot be larger. assert. we write logs in order. cannt get reordered.
                        has_started_commit = true;
                        num_valid_logs = r;  // remove the last log recored. which is old.
                        break;
                    }


                    coro_agreed_tx_id = curr_agreed_tx_id;                
    
                    if(!last_flagged){
                        //continue with the next log record if if
                        assert((r+1) <= MAX_DATA_LOG_RECORDS);

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
               coro_has_started_commit[t][c] = has_started_commit; ; //TODO- iinvoke unlocks
            }
    
            else{
    
                // TODO - check in-place updates 
                coro_num_valid_logs[t][c] = (int)num_valid_logs;
                coro_has_started_commit[t][c] = has_started_commit;

                //remember. last flagged only workd in with inserts.
                //RDMA_LOG(FATAL) << "Thread " << t << " , Coro " << c << " , has started commited Tx " << coro_agreed_tx_id; 
    
                //Iterate through logs (table, key) and read in-place values.
    
                UndoLogRecord* record_node_0 =  (UndoLogRecord *)undo_logs [t][c][0]; 
    
                //Is this replicas?. Is this same as replicas.

                //Assumoing num machine sare num of replicas.
                for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){
                    
                    char* inplace_update = thread_rdma_buffer_alloc->Alloc(inplace_update_size); // or 512 bytes        
                    inplace_updates [t][c][i] = inplace_update; //allocating space; 

                }
    
                for (int rc =0 ; rc < num_valid_logs; rc++){  //recorc c

                        DataItem* logged_item= &record_node_0[rc].data_ ; 

                        table_id_t logged_table_id = logged_item->table_id;
                        itemkey_t logged_key = logged_item->key;

                        //RDMA_LOG(FATAL) << "Thread " << t << " , Coro " << c << " , has started commited Tx " <<  logged_key<< " of Table " << logged_table_id; 

                        node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(logged_item->table_id);
                                                
                        RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

                        //FIXED auto offset = addr_cache->Search(remote_node_id, logged_item->table_id, logged_item->key);
                        auto offset = addr_caches[t]->Search(remote_node_id, logged_item->table_id, logged_item->key);

                        //assume all logs keys are a cache hit.
                        if (offset != NOT_FOUND) {
                            //TODO - pointers are 
                            if (!coro_sched->RDMARead(coro_id, qp, &inplace_updates [t][c][0][rc*DataItemSize], offset, DataItemSize)) {
                            //if (!coro_sched->RDMARead(coro_id, qp, &inplace_update[rc], offset, DataItemSize)) {
                                return false;
                            }

                        } 
                        else{
                            //TODO- report back to me
                            RDMA_LOG(FATAL) << "Error 0- Local Cache Miss "; 
                            assert(false);
                        }          


                        //NEW-getting the backup offset can be done using primary
                        const HashMeta& primary_hash_meta = global_meta_man->GetPrimaryHashMetaWithTableID(logged_table_id);
                        auto offset_in_backup_hash_store = offset - primary_hash_meta.base_off; //absolute value
                        const std::vector<HashMeta>* backup_hash_metas = global_meta_man->GetBackupHashMetasWithTableID(logged_table_id);


                        auto* backup_node_ids = global_meta_man->GetBackupNodeID(logged_item->table_id);
                        if (!backup_node_ids) continue;  // There are no backups in the PM pool

                        assert(backup_node_ids->size() <=  (global_meta_man->remote_nodes.size()-1));

                        for (size_t i = 0; i < backup_node_ids->size(); i++) {

                            //NEW
                            auto remote_item_off = offset_in_backup_hash_store + (*backup_hash_metas)[i].base_off;
                            //auto remote_lock_off = it->GetRemoteLockAddr(remote_item_off);

                            RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(backup_node_ids->at(i));

                            if (!coro_sched->RDMARead(coro_id, qp, &inplace_updates [t][c][i+1][rc*DataItemSize], remote_item_off, DataItemSize)) {
                                    return false;
                            }
                            
                            /* NEW Deprecated auto offset = addr_cache->Search(backup_node_ids->at(i), logged_item->table_id, logged_item->key);

                            if (offset != NOT_FOUND) {
                                if (!coro_sched->RDMARead(coro_id, qp, &inplace_updates [t][c][i+1][rc*DataItemSize], offset, DataItemSize)) {
                                    return false;
                                }
                            }else{
                                RDMA_LOG(INFO) << "Error 0: Local Cache Miss"; 
                                assert(false); // When the offset is not present in the cache, for recovery i assume that everything is in the cache.
                            }
                            */

                        }

                        //+ we can read the undo logs at the same time. or do that later 
                        //RDMAREAD - logs.
                }//Valid log entries per transactions.    
    
                
    
                //check in-place values.
                //coro_sched->Yield(yield, coro_id); //wait for all in-place to arrive.
                //
                //for (int c = 1 ; c < num_coro ; c++){ 
                //    
                //    if(coro_has_started_commit[t][c]){
                //       
                //       for(int j=0; j < coro_num_valid_logs[c]; j++){
                //            RDMA_LOG(INFO) << "recovery coro  - " << c;  
                //        }
                //
                //    }
                //    
                //}
    
    
            }// read all in place updates

            //Freening pointer and threads.
            //TODO. 

    
        } //every coro

    }// for every thread

    //check in-place values.

    //coro_sched->Yield(yield, coro_id); //wait for all in-place to arrive.
    //Replacing yield with just a while loop.
    //Complexity if a upper bound of nuber of cncurrent transactions running on  single compute node. 

    //coro_sched->Yield(yield, coro_id);
    while(!coro_sched->CheckRecoveryDataAck(coro_id)) ;
    //while(!coro_sched->CheckRecoveryDataAck(coro_id));

        for (int t=0; t<num_thread; t++){           

            for (int c = 1 ; c < num_coro ; c++){ 
                //RDMA_LOG(INFO) << "recovery coro  - " << c;  
                
                if(coro_has_started_commit[t][c]){
                    //RDMA_LOG(INFO) << " --> TX has started commit  - " << c << " with valid logs " <<  coro_num_valid_logs[t][c] ;  

                   UndoLogRecord* record_node_0 =  (UndoLogRecord *)undo_logs [t][c][0]; 

                   bool  tx_completed = true;

                   for(int j=0; j < coro_num_valid_logs[t][c]; j++){
                        

                        //TODO; assume thet it is equal to the number of replicas. 
                        //ideally relicas+1
                        DataItem* logged_item = &record_node_0[j].data_ ;
                        bool is_updated_inplace=true;
                        int match_count=0;

                         //Check if all inplace updates matches for every transactions
                        //TODO: if amemory replic fails whatever present in the backup replicas will be used. 
                        for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){                            
                                
                                //FIX DataItem * in_place_item =  (DataItem *) inplace_updates [t][c][i][j*DataItemSize];
                                DataItem in_place_item =  ((DataItem *)(inplace_updates [t][c][i]))[j];
                                
                                assert(in_place_item != NULL);

                                if(logged_item->version <= in_place_item->version){
                                    is_updated_inplace= true;
                                    match_count++; 
                                    //break;
                                }
                        }

                        //APPLy UNDO Logs.
                        if(match_count != global_meta_man->remote_nodes.size()) {
                            tx_completed=false;
                            //apply all in-place updates.
                            node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(logged_item->table_id);                                                
                            RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
                            
                            //FIXED auto offset = addr_cache->Search(remote_node_id, logged_item->table_id, logged_item->key);
                            auto offset = addr_caches[t]->Search(remote_node_id, logged_item->table_id, logged_item->key);

                             if (offset != NOT_FOUND) {

                               if (!coro_sched->RDMAWrite(coro_id, qp, (char*)logged_item, offset, DataItemSize)) {
                                       return false;
                                }    
                            }else{

                                RDMA_LOG(FATAL) << "Error 1 - no offset for primary";  
                                assert(false);
                            }

                            auto* backup_node_ids = global_meta_man->GetBackupNodeID(logged_item->table_id);
                            if (!backup_node_ids) continue;  // There are no backups in the PM pool
    
                            for (size_t i = 0; i < backup_node_ids->size(); i++) {
    
                                RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(backup_node_ids->at(i));
                                auto offset = addr_cache->Search(backup_node_ids->at(i), logged_item->table_id, logged_item->key);
    
                                if (offset != NOT_FOUND) {
                                    if (!coro_sched->RDMAWrite(coro_id, qp, (char*)logged_item, offset, DataItemSize)) {
                                        return false;
                                    }
                                }else{
                                    RDMA_LOG(FATAL) << "Error 2- no cache for bkup ";  
                                    assert(false); // When the offset is not present in the cache, for recovery i assume that everything is in the cache.
                                }
                            }

                            break;
                        }
                        
                    }

                    //come back anc undo all the changes.
                    
                }
                
            }
        }

        while(!coro_sched->CheckRecoveryDataAck(coro_id));

}
#endif