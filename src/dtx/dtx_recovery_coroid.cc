#include "dtx/dtx.h"
//Recovery program

#if defined(LATCH_RECOVERY) || defined(UNDO_RECOVERY)

bool DTX::TxUndoRecoveryCoroId(coro_yield_t& yield, AddrCache ** addr_caches){
    //IssueUndoLogRecovery(yield);
    UpdatedIssueUndoLogRecoveryForAllThreadsCoroId(yield, addr_caches);
    //IssueLatchLogRecoveryRead(yield);
}

bool DTX::TxUndoRecoveryCoroId(coro_yield_t& yield, AddrCache ** addr_caches, t_id_t failed_thread_id, coro_id_t failed_coro_id){
    //IssueUndoLogRecovery(yield);
    UpdatedIssueUndoLogRecoveryForAllThreadsCoroId(yield, addr_caches, failed_thread_id, failed_coro_id);
    //IssueLatchLogRecoveryRead(yield);
}

bool DTX::TxUndoRecoveryCoroId(coro_yield_t& yield, AddrCache ** addr_caches, t_id_t start_failed_thread_id, t_id_t end_failed_thread_id){
    //IssueUndoLogRecovery(yield);
    UpdatedIssueUndoLogRecoveryForAllThreadsCoroId(yield, addr_caches, start_failed_thread_id, end_failed_thread_id);
    //IssueLatchLogRecoveryRead(yield);
}


//Newwest one with reading data from all replicas. //
//1. in our setup all machines are logging machine sand replicas. f+2 replicas.
//2. 
bool DTX::UpdatedIssueUndoLogRecoveryForAllThreadsCoroId(coro_yield_t& yield, AddrCache ** addr_caches){

    coro_id_t num_coro = thread_remote_log_offset_alloc->GetNumCoro();

    //TODO- fix this. only the failed process threads. otherwise caches overflowing. 
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
               
                //FIX LAST FLAG
                bool last_flagged=false; //in this log record. 
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
                    //last_flagged &= (bool)record[r].t_id_;  
                    last_flagged = (bool)record[r].t_id_;  

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
                        //has_started_commit = true; //FIX
                        has_started_commit = false;
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
                    //RDMA_LOG(INFO) << " --> TX has started commit  - " << c << " with valid logs " << coro_num_valid_logs[t][c];
                   UndoLogRecord* record_node_0 =  (UndoLogRecord *)undo_logs [t][c][0]; 

                   bool  tx_completed = true;
                   bool is_updated_inplace = true;

                   for(int j=0; j < coro_num_valid_logs[t][c]; j++){                        

                        //TODO; assume thet it is equal to the number of replicas. 
                        //ideally relicas+1
                        DataItem* logged_item = &record_node_0[j].data_ ;                        
                        int match_count = 0;
                         //Check if all inplace updates matches for every transactions
                        //TODO: if amemory replic fails whatever present in the backup replicas will be used. 
                        for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){                            
                                
                                //FIX DataItem * in_place_item =  (DataItem *) inplace_updates [t][c][i][j*DataItemSize];
                                DataItem in_place_item =  ((DataItem *)(inplace_updates [t][c][i]))[j];

                                //assert(in_place_item != NULL);

                                //RDMA_LOG(INFO) << " logged_item " << logged_item->key << " with version " << logged_item->version;
                                //RDMA_LOG(INFO) << " inplce_item of node " << in_place_item.key << " with version " << in_place_item.version;

                                if(logged_item->version < in_place_item.version){
                                    //is_updated_inplace &= true;
                                    match_count++; 
                                    //break;
                                }
                                else{
                                    is_updated_inplace &= false;
                                }
                        }

                        //APPLy UNDO Logs.
                        if((match_count != global_meta_man->remote_nodes.size()) &&  (match_count != 0) ) {
                            tx_completed &= false;    //partial   
                            break;
                        }

                        
                    }//valida records


                    //come back anc undo all the changes. pessimistic. apply all undos. not necessary. 
                    //SUCC (tx_completed && is_updated_inplace asll (match))

                    if((!tx_completed) || (!is_updated_inplace)) {
                        for(int j=0; j < coro_num_valid_logs[t][c]; j++){
                    
                            DataItem* logged_item = &record_node_0[j].data_ ;
    
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
    
                                //NEW-getting the backup offset can be done using primary
                                const HashMeta& primary_hash_meta = global_meta_man->GetPrimaryHashMetaWithTableID(logged_item->table_id);
                                auto offset_in_backup_hash_store = offset - primary_hash_meta.base_off; //absolute value
                                const std::vector<HashMeta>* backup_hash_metas = global_meta_man->GetBackupHashMetasWithTableID(logged_item->table_id);
    
    
                                auto* backup_node_ids = global_meta_man->GetBackupNodeID(logged_item->table_id);
                                if (!backup_node_ids) continue;  // There are no backups in the PM pool
    
                                assert(backup_node_ids->size() <=  (global_meta_man->remote_nodes.size()-1));
        
                                for (size_t i = 0; i < backup_node_ids->size(); i++) {   
                                    
    
                                    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(backup_node_ids->at(i));
                                    
                                    //FIXED auto offset = addr_cache->Search(backup_node_ids->at(i), logged_item->table_id, logged_item->key);
                                    auto remote_item_off = offset_in_backup_hash_store + (*backup_hash_metas)[i].base_off;
    
                                    
                                    if (!coro_sched->RDMAWrite(coro_id, qp, (char*)logged_item, remote_item_off, DataItemSize)) {
                                        return false;
                                    }
                                  
                                }
    
                        }// UNdo apply finished
                    }
                    
                } //has started commit
                
            }
        }

        while(!coro_sched->CheckRecoveryDataAck(coro_id));

}



//Mainly for Litmus tests.
bool DTX::UpdatedIssueUndoLogRecoveryForAllThreadsCoroId(coro_yield_t& yield, AddrCache ** addr_caches, t_id_t failed_thread_id, coro_id_t failed_coro_id){

    coro_id_t num_coro = thread_remote_log_offset_alloc->GetNumCoro();

    //TODO- fix this. only the failed process threads. otherwise caches overflowing. 
    t_id_t num_thread = thread_remote_log_offset_alloc->GetNumThreadsPerMachine();

    assert(failed_thread_id <= num_thread);
    assert(failed_coro_id <= num_coro);   

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

        if(t != failed_thread_id) continue;

        for (int c=1 ; c < num_coro ; c++){  

            if(c != failed_coro_id) continue;

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

        if(t != failed_thread_id) continue;

        for (int c = 1 ; c < num_coro ; c++){   

            if(c != failed_coro_id) continue;
    
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

                //FIX LAST FLAG
                bool last_flagged=false; //in this log record. 
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
                    //last_flagged &= (bool)record[r].t_id_; 
                    last_flagged = (bool)record[r].t_id_;   

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
                        //has_started_commit = true; //FIX
                        has_started_commit = false;
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

            if(t != failed_thread_id) continue;        

            for (int c = 1 ; c < num_coro ; c++){ 

                if(c != failed_coro_id) continue;
                //RDMA_LOG(INFO) << "recovery coro  - " << c;  
                
                if(coro_has_started_commit[t][c]){
                    //RDMA_LOG(INFO) << " --> TX has started commit  - " << c << " with valid logs " <<  coro_num_valid_logs[t][c] ;  
                    //RDMA_LOG(INFO) << " --> TX has started commit  - " << c << " with valid logs " << coro_num_valid_logs[t][c];
                   UndoLogRecord* record_node_0 =  (UndoLogRecord *)undo_logs [t][c][0]; 

                   bool  tx_completed = true;
                   bool is_updated_inplace = true;

                   for(int j=0; j < coro_num_valid_logs[t][c]; j++){                        

                        //TODO; assume thet it is equal to the number of replicas. 
                        //ideally relicas+1
                        DataItem* logged_item = &record_node_0[j].data_ ;                        
                        int match_count = 0;
                         //Check if all inplace updates matches for every transactions
                        //TODO: if amemory replic fails whatever present in the backup replicas will be used. 
                        for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){                            
                                
                                //FIX DataItem * in_place_item =  (DataItem *) inplace_updates [t][c][i][j*DataItemSize];
                                DataItem in_place_item =  ((DataItem *)(inplace_updates [t][c][i]))[j];

                                //assert(in_place_item != NULL);

                                //RDMA_LOG(INFO) << " logged_item " << logged_item->key << " with version " << logged_item->version;
                                //RDMA_LOG(INFO) << " inplce_item of node " << in_place_item.key << " with version " << in_place_item.version;

                                if(logged_item->version < in_place_item.version){
                                    //is_updated_inplace &= true;
                                    match_count++; 
                                    //break;
                                }
                                else{
                                    is_updated_inplace &= false;
                                }
                        }

                        //APPLy UNDO Logs.
                        if((match_count != global_meta_man->remote_nodes.size()) &&  (match_count != 0) ) {
                            tx_completed &= false;    //partial   
                            break;
                        }

                        
                    }//valida records


                    //come back anc undo all the changes. pessimistic. apply all undos. not necessary. 
                    //SUCC (tx_completed && is_updated_inplace asll (match))

                    if((!tx_completed) || (!is_updated_inplace)) {
                        for(int j=0; j < coro_num_valid_logs[t][c]; j++){
                    
                            DataItem* logged_item = &record_node_0[j].data_ ;
    
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
    
                                //NEW-getting the backup offset can be done using primary
                                const HashMeta& primary_hash_meta = global_meta_man->GetPrimaryHashMetaWithTableID(logged_item->table_id);
                                auto offset_in_backup_hash_store = offset - primary_hash_meta.base_off; //absolute value
                                const std::vector<HashMeta>* backup_hash_metas = global_meta_man->GetBackupHashMetasWithTableID(logged_item->table_id);
    
    
                                auto* backup_node_ids = global_meta_man->GetBackupNodeID(logged_item->table_id);
                                if (!backup_node_ids) continue;  // There are no backups in the PM pool
    
                                assert(backup_node_ids->size() <=  (global_meta_man->remote_nodes.size()-1));
        
                                for (size_t i = 0; i < backup_node_ids->size(); i++) {   
                                    
    
                                    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(backup_node_ids->at(i));
                                    
                                    //FIXED auto offset = addr_cache->Search(backup_node_ids->at(i), logged_item->table_id, logged_item->key);
                                    auto remote_item_off = offset_in_backup_hash_store + (*backup_hash_metas)[i].base_off;
    
                                    
                                    if (!coro_sched->RDMAWrite(coro_id, qp, (char*)logged_item, remote_item_off, DataItemSize)) {
                                        return false;
                                    }
                                  
                                }
    
                        }// UNdo apply finished
                    }
                    
                } //has started commit
                
            }
        }

        while(!coro_sched->CheckRecoveryDataAck(coro_id));

}

//We assume contigoius thread ids.

bool DTX::UpdatedIssueUndoLogRecoveryForAllThreadsCoroId(coro_yield_t& yield, AddrCache ** addr_caches, t_id_t start_failed_thread_id, t_id_t end_failed_thread_id){

    coro_id_t num_coro = thread_remote_log_offset_alloc->GetNumCoro();

    //TODO- fix this. only the failed process threads. otherwise caches overflowing. 
    t_id_t num_thread = thread_remote_log_offset_alloc->GetNumThreadsPerMachine();

    assert(end_failed_thread_id <= num_thread);
    //assert(failed_coro_id <= num_coro);   

    const int MAX_DATA_LOG_RECORDS = 32; //default =32. reduce to 16 for non tpcc

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
    for (int t=start_failed_thread_id; t < end_failed_thread_id; t++){

        //if(t != failed_thread_id) continue;

        for (int c=1 ; c < num_coro ; c++){  

            //if(c != failed_coro_id) continue;

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

    for (int t=start_failed_thread_id; t<end_failed_thread_id; t++){

        //if(t != failed_thread_id) continue;

        for (int c = 1 ; c < num_coro ; c++){   

            //if(c != failed_coro_id) continue;
    
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

                //FIX LAST FLAG
                bool last_flagged=false; //in this log record. 
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
                    //last_flagged &= (bool)record[r].t_id_;  
                    last_flagged = (bool)record[r].t_id_;  

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
                        //has_started_commit = true; //FIX: without last_flagged
                        has_started_commit = false;
                        num_valid_logs = r;  // remove the last log recored. which is old.
                        break;
                    }

                    //RDMA_LOG(INFO) << "curr " << curr_agreed_tx_id  << " agreed " << coro_agreed_tx_id;
                    assert((coro_agreed_tx_id ==0) || (curr_agreed_tx_id <= coro_agreed_tx_id));

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
                        
                        //has_started_commit = true;
                        //num_valid_logs = r+1; 
                        //break;
                        continue;
    
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

        for (int t=start_failed_thread_id; t<end_failed_thread_id; t++){   

            //if(t != failed_thread_id) continue;        

            for (int c = 1 ; c < num_coro ; c++){ 

                //if(c != failed_coro_id) continue;
                //RDMA_LOG(INFO) << "recovery coro  - " << c;  
                
                if(coro_has_started_commit[t][c]){
                    //RDMA_LOG(INFO) << " --> TX has started commit  - " << c << " with valid logs " <<  coro_num_valid_logs[t][c] ;  
                    //RDMA_LOG(INFO) << " --> TX has started commit  - " << c << " with valid logs " << coro_num_valid_logs[t][c];
                   UndoLogRecord* record_node_0 =  (UndoLogRecord *)undo_logs [t][c][0]; 

                   bool  tx_completed = true;
                   bool is_updated_inplace = true;

                   for(int j=0; j < coro_num_valid_logs[t][c]; j++){                        

                        //TODO; assume thet it is equal to the number of replicas. 
                        //ideally relicas+1
                        DataItem* logged_item = &record_node_0[j].data_ ;                        
                        int match_count = 0;
                         //Check if all inplace updates matches for every transactions
                        //TODO: if amemory replic fails whatever present in the backup replicas will be used. 
                        for (int i = 0; i < global_meta_man->remote_nodes.size(); i++){                            
                                
                                //FIX DataItem * in_place_item =  (DataItem *) inplace_updates [t][c][i][j*DataItemSize];
                                DataItem in_place_item =  ((DataItem *)(inplace_updates [t][c][i]))[j];

                                //assert(in_place_item != NULL);

                                //RDMA_LOG(INFO) << " logged_item " << logged_item->key << " with version " << logged_item->version;
                                //RDMA_LOG(INFO) << " inplce_item of node " << in_place_item.key << " with version " << in_place_item.version;

                                if(logged_item->version < in_place_item.version){
                                    //is_updated_inplace &= true;
                                    match_count++; 
                                    //break;
                                }
                                else{
                                    is_updated_inplace &= false;
                                }
                        }

                        //APPLy UNDO Logs.
                        if((match_count != global_meta_man->remote_nodes.size()) &&  (match_count != 0) ) {
                            tx_completed &= false;    //partial   
                            break;
                        }

                        
                    }//valida records


                    //come back anc undo all the changes. pessimistic. apply all undos. not necessary. 
                    //SUCC (tx_completed && is_updated_inplace asll (match))

                    if((!tx_completed) || (!is_updated_inplace)) {
			   /// RDMA_LOG(INFO) << "roollback";
                        for(int j=0; j < coro_num_valid_logs[t][c]; j++){
                    
                            DataItem* logged_item = &record_node_0[j].data_ ;
    
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
    
                                //NEW-getting the backup offset can be done using primary
                                const HashMeta& primary_hash_meta = global_meta_man->GetPrimaryHashMetaWithTableID(logged_item->table_id);
                                auto offset_in_backup_hash_store = offset - primary_hash_meta.base_off; //absolute value
                                const std::vector<HashMeta>* backup_hash_metas = global_meta_man->GetBackupHashMetasWithTableID(logged_item->table_id);
    
    
                                auto* backup_node_ids = global_meta_man->GetBackupNodeID(logged_item->table_id);
                                if (!backup_node_ids) continue;  // There are no backups in the PM pool
    
                                assert(backup_node_ids->size() <=  (global_meta_man->remote_nodes.size()-1));
        
                                for (size_t i = 0; i < backup_node_ids->size(); i++) {   
                                    
    
                                    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(backup_node_ids->at(i));
                                    
                                    //FIXED auto offset = addr_cache->Search(backup_node_ids->at(i), logged_item->table_id, logged_item->key);
                                    auto remote_item_off = offset_in_backup_hash_store + (*backup_hash_metas)[i].base_off;
    
                                    
                                    if (!coro_sched->RDMAWrite(coro_id, qp, (char*)logged_item, remote_item_off, DataItemSize)) {
                                        return false;
                                    }
                                  
                                }
    
                        }// UNdo apply finished
                    }
                    
                } //has started commit
                
            }
        }

        while(!coro_sched->CheckRecoveryDataAck(coro_id));

}
#endif
