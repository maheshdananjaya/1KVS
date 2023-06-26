// Author: Ming Zhang
// Copyright (c) 2021

#include "micro/micro_bench.h"

#include <atomic>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>
#include <set>

#include "allocator/buffer_allocator.h"
#include "allocator/log_allocator.h"
#include "connection/qp_manager.h"
#include "dtx/dtx.h"
#include "util/latency.h"
#include "util/zipf.h"

using namespace std::placeholders;

#define ATTEMPED_NUM 1000000
#define STAT_NUM_MAX_THREADS 128
// All the functions are executed in each thread
//For Crash TPUT
#ifdef STATS
  //extern uint64_t * tx_attempted;
  //extern uint64_t * tx_commited;
  //extern bool * thread_done;
  //extern double * window_start_time;
  //extern double * window_curr_time;

 extern uint64_t tx_attempted alignas(8)[STAT_NUM_MAX_THREADS];
 extern uint64_t tx_commited alignas(8) [STAT_NUM_MAX_THREADS];
 extern bool thread_done [STAT_NUM_MAX_THREADS];
 extern double window_start_time alignas(8) [STAT_NUM_MAX_THREADS];
 extern double window_curr_time alignas (8) [STAT_NUM_MAX_THREADS];
 extern REC* record_ptrs [STAT_NUM_MAX_THREADS];
 extern node_id_t machine_num_;
 extern node_id_t machine_id_;
 extern t_id_t thread_num_per_machine_;
#endif

__thread t_id_t local_thread_id; 


extern std::atomic<uint64_t> tx_id_generator;
extern std::atomic<uint64_t> connected_t_num;
extern std::vector<double> lock_durations;
extern std::mutex mux;

extern std::vector<t_id_t> tid_vec;
extern std::vector<double> attemp_tp_vec;
extern std::vector<double> tp_vec;
extern std::vector<double> medianlat_vec;
extern std::vector<double> taillat_vec;

extern AddrCache**  addr_caches;
extern bool * failed_id_list;


__thread uint64_t seed; /* Thread-global random seed */
__thread t_id_t thread_gid;
__thread t_id_t thread_local_id;
__thread t_id_t thread_num;
__thread MICRO* micro_client;
__thread MetaManager* meta_man;
__thread QPManager* qp_man;
__thread RDMABufferAllocator* rdma_buffer_allocator;
__thread LogOffsetAllocator* log_offset_allocator;
__thread AddrCache* addr_cache;
__thread MicroTxType* workgen_arr;

__thread coro_id_t coro_num;
__thread CoroutineScheduler* coro_sched;  // Each transaction thread has a coroutine scheduler
__thread bool stop_run;

// Performance measurement (thread granularity)
__thread struct timespec msr_start, msr_end;
// __thread Latency* latency;
__thread double* timer;
__thread ZipfGen* zipf_gen;
__thread bool is_skewed;
__thread uint64_t data_set_size;
__thread uint64_t num_keys_global;
__thread uint64_t write_ratio;
// const int lat_multiplier = 10; // For sub-microsecond latency measurement
__thread uint64_t stat_attempted_tx_total = 0;  // Issued transaction number
__thread uint64_t stat_committed_tx_total = 0;  // Committed transaction number
const coro_id_t POLL_ROUTINE_ID = 0;            // The poll coroutine ID

extern bool crash_emu;
extern t_id_t new_base_tid;
extern uint64_t num_crashes;

#define CRASH_INTERVAL 500000
__thread uint64_t next_crash_count=CRASH_INTERVAL;


#ifdef CRASH_TPUT
thread_local std::ofstream file_out;// per thread file writes
__thread const double window_time_ns=500000; // exmaple 100 microseconds -  
__thread double last_recorded_nsec_time = 0;
__thread uint64_t last_recorded_attempted_tx = 0;  // Issued transaction number
__thread uint64_t last_recorded_committed_tx = 0;  // Committed transaction number
__thread double curr_time =0;
__thread double recorded_start_time=0;
#endif

#ifdef MEM_FAILURES
__thread uint64_t mem_crash_coros =0; // number of coros finished after mem crash recoeved.
extern bool mem_crash_enable;
extern std::atomic<uint64_t> mem_crash_tnums;
extern uint64_t num_mem_crashes;
#endif

/******************** The business logic (Transaction) start ********************/

struct DataItemDuplicate {
  DataItemPtr data_item_ptr;
  bool is_dup;
};

bool TxTestCachedAddr(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);
  bool is_write[data_set_size];
  DataItemDuplicate micro_objs[data_set_size];

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_key_t micro_key;
    micro_key.micro_id = 688;  // First read is non-cacheable, set ATTEMPED_NUM to 5

    assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

    micro_objs[i].data_item_ptr = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);
    micro_objs[i].is_dup = false;

    dtx->AddToReadWriteSet(micro_objs[i].data_item_ptr);
    is_write[i] = true;
  }

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_val_t* micro_val = (micro_val_t*)micro_objs[i].data_item_ptr->value;
    if (micro_val->magic[0] != micro_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (is_write[i]) {
      micro_val->magic[1] = micro_magic * 2;
    }
  }

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

bool TxLockContention(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);
  bool is_write[data_set_size];
  DataItemPtr micro_objs[data_set_size];

//DAM- do the same thing multiple times
for (uint64_t i = 0; i < data_set_size; i++) { 

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_key_t micro_key;
    if (is_skewed) {
      // Skewed distribution
      micro_key.micro_id = (itemkey_t)(zipf_gen->next());
    } else {
      // Uniformed distribution
      micro_key.micro_id = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);
    }

    assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

    micro_objs[i] = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);

    if (FastRand(&seed) % 100 < write_ratio) {
      dtx->AddToReadWriteSet(micro_objs[i]);
      is_write[i] = true;
    } else {
      dtx->AddToReadOnlySet(micro_objs[i]);
      is_write[i] = false;
    }
  }

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_val_t* micro_val = (micro_val_t*)micro_objs[i]->value;
    if (micro_val->magic[0] != micro_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (is_write[i]) {
      micro_val->magic[1] = micro_magic * 2;
    }
  }

}//numer of sequential writes before commits.

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

bool TxReadBackup(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // This is used to evaluate the performance of reading backup vs. not reading backup when the write ratio is not 0
  // Remember to set remote_node_id = 0; in dtx_issue.cc to read backup for RO data
  // Use 32 threads and 8 corotines
  dtx->TxBegin(tx_id);
  bool is_write[data_set_size];
  DataItemDuplicate micro_objs[data_set_size];

  std::set<uint64_t> ids;
  std::vector<uint64_t> duplicate_item;

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_key_t micro_key;
    if (is_skewed) {
      // Skewed distribution
      micro_key.micro_id = (itemkey_t)(zipf_gen->next());
    } else {
      // Uniformed distribution
      micro_key.micro_id = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);
    }

    auto ret = ids.insert(micro_key.item_key);
    if (!ret.second) {
      micro_objs[i].is_dup = true;
      continue;
    }

    assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

    micro_objs[i].data_item_ptr = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);
    micro_objs[i].is_dup = false;

    if (FastRand(&seed) % 100 < write_ratio) {
      dtx->AddToReadWriteSet(micro_objs[i].data_item_ptr);
      is_write[i] = true;
    } else {
      dtx->AddToReadOnlySet(micro_objs[i].data_item_ptr);
      is_write[i] = false;
    }
  }

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  for (uint64_t i = 0; i < data_set_size; i++) {
    if (micro_objs[i].is_dup) continue;
    micro_val_t* micro_val = (micro_val_t*)micro_objs[i].data_item_ptr->value;
    if (micro_val->magic[0] != micro_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (is_write[i]) {
      micro_val->magic[1] = micro_magic * 2;
    }
  }

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

bool TxReadOnly(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // This is used to evaluate the performance of reading backup vs. not reading backup when the write ratio is 0
  // Remember to set remote_node_id = t_id % (BACKUP_DEGREE + 1); in dtx_issue.cc to enable read from primary and backup
  dtx->TxBegin(tx_id);

  micro_key_t micro_key;
  if (is_skewed) {
    // Skewed distribution
    micro_key.micro_id = (itemkey_t)(zipf_gen->next());
  } else {
    // Uniformed distribution
    micro_key.micro_id = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);
  }
  assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

  DataItemPtr micro_obj = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);

  dtx->AddToReadOnlySet(micro_obj);

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

bool TxRFlush1(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);
  DataItemPtr micro_objs[data_set_size];

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_key_t micro_key;
    micro_key.micro_id = i;
    assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);
    micro_objs[i] = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);
    dtx->AddToReadWriteSet(micro_objs[i]);
  }

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_val_t* micro_val = (micro_val_t*)micro_objs[i]->value;
    if (micro_val->magic[0] != micro_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
    }
    micro_val->magic[1] = micro_magic * 2;
  }

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

bool TxRFlush2(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // Test remote flush steps:
  // 1. Turn off undo log to accurately test the perf. difference
  // 2. In Coalescent Commit. Use RDMABatchSync and RDMAReadSync for both full/batch flush
  // 3. Turn off Yield in Coalescent Commit because RDMABatchSync and RDMAReadSync already poll acks
  std::set<itemkey_t> key_set;

  dtx->TxBegin(tx_id);
  DataItemPtr micro_objs[data_set_size];

  // gen keys
  itemkey_t key = 0;
  while (key_set.size() != data_set_size) {
    if (is_skewed) {
      key = (itemkey_t)(zipf_gen->next());
    } else {
      key = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);
    }
    key_set.insert(key);
  }

  int i = 0;
  for (auto it = key_set.begin(); it != key_set.end(); ++it) {
    micro_key_t micro_key;
    micro_key.micro_id = *it;
    assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);
    micro_objs[i] = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);
    dtx->AddToReadWriteSet(micro_objs[i]);
    i++;
  }

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_val_t* micro_val = (micro_val_t*)micro_objs[i]->value;
    if (micro_val->magic[0] != micro_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
    }
    micro_val->magic[1] = micro_magic * 2;
  }

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

/******************** The business logic (Transaction) end ********************/

void PollCompletion(coro_yield_t& yield) {


  #ifdef CRASH_TPUT
     while(crash_emu);
    clock_gettime(CLOCK_REALTIME, &msr_start);
    last_recorded_nsec_time = (double)(msr_start.tv_sec) * 1000000000 + (double)(msr_start.tv_nsec);
    recorded_start_time=last_recorded_nsec_time;
    window_start_time[local_thread_id] = recorded_start_time; // in nano seconds
  #endif

  while (true) {
    coro_sched->PollCompletion();
    Coroutine* next = coro_sched->coro_head->next_coro;
    if (next->coro_id != POLL_ROUTINE_ID) {
      // RDMA_LOG(DBG) << "Coro 0 yields to coro " << next->coro_id;
      coro_sched->RunCoroutine(yield, next);
    }
    if (stop_run) break;

    #ifdef MEM_FAILURES
      if(mem_crash_enable && (mem_crash_coros== (coro_num-1) ) ){
        //mem_crash_enable = false;
        mem_crash_coros = 0;
	mem_crash_tnums ++;
         __asm__ __volatile__("mfence":::"memory"); 
        RDMA_LOG(WARNING) << "Compute Server - Sign Off: Bye bye " << thread_gid;
        break;
      }
    #endif

    #ifdef CRASH_TPUT
        while(crash_emu);

        //only a single coro has to do this.
        clock_gettime(CLOCK_REALTIME, &msr_end);
        curr_time =  (double) msr_end.tv_sec *1000000000 + (double)(msr_end.tv_nsec);

         if(curr_time >= (last_recorded_nsec_time + window_time_ns)){
             //take tput numbers
              uint64_t attempted_tx = (stat_attempted_tx_total-last_recorded_attempted_tx);
              uint64_t commited_tx = (stat_committed_tx_total-last_recorded_committed_tx);
              file_out << (curr_time-recorded_start_time)/1000 << ", " << ( ((double)commited_tx*1000) / ((double) (curr_time - last_recorded_nsec_time)) ) << std::endl;
              last_recorded_nsec_time = curr_time;
              last_recorded_attempted_tx = stat_attempted_tx_total;
              last_recorded_committed_tx = stat_committed_tx_total;
         }

      #endif
  }
}

// Run actual transactions
void RunTx(coro_yield_t& yield, coro_id_t coro_id) {
  double total_msr_us = 0;
  // Each coroutine has a dtx: Each coroutine is a coordinator
  DTX* dtx = new DTX(meta_man, qp_man, thread_gid+(num_crashes*thread_num), coro_id, coro_sched, rdma_buffer_allocator,
                     log_offset_allocator, addr_cache);

  dtx->InitFailedList(failed_id_list);
  dtx->InitCrashEmu(&crash_emu);

  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;

  // // warm up
  // RDMA_LOG(INFO) << "WARM UP...";
  // for (int i=0;i<ATTEMPED_NUM;i++) {
  //   uint64_t iter = ++tx_id_generator;
  //   TxLockContention(yield, iter, dtx);
  // }
  // RDMA_LOG(INFO) << "WARM UP finish";

  // Running transactions
  if( (num_crashes + num_mem_crashes) ==0 ) //only before any failures.
      clock_gettime(CLOCK_REALTIME, &msr_start);

  #ifdef STATS
    //RDMA_LOG(INFO) << "Stats Starting" ;
    double tx_usec_start = (msr_start.tv_sec) * 1000000 + (double)(msr_start.tv_nsec) / 1000;
    window_start_time[local_thread_id] = tx_usec_start;  // in miro seconds   
  #endif

  while (true) {
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;
    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    tx_committed = TxLockContention(yield, iter, dtx);
    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
      // latency->update(tx_usec * lat_multiplier);
      // stat_committed_tx_total++;

            //adding to the 
      #ifdef STATS
        tx_attempted[local_thread_id] = stat_attempted_tx_total;
        tx_commited[local_thread_id] = stat_committed_tx_total;
        double usec = (tx_end_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec) / 1000;  // in miro seconds ;
        window_curr_time[local_thread_id] =   usec;
      
        REC * atomic_record = new REC(); // dynamic allocations within stacks
        atomic_record->txs = stat_committed_tx_total;
        atomic_record->usecs = usec;

        record_ptrs[local_thread_id] = atomic_record; // pointer change. 

      #endif

    }
    // Stat after a million of transactions finish
    if (stat_attempted_tx_total == ATTEMPED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
      double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;

      total_msr_us = msr_sec * 1000000;

      double attemp_tput = (double)stat_attempted_tx_total / msr_sec;
      double tx_tput = (double)stat_committed_tx_total / msr_sec;

      std::string thread_num_coro_num;
      if (coro_num < 10) {
        thread_num_coro_num = std::to_string(thread_num) + "_0" + std::to_string(coro_num);
      } else {
        thread_num_coro_num = std::to_string(thread_num) + "_" + std::to_string(coro_num);
      }
      std::string log_file_path = "../../../bench_results/MICRO/" + thread_num_coro_num + "/output.txt";

      std::ofstream output_of;
      output_of.open(log_file_path, std::ios::app);

      std::sort(timer, timer + stat_committed_tx_total);
      double percentile_50 = timer[stat_committed_tx_total / 2];
      double percentile_99 = timer[stat_committed_tx_total * 99 / 100];
      mux.lock();
      tid_vec.push_back(thread_gid);
      attemp_tp_vec.push_back(attemp_tput);
      tp_vec.push_back(tx_tput);
      medianlat_vec.push_back(percentile_50);
      taillat_vec.push_back(percentile_99);
      mux.unlock();
      output_of << tx_tput << " " << percentile_50 << " " << percentile_99 << std::endl;
      output_of.close();
      // std::cout << tx_tput << " " << percentile_50 << " " << percentile_99 << std::endl;

      // Output the local addr cache miss rate
      log_file_path = "../../../bench_results/MICRO/" + thread_num_coro_num + "/miss_rate.txt";
      output_of.open(log_file_path, std::ios::app);
      output_of << double(dtx->miss_local_cache_times) / (dtx->hit_local_cache_times + dtx->miss_local_cache_times) << std::endl;
      output_of.close();

      log_file_path = "../../../bench_results/MICRO/" + thread_num_coro_num + "/cache_size.txt";
      output_of.open(log_file_path, std::ios::app);
      output_of << dtx->GetAddrCacheSize() << std::endl;
      output_of.close();

      break;
    }


     #ifdef CRASH_ENABLE
      if( (stat_attempted_tx_total >= next_crash_count) && (thread_gid==0)){
          printf("Crashed-Recovery Start \n");

          crash_emu = true;
           __asm__ __volatile__("mfence":::"memory"); //NEEDED HERE
          //send a crash signal to failure detector.         
          // send a signal and get the ack back
          usleep(56); //
          usleep(5000);
          //Danger
          coro_sched->PollCompletion();

          usleep(56); //
          
          #ifdef EEL
              //update the failed_id_list
              t_id_t start_thread_id = 0;
              t_id_t end_thread_id = (thread_num/2);

              dtx->TxUndoRecovery(yield, addr_caches, start_thread_id, end_thread_id);

              usleep(56); // grpc latency.

              for(int f=0; f < (thread_num/2); f++){

                failed_id_list[(num_crashes*thread_num)+ f +1]= true; // set failed locks ids
              }
              
              usleep(56); // grpc latency

          #else
             dtx->TxUndoRecovery(yield, addr_caches);
            dtx->TxLatchRecovery(yield, addr_caches);
          #endif

          usleep(56); //  
          //printf("Wait starts \n");          
          //printf("Wait Ends \n");
          //Todo- need to update thethread_gid. just shift it. 
          //UPDATE THE NEW BASE TID

          num_crashes++;
          next_crash_count += CRASH_INTERVAL;
          new_base_tid = thread_gid + (thread_num*num_crashes);
          dtx->ChangeCurrentTID(new_base_tid);
          #ifdef NORESUME
	  	usleep(10000000);
	  #endif
          crash_emu = false;
           __asm__ __volatile__("mfence":::"memory"); //NEEDED HERE
          printf("Crashed-Recovery End \n");

          //Dangerouns Code
          break;
      }


      bool set=false;
      while(crash_emu && (thread_gid < (thread_num/2)) )
      {
              if(!set) {
                      //RDMA_LOG(INFO) << "Stopping Thread - " << thread_gid; 
                      set=true;

              }
             __asm__ __volatile__("mfence":::"memory");
      }

      if(set){
        dtx->ChangeCurrentTID(thread_gid+ (thread_num*num_crashes) );
              //RDMA_LOG(INFO) << "Resuming Thread - " << thread_gid << "Inverted Thread Back " << thread_gid+thread_num; 
        //dtx->ChangeCurrentTID(thread_gid+thread_num);
        set=false;
        //usleep(1000000);

        //for resume. 
        //Potential Dangerours
        usleep(10);
        coro_sched->PollCompletion();
        break;
      }
              ; // stop all other threads from progressing 

    #endif




    #ifdef MEM_FAILURES
      #ifdef MEM_CRASH_ENABLE
        
        if(thread_gid==0 && ((stat_attempted_tx_total==(ATTEMPED_NUM/3)) && !mem_crash_enable) && (num_mem_crashes==0)){  
          mem_crash_enable = true;
          mem_crash_coros++ ;
           __asm__ __volatile__("mfence":::"memory");

          //sleep(50);

          coro_sched->Yield(yield, coro_id, true); //r emmeber to use waiting one. not
  
          //dtx->TxUndoRecovery(yield, addr_caches, 0, STAT_NUM_MAX_THREADS);

          // TODO: We need to restart the entire thing. locks are lost. what if compute did not die. but resumes. IIL would not work.
          // Problem: there are transactions accessing different primaries. so we cannot stop without aborting. locks?
           //sleep(50);
           //break;
        }else if(mem_crash_enable){
            //all coros exept the one enable
            mem_crash_coros++;
           __asm__ __volatile__("mfence":::"memory");
             coro_sched->Yield(yield, coro_id, true);//r emmeber to use waiting one. not
        }


      #endif
    #endif

  }

  std::string thread_num_coro_num;
  if (coro_num < 10) {
    thread_num_coro_num = std::to_string(thread_num) + "_0" + std::to_string(coro_num);
  } else {
    thread_num_coro_num = std::to_string(thread_num) + "_" + std::to_string(coro_num);
  }
  uint64_t total_duration = 0;
  double average_lock_duration = 0;

  // only for test
#if LOCK_WAIT

  for (auto duration : dtx->lock_durations) {
    total_duration += duration;
  }

  std::string total_lock_duration_file = "../../../bench_results/MICRO/" + thread_num_coro_num + "/total_lock_duration.txt";
  std::ofstream of;
  of.open(total_lock_duration_file, std::ios::app);
  std::sort(dtx->lock_durations.begin(), dtx->lock_durations.end());
  auto min_lock_duration = dtx->lock_durations.empty() ? 0 : dtx->lock_durations[0];
  auto max_lock_duration = dtx->lock_durations.empty() ? 0 : dtx->lock_durations[dtx->lock_durations.size() - 1];
  average_lock_duration = dtx->lock_durations.empty() ? 0 : (double)total_duration / dtx->lock_durations.size();
  lock_durations[thread_local_id] = average_lock_duration;
  of << thread_gid << " " << average_lock_duration << " " << max_lock_duration << std::endl;
  of.close();
#endif

  // only for test
#if INV_BUSY_WAIT
  total_duration = 0;
  for (auto duration : dtx->invisible_durations) {
    total_duration += duration;
  }
  std::string total_inv_duration_file = "../../../bench_results/MICRO/" + thread_num_coro_num + "/total_inv_duration.txt";
  std::ofstream ofs;
  ofs.open(total_inv_duration_file, std::ios::app);
  std::sort(dtx->invisible_durations.begin(), dtx->invisible_durations.end());
  auto min_inv_duration = dtx->invisible_durations.empty() ? 0 : dtx->invisible_durations[0];
  auto max_inv_duration = dtx->invisible_durations.empty() ? 0 : dtx->invisible_durations[dtx->invisible_durations.size() - 1];
  auto average_inv_duration = dtx->invisible_durations.empty() ? 0 : (double)total_duration / dtx->invisible_durations.size();

  double total_execution_time = 0;
  for (uint64_t i = 0; i < stat_committed_tx_total; i++) {
    total_execution_time += timer[i];
  }

  uint64_t re_read_times = 0;
  for (uint64_t i = 0; i < dtx->invisible_reread.size(); i++) {
    re_read_times += dtx->invisible_reread[i];
  }

  uint64_t avg_re_read_times = dtx->invisible_reread.empty() ? 0 : re_read_times / dtx->invisible_reread.size();

  auto average_execution_time = (total_execution_time / stat_committed_tx_total) * 1000000;  // us

  ofs << thread_gid << " " << average_inv_duration << " " << max_inv_duration << " " << average_execution_time << " " << avg_re_read_times << " " << double(total_duration / total_execution_time) << " " << (double)(total_duration / total_msr_us) << std::endl;

  ofs.close();
#endif

  /********************************** Stat end *****************************************/
  
  #ifdef UNDO_RECOVERY_BENCH
  
    if(thread_gid==0){
      usleep(5000000);
      printf("Starting Coordinator-Side Undo Recovery at gid=0.. \n");
      clock_gettime(CLOCK_REALTIME, &msr_start);
      dtx->TxUndoRecovery(yield, addr_caches);
      clock_gettime(CLOCK_REALTIME, &msr_end);
      double rec_msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
      printf("Recovery time - %f \n", rec_msr_sec);
    }
  #endif


  #ifdef LATCH_RECOVERY_BENCH
    if(thread_gid==0){
      printf("Starting Coordinator-Side Latch Recovery at gid=0.. \n");
      clock_gettime(CLOCK_REALTIME, &msr_start);
      dtx->TxLatchRecovery(yield, addr_caches);
      clock_gettime(CLOCK_REALTIME, &msr_end);
      double rec_msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
      printf("Recovery time - %f \n", rec_msr_sec);
    }
  #endif



  delete dtx;
}

void run_thread(struct thread_params* params) {
  stop_run = false;
  thread_gid = params->thread_global_id;
  thread_local_id = params->thread_local_id;
  thread_num = params->thread_num_per_machine;
  micro_client = params->micro_client;
  meta_man = params->global_meta_man;
  coro_num = (coro_id_t)params->coro_num;
  coro_sched = new CoroutineScheduler(thread_gid, coro_num);
  auto alloc_rdma_region_range = params->global_rdma_region->GetThreadLocalRegion(params->thread_local_id);


  addr_cache = new AddrCache();
  addr_caches[params->thread_local_id] = addr_cache;

  rdma_buffer_allocator = new RDMABufferAllocator(alloc_rdma_region_range.first, alloc_rdma_region_range.second);
  //log_offset_allocator = new LogOffsetAllocator(thread_gid, params->total_thread_num);
  //DAM - new allocator
  log_offset_allocator = new LogOffsetAllocator(thread_gid, params->total_thread_num, coro_num);
  // latency = new Latency();
  timer = new double[ATTEMPED_NUM]();

  /* Initialize Zipf generator */
  uint64_t zipf_seed = 2 * thread_gid * GetCPUCycle();
  uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
  std::string config_filepath = "../../../config/micro_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto micro_conf = json_config.get("micro");
  num_keys_global = align_pow2(micro_conf.get("num_keys").get_int64());
  auto zipf_theta = micro_conf.get("zipf_theta").get_double();
  is_skewed = micro_conf.get("is_skewed").get_bool();
  write_ratio = micro_conf.get("write_ratio").get_uint64();
  data_set_size = micro_conf.get("data_set_size").get_uint64();
  zipf_gen = new ZipfGen(num_keys_global, zipf_theta, zipf_seed & zipf_seed_mask);

  seed = 0xdeadbeef + thread_gid;  // Guarantee that each thread has a global different initial seed
  workgen_arr = micro_client->CreateWorkgenArray();

  // Init coroutines
  for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++) {
    coro_sched->coro_array[coro_i].coro_id = coro_i;
    // Bind workload to coroutine
    if (coro_i == POLL_ROUTINE_ID) {
      coro_sched->coro_array[coro_i].func = coro_call_t(bind(PollCompletion, _1));
    } else {
      coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunTx, _1, coro_i));
    }
  }

  // Link all coroutines via pointers in a loop manner
  coro_sched->LoopLinkCoroutine(coro_num);

  // Build qp connection in thread granularity
  qp_man = new QPManager(thread_gid);
  qp_man->BuildQPConnection(meta_man);

  // Sync qp connections in one compute node before running transactions
  connected_t_num += 1;
  while (connected_t_num != thread_num) {
    usleep(2000);
  }

    //initializing the file
  #ifdef CRASH_TPUT
    std::string file_name = "results/result_"+ std::to_string(thread_gid) + ".txt";
    file_out.open(file_name.c_str(), std::ios::app);
    //local_thread_id =    thread_gid - (machine_id_*thread_num_per_machine_); 
  #endif
  
  #ifdef STATS
    local_thread_id =  thread_gid - (machine_id_*thread_num_per_machine_);
  #endif

  // Start the first coroutine
  coro_sched->coro_array[0].func();


 //Dangerouns
  while(stat_attempted_tx_total < ATTEMPED_NUM){

      //RDMA_LOG(INFO) << "START " << thread_gid << "  " << next_crash_count;

      #ifdef MEM_FAILURES
          #ifdef MEM_CRASH_ENABLE
           //sleep(1000000);
              if(thread_gid ==0){
                        usleep(5000);
			while(mem_crash_tnums < thread_num){
				uint64_t m = mem_crash_tnums;
				__asm__ __volatile__("mfence":::"memory");
			}

			//usleep(1000000);
                        RDMA_LOG(INFO) << "Waiting... ";

                        usleep(112);

                        //Recovery
                        usleep(112);
                        DTX* dtx = new DTX(meta_man, qp_man, 0, 1, coro_sched, rdma_buffer_allocator, log_offset_allocator, addr_cache);
                        coro_yield_t yield;
                        dtx->TxUndoRecovery(yield, addr_caches, 0 , thread_num);
                        //
                        usleep(112);


                        meta_man->removeMemServer(1);
                        mem_crash_enable = false;
                        mem_crash_coros = 0;
			mem_crash_tnums=0;
			num_mem_crashes ++;
                        __asm__ __volatile__("mfence":::"memory");
                }
                while (mem_crash_enable){
                    	bool b=mem_crash_enable;
			 __asm__ __volatile__("mfence":::"memory");
		}
          #endif
        #endif

      coro_sched = new CoroutineScheduler(thread_gid, coro_num);


                 // Init coroutines
      for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++) {
        coro_sched->coro_array[coro_i].coro_id = coro_i;
        // Bind workload to coroutine
        if (coro_i == POLL_ROUTINE_ID) {
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(PollCompletion, _1));
        } else {
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunTx, _1, coro_i));
        }
      }

                 coro_sched->LoopLinkCoroutine(coro_num);
          coro_sched->coro_array[0].func();

         //RDMA_LOG(INFO) << "THREAD "<< thread_gid << " " << stat_attempted_tx_total;
  }


  // Stop running
  stop_run = true;

  #ifdef STATS
    thread_done[local_thread_id] = true;
  #endif

  RDMA_LOG(DBG) << "Thread: " << thread_gid << ". Loop RDMA alloc times: " ; //<< rdma_buffer_allocator->loop_times;
  #ifdef CRASH_TPUT  
    file_out.close();
  #endif  


  // Clean
  // delete latency;
  delete[] timer;
  //delete addr_cache;
  delete[] workgen_arr;
  delete coro_sched;
  delete zipf_gen;
}
