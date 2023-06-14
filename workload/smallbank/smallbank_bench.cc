// Author: Ming Zhang
// Copyright (c) 2021

#include "smallbank/smallbank_bench.h"

#include <atomic>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>

#include "allocator/buffer_allocator.h"
#include "allocator/log_allocator.h"
#include "connection/qp_manager.h"
#include "dtx/dtx.h"
// #include "util/latency.h"
//#define CRASH_TPUT
using namespace std::placeholders;
#define STAT_NUM_MAX_THREADS 128
// All the functions are executed in each thread

//For Crash TPUT
#ifdef STATS
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
extern std::mutex mux;

extern std::vector<t_id_t> tid_vec;
extern std::vector<double> attemp_tp_vec;
extern std::vector<double> tp_vec;
extern std::vector<double> medianlat_vec;
extern std::vector<double> taillat_vec;


extern AddrCache**  addr_caches;
extern bool * failed_id_list;

__thread size_t ATTEMPTED_NUM;
__thread uint64_t seed; /* Thread-global random seed */
__thread t_id_t thread_gid;
__thread t_id_t thread_num;
__thread SmallBank* smallbank_client;
__thread MetaManager* meta_man;
__thread QPManager* qp_man;
__thread RDMABufferAllocator* rdma_buffer_allocator;
__thread LogOffsetAllocator* log_offset_allocator;

//#ifdef UNDO_RECOVERY
//extern AddrCache* addr_cache;
//extern std::mutex cache_mux; 
//#else

__thread AddrCache* addr_cache;

__thread SmallBankTxType* workgen_arr;

__thread coro_id_t coro_num;
__thread CoroutineScheduler* coro_sched;  // Each transaction thread has a coroutine scheduler
__thread bool stop_run;

// Performance measurement (thread granularity)
__thread struct timespec msr_start, msr_end;
// __thread Latency* latency;
__thread double* timer;
// const int lat_multiplier = 10; // For sub-microsecond latency measurement
__thread uint64_t stat_attempted_tx_total = 0;  // Issued transaction number
__thread uint64_t stat_committed_tx_total = 0;  // Committed transaction number

//__thread uint64_t window_attempted_tx_total = 0;  // Issued transaction number
//__thread uint64_t window_committed_tx_total = 0;  // Committed transaction number







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
#endif


const coro_id_t POLL_ROUTINE_ID = 0;            // The poll coroutine ID

/******************** The business logic (Transaction) start ********************/

bool TxAmalgamate(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);

  /* Transaction parameters */
  uint64_t acct_id_0, acct_id_1;
  smallbank_client->get_two_accounts(&seed, &acct_id_0, &acct_id_1);

  /* Read from savings and checking tables for acct_id_0 */
  smallbank_savings_key_t sav_key_0;
  sav_key_0.acct_id = acct_id_0;
  auto sav_obj_0 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key_0.item_key);
  dtx->AddToReadWriteSet(sav_obj_0);

  smallbank_checking_key_t chk_key_0;
  chk_key_0.acct_id = acct_id_0;
  auto chk_obj_0 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_0.item_key);
  dtx->AddToReadWriteSet(chk_obj_0);

  /* Read from checking account for acct_id_1 */
  smallbank_checking_key_t chk_key_1;
  chk_key_1.acct_id = acct_id_1;
  auto chk_obj_1 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_1.item_key);
  dtx->AddToReadWriteSet(chk_obj_1);

  if (!dtx->TxExe(yield)) return false;

  /* If we are here, execution succeeded and we have locks */
  smallbank_savings_val_t* sav_val_0 = (smallbank_savings_val_t*)sav_obj_0->value;
  smallbank_checking_val_t* chk_val_0 = (smallbank_checking_val_t*)chk_obj_0->value;
  smallbank_checking_val_t* chk_val_1 = (smallbank_checking_val_t*)chk_obj_1->value;
  if (sav_val_0->magic != smallbank_savings_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (chk_val_0->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (chk_val_1->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(sav_val_0->magic == smallbank_savings_magic);
  // assert(chk_val_0->magic == smallbank_checking_magic);
  // assert(chk_val_1->magic == smallbank_checking_magic);

  /* Increase acct_id_1's kBalance and set acct_id_0's balances to 0 */
  chk_val_1->bal += (sav_val_0->bal + chk_val_0->bal);

  sav_val_0->bal = 0;
  chk_val_0->bal = 0;

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

/* Calculate the sum of saving and checking kBalance */
bool TxBalance(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);

  /* Transaction parameters */
  uint64_t acct_id;
  smallbank_client->get_account(&seed, &acct_id);

  /* Read from savings and checking tables */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  auto sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key.item_key);
  dtx->AddToReadOnlySet(sav_obj);

  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  auto chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  dtx->AddToReadOnlySet(chk_obj);

  if (!dtx->TxExe(yield)) return false;

  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)sav_obj->value;
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj->value;
  if (sav_val->magic != smallbank_savings_magic) {
    RDMA_LOG(INFO) << "read value: " << sav_val;
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (chk_val->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(sav_val->magic == smallbank_savings_magic);
  // assert(chk_val->magic == smallbank_checking_magic);

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

/* Add $1.3 to acct_id's checking account */
bool TxDepositChecking(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);

  /* Transaction parameters */
  uint64_t acct_id;
  smallbank_client->get_account(&seed, &acct_id);
  float amount = 1.3;

  /* Read from checking table */
  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  auto chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  dtx->AddToReadWriteSet(chk_obj);

  if (!dtx->TxExe(yield)) return false;

  /* If we are here, execution succeeded and we have a lock*/
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj->value;
  if (chk_val->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(chk_val->magic == smallbank_checking_magic);

  chk_val->bal += amount; /* Update checking kBalance */

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

/* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
bool TxSendPayment(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);

  /* Transaction parameters: send money from acct_id_0 to acct_id_1 */
  uint64_t acct_id_0, acct_id_1;
  smallbank_client->get_two_accounts(&seed, &acct_id_0, &acct_id_1);
  float amount = 5.0;

  /* Read from checking table */
  smallbank_checking_key_t chk_key_0;
  chk_key_0.acct_id = acct_id_0;
  auto chk_obj_0 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_0.item_key);
  dtx->AddToReadWriteSet(chk_obj_0);

  /* Read from checking account for acct_id_1 */
  smallbank_checking_key_t chk_key_1;
  chk_key_1.acct_id = acct_id_1;
  auto chk_obj_1 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_1.item_key);
  dtx->AddToReadWriteSet(chk_obj_1);

  if (!dtx->TxExe(yield)) return false;

  /* if we are here, execution succeeded and we have locks */
  smallbank_checking_val_t* chk_val_0 = (smallbank_checking_val_t*)chk_obj_0->value;
  smallbank_checking_val_t* chk_val_1 = (smallbank_checking_val_t*)chk_obj_1->value;
  if (chk_val_0->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (chk_val_1->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(chk_val_0->magic == smallbank_checking_magic);
  // assert(chk_val_1->magic == smallbank_checking_magic);

  if (chk_val_0->bal < amount) {
    dtx->TxAbortReadWrite(yield);
    return false;
  }

  chk_val_0->bal -= amount; /* Debit */
  chk_val_1->bal += amount; /* Credit */

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

/* Add $20 to acct_id's saving's account */
bool TxTransactSaving(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);

  /* Transaction parameters */
  uint64_t acct_id;
  smallbank_client->get_account(&seed, &acct_id);
  float amount = 20.20;

  /* Read from saving table */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  auto sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key.item_key);
  dtx->AddToReadWriteSet(sav_obj);
  if (!dtx->TxExe(yield)) return false;

  /* If we are here, execution succeeded and we have a lock */
  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)sav_obj->value;
  if (sav_val->magic != smallbank_savings_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(sav_val->magic == smallbank_savings_magic);

  sav_val->bal += amount; /* Update saving kBalance */

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

/* Read saving and checking kBalance + update checking kBalance unconditionally */
bool TxWriteCheck(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);

  /* Transaction parameters */
  uint64_t acct_id;
  smallbank_client->get_account(&seed, &acct_id);
  float amount = 5.0;

  /* Read from savings. Read checking record for update. */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  auto sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key.item_key);
  dtx->AddToReadOnlySet(sav_obj);

  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  auto chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  dtx->AddToReadWriteSet(chk_obj);

  if (!dtx->TxExe(yield)) return false;

  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)sav_obj->value;
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj->value;
  if (sav_val->magic != smallbank_savings_magic) {
    RDMA_LOG(INFO) << "read value: " << sav_val;
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (chk_val->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(sav_val->magic == smallbank_savings_magic);
  // assert(chk_val->magic == smallbank_checking_magic);

  if (sav_val->bal + chk_val->bal < amount) {
    chk_val->bal -= (amount + 1);
  } else {
    chk_val->bal -= amount;
  }

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

/******************** The business logic (Transaction) end ********************/

void PollCompletion(coro_yield_t& yield) {

  
  // To do. we need to stop here for crashes. 


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
      if(mem_crash_enable && (mem_crash_coros == (coro_num-1) ) ){
         //mem_crash_enable = false;
        mem_crash_coros = 0;
	mem_crash_tnums++;
         __asm__ __volatile__("mfence":::"memory");
             RDMA_LOG(WARNING) << "Compute Server Sign Off: Bye bye " << thread_gid <<  " " <<mem_crash_tnums ; //RDMA_LOG(INFO) << "THREAD "<< thread_gid << " " << stat_attempted_tx_total;
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
  // Each coroutine has a dtx: Each coroutine is a coordinator
  DTX* dtx = new DTX(meta_man, qp_man, thread_gid+(num_crashes*thread_num), coro_id, coro_sched, rdma_buffer_allocator,
                     log_offset_allocator, addr_cache);

  dtx->InitFailedList(failed_id_list);
  dtx->InitCrashEmu(&crash_emu);

  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;

  //This to log the throughput in crash-recovery window. this is run by non-faulty ones. 
  //we only need to do this with the single coroutine. 

  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  
  #ifdef STATS
    double tx_usec_start = (msr_start.tv_sec) * 1000000 + (double)(msr_start.tv_nsec) / 1000;
    window_start_time[local_thread_id] = tx_usec_start;  // in miro seconds   
  #endif

  while (true) {
    SmallBankTxType tx_type = workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;

#if ABORT_DISCARD
    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    switch (tx_type) {
      case SmallBankTxType::kAmalgamate:
        tx_committed = TxAmalgamate(yield, iter, dtx);
        break;
      case SmallBankTxType::kBalance:
        tx_committed = TxBalance(yield, iter, dtx);
        break;
      case SmallBankTxType::kDepositChecking:
        tx_committed = TxDepositChecking(yield, iter, dtx);
        break;
      case SmallBankTxType::kSendPayment:
        tx_committed = TxSendPayment(yield, iter, dtx);
        break;
      case SmallBankTxType::kTransactSaving:
        tx_committed = TxTransactSaving(yield, iter, dtx);
        break;
      case SmallBankTxType::kWriteCheck:
        tx_committed = TxWriteCheck(yield, iter, dtx);
        break;
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }
#else
    switch (tx_type) {
      case SmallBankTxType::kAmalgamate: {
        do {
          clock_gettime(CLOCK_REALTIME, &tx_start_time);
          tx_committed = TxAmalgamate(yield, iter, dtx);
        } while (tx_committed != true);
        break;
      }
      case SmallBankTxType::kBalance: {
        do {
          clock_gettime(CLOCK_REALTIME, &tx_start_time);
          tx_committed = TxBalance(yield, iter, dtx);
        } while (tx_committed != true);
        break;
      }
      case SmallBankTxType::kDepositChecking: {
        do {
          clock_gettime(CLOCK_REALTIME, &tx_start_time);
          tx_committed = TxDepositChecking(yield, iter, dtx);
        } while (tx_committed != true);
        break;
      }
      case SmallBankTxType::kSendPayment: {
        do {
          clock_gettime(CLOCK_REALTIME, &tx_start_time);
          tx_committed = TxSendPayment(yield, iter, dtx);
        } while (tx_committed != true);
        break;
      }
      case SmallBankTxType::kTransactSaving: {
        do {
          clock_gettime(CLOCK_REALTIME, &tx_start_time);
          tx_committed = TxTransactSaving(yield, iter, dtx);
        } while (tx_committed != true);
        break;
      }
      case SmallBankTxType::kWriteCheck: {
        do {
          clock_gettime(CLOCK_REALTIME, &tx_start_time);
          tx_committed = TxWriteCheck(yield, iter, dtx);
        } while (tx_committed != true);
        break;
      }
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }

#endif

    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;

      timer[stat_committed_tx_total++] = tx_usec;
      // latency->update(tx_usec * lat_multiplier);
      // stat_committed_tx_total++;


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

    //

    // Stat after a million of transactions finish
    if (stat_attempted_tx_total == ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
      double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;

      double attemp_tput = (double)stat_attempted_tx_total / msr_sec;
      double tx_tput = (double)stat_committed_tx_total / msr_sec;

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

        if(thread_gid==0 && ((stat_attempted_tx_total==(ATTEMPTED_NUM/10)) && (!mem_crash_enable)) ){
          RDMA_LOG(INFO) << "Starting Mem Crash " ;
            mem_crash_enable = true;
           mem_crash_coros++;
          __asm__ __volatile__("mfence":::"memory");

          //sleep(50);
         // mem_crash_count++; 
	 // RDMA_LOG(INFO) << "MEM CRASH:  thread " << thread_gid << " coro " <<coro_id  << " mem_crash_count " << mem_crash_coros ;

          coro_sched->Yield(yield, coro_id, true);

          //dtx->TxUndoRecovery(yield, addr_caches, 0, STAT_NUM_MAX_THREADS);

          // TODO: We need to restart the entire thing. locks are lost. what if compute did not die. but resumes. IIL would not work.
          // Problem: there are transactions accessing different primaries. so we cannot stop without aborting. locks?
           //sleep(50);
           //break;
        }else if(mem_crash_enable){
            //all coros exept the one enable
             mem_crash_coros++;
              __asm__ __volatile__("mfence":::"memory");
            // RDMA_LOG(INFO) << "MEM CRASH:  thread " << thread_gid << " coro " <<coro_id << " mem_crash_count " << mem_crash_coros;
             coro_sched->Yield(yield, coro_id, true);
        }


      #endif
    #endif	      


    /********************************** Stat end *****************************************/
  }

    #ifdef UNDO_RECOVERY_BENCH
  
    if(thread_gid==0){
      usleep(5000000);
      printf("Starting Coordinator-Side Undo Recovery at gid=0.. \n");
      clock_gettime(CLOCK_REALTIME, &msr_start);
      dtx->TxUndoRecovery(yield,addr_caches);
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
  std::string config_filepath = "../../../config/smallbank_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto conf = json_config.get("smallbank");
  ATTEMPTED_NUM = conf.get("attempted_num").get_uint64();

  stop_run = false;
  thread_gid = params->thread_global_id;
  thread_num = params->thread_num_per_machine;
  smallbank_client = params->smallbank_client;
  meta_man = params->global_meta_man;
  coro_num = (coro_id_t)params->coro_num;
  coro_sched = new CoroutineScheduler(thread_gid, coro_num);
  auto alloc_rdma_region_range = params->global_rdma_region->GetThreadLocalRegion(params->thread_local_id);

  //#ifndef UNDO_RECOVERY
    addr_cache = new AddrCache();
    addr_caches[params->thread_local_id] = addr_cache;
  //#endif

  rdma_buffer_allocator = new RDMABufferAllocator(alloc_rdma_region_range.first, alloc_rdma_region_range.second);
  log_offset_allocator = new LogOffsetAllocator(thread_gid, params->total_thread_num, coro_num);
  // latency = new Latency();
  timer = new double[ATTEMPTED_NUM]();

  seed = 0xdeadbeef + thread_gid;  // Guarantee that each thread has a global different initial seed
  workgen_arr = smallbank_client->CreateWorkgenArray();

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
  while(stat_attempted_tx_total < ATTEMPTED_NUM){
      
       	#ifdef MEM_FAILURES	
	  if(thread_gid == 0){
                        usleep(5000);
			while((mem_crash_tnums) < thread_num){
				uint64_t m = mem_crash_tnums;	
				__asm__ __volatile__("mfence":::"memory");

			}
			
			 __asm__ __volatile__("mfence":::"memory");
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
			mem_crash_tnums = 0;
                        __asm__ __volatile__("mfence":::"memory");
                }

		
                while (mem_crash_enable){
			bool b = mem_crash_enable;
                __asm__ __volatile__("mfence":::"memory");
        	}

        #endif

      //RDMA_LOG(INFO) << "START " << thread_gid << "  " << next_crash_count;
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

   RDMA_LOG(DBG) << "Thread: " << thread_gid << ". Loop RDMA alloc times: "; // << rdma_buffer_allocator->loop_times;
  #ifdef CRASH_TPUT  
    file_out.close();
  #endif  

  // Clean
  // delete latency;
  delete[] timer;
  //delete addr_cache;
  delete[] workgen_arr;
  delete coro_sched;
}
