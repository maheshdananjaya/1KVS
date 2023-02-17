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

__thread int litmus=2; //1 2 3 4


extern bool crash_emu;

#ifdef CRASH_TPUT
thread_local std::ofstream file_out;// per thread file writes
__thread const double window_time_ns=500000; // exmaple 100 microseconds -  
__thread double last_recorded_nsec_time = 0;
__thread uint64_t last_recorded_attempted_tx = 0;  // Issued transaction number
__thread uint64_t last_recorded_committed_tx = 0;  // Committed transaction number
__thread double curr_time =0;
__thread double recorded_start_time=0;
#endif

/******************** The business logic (Transaction) start ********************/

struct DataItemDuplicate {
  DataItemPtr data_item_ptr;
  bool is_dup;
};

//array of locations for testing. 

//create a test with random sequence. imcrease the coverage. 

bool StartTheTest(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {

}


//if the last one is delete. insert must be on the way. 
bool Litmus1(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {

  //No more random writes. pick any combinations. 
  //micro_key.item_key = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);
  //
  dtx->TxBegin(tx_id);
  bool is_write[data_set_size]; 
  DataItemPtr micro_objs[data_set_size];

    //CRASH points

   //DelayRandom();
  
    for (uint64_t i = 0; i < data_set_size; i++) {
      micro_key_t micro_key;
      micro_key.item_key = (itemkey_t) i+1;
      assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);
      micro_objs[i] = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);

      //do this with deletes and inserts radomly.

      dtx->AddToReadWriteSet(micro_objs[i]);
      is_write[i] = true;

    }
  
      //CRASH points 1: CRASH must happen in the background rather thanhere.

    if (!dtx->TxExe(yield)) {
      // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
      return false;
    }
 
    //CRASH points 2


    uint64_t value_v = (uint64_t)FastRand(&seed) & (num_keys_global - 1);
  
    for (uint64_t i = 0; i < data_set_size; i++) {

      //randomly insert/delete all objects.
      micro_val_t* micro_val = (micro_val_t*) micro_objs[i]->value;
      micro_val->magic[1] = value_v; // new version value for all writes.
    }

    //CRASH points 3

    bool commit_status = dtx->TxCommit(yield); // We also need to emulate crashes within commit. use interrupts.

    //CRASH points 4

    //CRASH points 5

    return commit_status;

}


//Assert nebver fails in the middle. emulaitons;
bool Assert1(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {

  //assertion function for the mitmus testing
  //trick: either stop the world or lock all read only objects.
  dtx->TxBegin(tx_id);
  bool is_write[data_set_size]; 
  DataItemPtr micro_objs[data_set_size];
 
    for (uint64_t i = 0; i < data_set_size; i++) {
      micro_key_t micro_key;
      micro_key.item_key = (itemkey_t) i+1;
      assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);
      micro_objs[i] = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);
     
      //Perssimistic Reads
      dtx->AddToReadWriteSet(micro_objs[i]);
      is_write[i] = true;

    }
  
    //Lock and Read
    if (!dtx->TxExeLitmus(yield)) {
      // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
         //dtx->AssertAbort();
      return false;
    } 
   
    //Assert versions
    uint64_t value_v=0;
    for (uint64_t i = 0; i < data_set_size; i++) {
      micro_val_t* micro_val = (micro_val_t*) micro_objs[i]->value;
      if(i==0)
        value_v= micro_val->magic[1]; // new version value for all writes.
      else
        assert(micro_val->magic[1] == value_v); //local asserts
    }

    //bool commit_status = dtx->TxCommit(yield); // We also need to emulate crashes within commit. use interrupts.
    //Unlock should be there. 

   dtx->AssertAbort(yield); //unlock all the places

  return true;

}


//Reall litmus two has two transactions. T1 and T2.

bool Litmus2_T1(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {

  //No more random writes. pick any combinations. 
  //micro_key.item_key = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);
  //
  dtx->TxBegin(tx_id);
  data_set_size=2;

  bool is_write[data_set_size]; 
  DataItemPtr micro_objs[data_set_size];

    //CRASH points
  
    
      micro_key_t micro_key_x;
      micro_key_x.item_key = (itemkey_t) 1; //X
      assert(micro_key_x.item_key >= 0 && micro_key_x.item_key < num_keys_global);
      micro_objs[0] = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key_x.item_key);
      dtx->AddToReadOnlySet(micro_objs[0]);
      is_write[0] = false;

       

    //CRASH points 1

    if (!dtx->TxExe(yield)) {
      // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
      return false;
    }

    micro_val_t* micro_val_x = (micro_val_t*) micro_objs[0]->value; // while x value


      micro_key_t micro_key_y;
      micro_key_y.item_key = (itemkey_t) 2; //X
      assert(micro_key_y.item_key >= 0 && micro_key_y.item_key < num_keys_global);
      micro_objs[1] = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key_y.item_key);
      dtx->AddToReadWriteSet(micro_objs[1]);
      is_write[1] = true;



         //CRASH points 1

    if (!dtx->TxExe(yield)) {
      // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
      return false;
    }


    micro_val_t* micro_val_y = (micro_val_t*) micro_objs[1]->value; // while x value

    RDMA_LOG(INFO) << "X= " <<  micro_val_x->magic[1] << " , Y= " <<   micro_val_y->magic[1];

    micro_val_y->magic[1] = (micro_val_x->magic[1] + 1); // new version value for all writes.

   //CRASH points 2


   //CRASH points 3

    bool commit_status = dtx->TxCommit(yield); // We also need to emulate crashes within commit. use interrupts.

    //CRASH points 4

    return commit_status;

}


bool Litmus2_T2(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {

  //No more random writes. pick any combinations. 
  //micro_key.item_key = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);
  //
  dtx->TxBegin(tx_id);
  data_set_size=2;

  bool is_write[data_set_size]; 
  DataItemPtr micro_objs[data_set_size];

    //CRASH points
  
    
      micro_key_t micro_key_y;
      micro_key_y.item_key = (itemkey_t) 2; //Y
      assert(micro_key_y.item_key >= 0 && micro_key_y.item_key < num_keys_global);
      micro_objs[0] = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key_y.item_key);
      dtx->AddToReadOnlySet(micro_objs[0]);
      is_write[0] = false;

       

    //CRASH points 1

    if (!dtx->TxExe(yield)) {
      // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
      return false;
    }

    micro_val_t* micro_val_y = (micro_val_t*) micro_objs[0]->value; // while x value


      micro_key_t micro_key_x;
      micro_key_x.item_key = (itemkey_t) 1; //X
      assert(micro_key_x.item_key >= 0 && micro_key_x.item_key < num_keys_global);
      micro_objs[1] = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key_x.item_key);
      dtx->AddToReadWriteSet(micro_objs[1]);
      is_write[1] = true;



         //CRASH points 1

    if (!dtx->TxExe(yield)) {
      // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
      return false;
    }


    micro_val_t* micro_val_x = (micro_val_t*) micro_objs[1]->value; // while x value
    RDMA_LOG(INFO) << "X= " <<  micro_val_x->magic[1] << " , Y= " <<   micro_val_y->magic[1];
    micro_val_x->magic[1] = (micro_val_y->magic[1]+1); // new version value for all writes.

   //CRASH points 2


   //CRASH points 3

    bool commit_status = dtx->TxCommit(yield); // We also need to emulate crashes within commit. use interrupts.

    //CRASH points 4

    return commit_status;

}


//Assertion for the litmus 2
bool Assert2(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {

  //assertion function for the mitmus testing
  //trick: either stop the world or lock all read only objects.
  dtx->TxBegin(tx_id);

  bool is_write[data_set_size]; 
  DataItemPtr micro_objs[data_set_size];
 
    for (uint64_t i = 0; i < 2; i++) {
      micro_key_t micro_key;
      micro_key.item_key = (itemkey_t) i+1;
      assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);
      micro_objs[i] = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);
     
      //Perssimistic Reads
      dtx->AddToReadWriteSet(micro_objs[i]);
      is_write[i] = true;

    }
  
    //Lock and Read
    if (!dtx->TxExeLitmus(yield)) {
      // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
         //dtx->AssertAbort();
      return false;
    } 
   
    //Assert versions
    uint64_t value_v=0;
    for (uint64_t i = 0; i < 2; i++) {
      micro_val_t* micro_val = (micro_val_t*) micro_objs[i]->value;
      if(i==0){
        value_v= micro_val->magic[1]; // new version value for all writes.
      }
      else{
          RDMA_LOG(INFO) << "X= " <<  value_v << " , Y= " <<  micro_val->magic[1];
          assert( ((value_v == 0) && (micro_val->magic[1] ==0)) || (micro_val->magic[1] != value_v)); //local asserts
          
      }    
    }

    //bool commit_status = dtx->TxCommit(yield); // We also need to emulate crashes within commit. use interrupts.
    //Unlock should be there. 

   dtx->AssertAbort(yield);


  return true;

}



//Reall litmus two has two transactions. T1 and T2.
bool Litmus3_T1(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {  
 //No more random writes. pick any combinations. 
  //micro_key.item_key = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);
  //
  dtx->TxBegin(tx_id);
  bool is_write[data_set_size]; 
  DataItemPtr micro_objs[data_set_size];

    //CRASH points

   //DelayRandom();
  
    for (uint64_t i = 0; i < data_set_size; i++) {
      micro_key_t micro_key;
      micro_key.item_key = (itemkey_t) i+1;
      assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);
      micro_objs[i] = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);

      //do this with deletes and inserts radomly.

      dtx->AddToReadWriteSet(micro_objs[i]);
      is_write[i] = true;

    }
  
      //CRASH points 1: CRASH must happen in the background rather thanhere.

    if (!dtx->TxExe(yield)) {
      // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
      return false;
    }
 
    //CRASH points 2


    uint64_t value_v = (uint64_t)FastRand(&seed) & (num_keys_global - 1); /// same value for everyone
  
    for (uint64_t i = 0; i < data_set_size; i++) {

      //randomly insert/delete all objects.
      micro_val_t* micro_val = (micro_val_t*) micro_objs[i]->value;
      micro_val->magic[1] = value_v; // new version value for all writes.
    }

    //CRASH points 3

    bool commit_status = dtx->TxCommit(yield); // We also need to emulate crashes within commit. use interrupts.

    //CRASH points 4

    //CRASH points 5

    return commit_status;

}

bool Litmus3_T2(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {

   //No more random writes. pick any combinations. 
  //micro_key.item_key = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);
  
  dtx->TxBegin(tx_id);
  bool is_write[data_set_size-1]; 
  DataItemPtr micro_objs[data_set_size-1];

    //CRASH points

   //DelayRandom();
  
    for (uint64_t i = 0; i < data_set_size-1; i++) {
      micro_key_t micro_key;
      micro_key.item_key = (itemkey_t) i+1;
      assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);
      micro_objs[i] = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);

      //do this with deletes and inserts radomly.

      dtx->AddToReadWriteSet(micro_objs[i]);
      is_write[i] = true;

    }
  
      //CRASH points 1: CRASH must happen in the background rather thanhere.

    if (!dtx->TxExe(yield)) {
      // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
      return false;
    }
 
    //CRASH points 2

    uint64_t value_v = (uint64_t)FastRand(&seed) & (num_keys_global - 1); /// same value for everyone
  
    for (uint64_t i = 0; i < data_set_size-1; i++) {

      //randomly insert/delete all objects.
      micro_val_t* micro_val = (micro_val_t*) micro_objs[i]->value;
      micro_val->magic[1] = value_v; // new version value for all writes.
    }

    //CRASH points 3

    bool commit_status = dtx->TxCommit(yield); // We also need to emulate crashes within commit. use interrupts.

    //CRASH points 4

    //CRASH points 5

    return commit_status;

}


//Assertion for the litmus 2
bool Assert3(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {

  //assertion function for the mitmus testing
  //trick: either stop the world or lock all read only objects.
  dtx->TxBegin(tx_id);
  bool is_write[data_set_size]; 
  DataItemPtr micro_objs[data_set_size];
 
    for (uint64_t i = 0; i < data_set_size; i++) {
      micro_key_t micro_key;
      micro_key.item_key = (itemkey_t) i+1;
      assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);
      micro_objs[i] = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);
     
      //Perssimistic Reads
      dtx->AddToReadWriteSet(micro_objs[i]);
      is_write[i] = true;

    }
  
    //Lock and Read
    if (!dtx->TxExeLitmus(yield)) {
      // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
         //dtx->AssertAbort();
      return false;
    } 
   
    //Assert versions
    uint64_t value_v=0;
    for (uint64_t i = 0; i < data_set_size-1; i++) {
      micro_val_t* micro_val = (micro_val_t*) micro_objs[i]->value;
      if(i==0)
        value_v= micro_val->magic[1]; // new version value for all writes.
      else
        assert(micro_val->magic[1] == value_v); //local asserts. false assert. basically litmus 1
    }

    micro_val_t* micro_val_final = (micro_val_t*) micro_objs[data_set_size-1]->value;
    assert(value_v !=  micro_val_final->magic[1]);

    //bool commit_status = dtx->TxCommit(yield); // We also need to emulate crashes within commit. use interrupts.
    //Unlock should be there. 

   dtx->AssertAbort(yield);


  return true;

}




//Validation tests


bool IncrementTest(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  
  dtx->TxBegin(tx_id);
  bool is_write;
  DataItemPtr micro_objs;
   micro_key_t micro_key;

//DAM- do the same thing multiple times

  micro_key.item_key = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);

  assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

  micro_objs = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);

  //dtx->AddToReadOnlySet(micro_obj);
  dtx->AddToReadWriteSet(micro_objs);
  //increament 1.

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_objs->value;
  
  if (micro_val->magic[0] != micro_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }

  micro_val->magic[1] = ((signed int)(micro_val->magic[1]) + 1); //increament by 1.

  bool commit_status = dtx->TxCommit(yield);

  return commit_status;
}


//Validation tests
bool DecrementTest(coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  
  dtx->TxBegin(tx_id);
  bool is_write;
  DataItemPtr micro_objs;
  micro_key_t micro_key;
  //DAM- do the same thing multiple times
  
  //micro_key.item_key = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);
  
  assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);
  micro_objs = std::make_shared<DataItem>((table_id_t)MicroTableType::kMicroTable, micro_key.item_key);

  //dtx->AddToReadOnlySet(micro_obj);
  dtx->AddToReadWriteSet(micro_objs);

  //increament 1.

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_objs->value;

  //This is the test.
  assert((signed int)micro_val->magic[1] > 0); // should not be
  
  if (micro_val->magic[0] != micro_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << thread_gid << "-" << dtx->coro_id << "-" << tx_id;
  }

  micro_val->magic[1] = ((signed int)micro_val->magic[1]-1); //increament by 1.


  bool commit_status = dtx->TxCommit(yield);

  return commit_status;
}




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
  DTX* dtx = new DTX(meta_man, qp_man, thread_gid, coro_id, coro_sched, rdma_buffer_allocator,
                     log_offset_allocator, addr_cache);
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
  clock_gettime(CLOCK_REALTIME, &msr_start);

  #ifdef STATS
    RDMA_LOG(INFO) << "Stats Starting" ;
    double tx_usec_start = (msr_start.tv_sec) * 1000000 + (double)(msr_start.tv_nsec) / 1000;
    window_start_time[local_thread_id] = tx_usec_start;  // in miro seconds   
  #endif

  while (true) {
    
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;
    clock_gettime(CLOCK_REALTIME, &tx_start_time);

   

    //litmus 4. 

    switch(litmus){
      case 4:{
    
              tx_committed = IncrementTest(yield, iter, dtx);
          
              //Artificial injected faults. and recovery.
              //This could be tested with threads.
          
              // ebale crash here.
               #ifdef EMULATED_CRASH
                if( ( (stat_attempted_tx_total % 1000) == 0) && (thread_gid==0) ){
                    
                    printf("Crashed-Recovery \n");
                    crash_emu = true;
                    dtx->TxUndoRecovery(yield, addr_caches);
                    dtx->TxLatchRecovery(yield, addr_caches);
                     //usleep(500000);
                    crash_emu = false;
                    asm volatile("mfence" ::: "memory");
          
                }
          
                while(crash_emu){}; // stop all other threads from progressing. 
              #endif
          
              if(tx_committed){
                while(true){
                    ///its not a performance thing
          
                   tx_committed = DecrementTest(yield, iter, dtx);
                   if(tx_committed) break;
          
                }
              }
        break;
      }
      case 1:{

          //update, insert, delete. failures at any point 
          tx_committed = Litmus1(yield, iter, dtx);
          //assert
          Assert1(yield, iter, dtx);
          break;
      }

      case 2:{

          if(coro_id%2 == 0)
            tx_committed = Litmus2_T1(yield, iter, dtx);
          else
             tx_committed = Litmus2_T2(yield, iter, dtx);
            //assert
           //some delay here
           Assert2(yield, iter, dtx);
           break;
      }
      case 3:{
           if(coro_id%2 == 0)
            tx_committed = Litmus3_T1(yield, iter, dtx);
          else
             tx_committed = Litmus3_T2(yield, iter, dtx);
            //assert
           //some delay here
           Assert3(yield, iter, dtx);
           break;
     }
    default:
          break;
    }
    
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
      
        REC atomic_record;
        atomic_record.txs = stat_committed_tx_total;
        atomic_record.usecs = usec;

        record_ptrs[local_thread_id] = &atomic_record; // pointer change. 

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


    // ebale crash here.
     #ifdef CRASH_ENABLE_VALID
      if( (stat_attempted_tx_total == (ATTEMPED_NUM/10)) && (thread_gid==0)){
          
          printf("Crashed-Recovery \n");
          crash_emu = true;

          //send gRPC failed. get the ack back. we sue the same machine for the recovery. we do not run the recovery on FD as its notnecessary.
          //100micro seconds. --> machine id and failed ids. 

          dtx->TxUndoRecovery(yield);
          
          //printf("Wait starts \n");
          //usleep(500000);
          //printf("Wait Ends \n");
          crash_emu = false;
          asm volatile("mfence" ::: "memory");

      }
      while(crash_emu){}; // stop all other threads from progressing. 

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
  
  #ifdef UNDO_RECOVERY
  
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


  #ifdef LATCH_RECOVERY
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

  // Stop running
  stop_run = true;

  #ifdef STATS
    thread_done[local_thread_id] = true;
  #endif

  // RDMA_LOG(DBG) << "Thread: " << thread_gid << ". Loop RDMA alloc times: " << rdma_buffer_allocator->loop_times;
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
