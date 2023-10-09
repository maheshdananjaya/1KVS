// Author: Ming Zhang
// Copyright (c) 2021

#include "stat/result_collect.h"
#include <unistd.h>
#include <assert.h>

#ifdef FD
#include "stat/grpc_client.h"
#endif

std::atomic<uint64_t> tx_id_generator;
std::atomic<uint64_t> connected_t_num;
std::mutex mux;

std::vector<t_id_t> tid_vec;
std::vector<double> attemp_tp_vec;
std::vector<double> tp_vec;
std::vector<double> medianlat_vec;
std::vector<double> taillat_vec;
std::vector<double> lock_durations;


bool crash_emu = false;

//server state. initial =0// Internal state.
int process_state = 0;// state of the coordinatror. 0. init -> 1. failed -> 2. recovered -> 3. reconf. 

//EEL Recovery. This in the order of 128
t_id_t new_base_tid;
uint64_t num_crashes;

//ToDO mutexes for accesses
AddrCache**  addr_caches;

bool * failed_id_list;
uint64_t num_mem_crashes;
bool mem_crash_enable = false;
std::atomic<uint64_t> mem_crash_tnums {0};
//#ifdef UNDO_RECOVERY
//  extern AddrCache* addr_cache;
// extern std::mutex cache_mux; 
//#else

//For partial results.
//uint64_t * tx_attempted;
//uint64_t * tx_commited;
//bool * thread_done;
//double * window_start_time;
//double * window_curr_time;

node_id_t machine_num_;
node_id_t machine_id_;
t_id_t thread_num_per_machine_;

#define STAT_NUM_MAX_THREADS 128

 uint64_t tx_attempted alignas(8)[STAT_NUM_MAX_THREADS];
 uint64_t tx_commited alignas(8) [STAT_NUM_MAX_THREADS];
 bool thread_done [STAT_NUM_MAX_THREADS];
 
 double window_start_time alignas(8) [STAT_NUM_MAX_THREADS];
 double window_curr_time alignas (8) [STAT_NUM_MAX_THREADS];


 REC* record_ptrs [STAT_NUM_MAX_THREADS];

//CounterTimer
 struct timespec timer_start,timer_end;
 struct timespec timer_start_fd,timer_end_fd;
 struct timespec timer_start_fd_zk,timer_end_fd_zk;


void CollectResult(std::string workload_name, std::string system_name) {
  std::ofstream of, of_detail;
  std::string res_file = "../../../bench_results/" + workload_name + "/result.txt";
  std::string detail_res_file = "../../../bench_results/" + workload_name + "/detail_result.txt";

  of.open(res_file.c_str(), std::ios::app);
  of_detail.open(detail_res_file.c_str(), std::ios::app);

  of_detail << system_name << std::endl;
  of_detail << "tid attemp_tp tp 50lat 99lat" << std::endl;

  double total_attemp_tp = 0;
  double total_tp = 0;
  double total_median = 0;
  double total_tail = 0;

  for (int i = 0; i < tid_vec.size(); i++) {
    of_detail << tid_vec[i] << " " << attemp_tp_vec[i] << " " << tp_vec[i] << " " << medianlat_vec[i] << " " << taillat_vec[i] << std::endl;
    total_attemp_tp += attemp_tp_vec[i];
    total_tp += tp_vec[i];
    total_median += medianlat_vec[i];
    total_tail += taillat_vec[i];
  }

  size_t thread_num = tid_vec.size();

  double avg_median = total_median / thread_num;
  double avg_tail = total_tail / thread_num;

  std::sort(medianlat_vec.begin(), medianlat_vec.end());
  std::sort(taillat_vec.begin(), taillat_vec.end());

  of_detail << total_attemp_tp << " " << total_tp << " " << medianlat_vec[0] << " " << medianlat_vec[thread_num - 1]
            << " " << avg_median << " " << taillat_vec[0] << " " << taillat_vec[thread_num - 1] << " " << avg_tail << std::endl;

  of << system_name << " " << total_attemp_tp / 1000 << " " << total_tp / 1000 << " " << avg_median << " " << avg_tail << std::endl;

  of_detail << std::endl;

  of.close();
  of_detail.close();

  // Open it when testing the duration
#if TEST_DURATION
  if (workload_name == "MICRO") {
    // print avg lock duration
    std::string file = "../../../bench_results/" + workload_name + "/avg_lock_duration.txt";
    of.open(file.c_str(), std::ios::app);

    double total_lock_dur = 0;
    for (int i = 0; i < lock_durations.size(); i++) {
      total_lock_dur += lock_durations[i];

    }

    of << system_name << " " << total_lock_dur / lock_durations.size() << std::endl;
    std::cerr << system_name << " avg_lock_dur: " << total_lock_dur / lock_durations.size() << std::endl;
  }
#endif
}
void InitCounters(node_id_t machine_num, node_id_t machine_id, t_id_t thread_num_per_machine){

  machine_num_ = machine_num; // 
  machine_id_ = machine_id; // 
  thread_num_per_machine_ = thread_num_per_machine; //

  //tx_attempted= new uint64_t[thread_num_per_machine]();
  //tx_commited = new uint64_t[thread_num_per_machine]();
  //thread_done =  new bool[thread_num_per_machine]();
  //window_start_time = new double[thread_num_per_machine]();
  //window_curr_time = new double[thread_num_per_machine]();

  //std::fill_n( a, 100, 0 ); 
  addr_caches = new AddrCache* [thread_num_per_machine];
  failed_id_list = new bool[65525]();

  for(int i=0;i<thread_num_per_machine;i++){
    //initial values
    tx_attempted[i] = 0;
    tx_commited[i] = 0;
    thread_done[i] = false;
    window_start_time [i] = 0.0;
    window_curr_time [i] = 0.0;

    record_ptrs[i] = NULL;
    addr_caches[i] = NULL;

  }

  crash_emu = false;
  new_base_tid = 0;
  num_crashes = 0;
  num_mem_crashes = 0;
  mem_crash_enable = false;
  mem_crash_tnums = 0;
  //#ifdef UNDO_RECOVERY
  //  addr_cache = new AddrCache();
  //#endif
  //std::fill_n( a, 100, 0 ); 
  //assert(!thread_done[0]);
}

//background thread taking stats.
void CollectStats(struct thread_params* params){

  //For grpc clinet- FD alsways runs on 10.10.1.1
  #ifdef FD_INSIDE_STAT
    GreeterClient greeter(grpc::CreateChannel("10.10.1.1:50051", grpc::InsecureChannelCredentials()));
    std::string user("node "+ machine_id_);
  #endif
  
  #ifndef STATS   
     usleep(10000);
    std::cout << "Exiting stat thread. Bye!" << std::endl;    
    return; 
  #endif
    //rrecord partial results.
  std::cout << "starting the counters" << std::endl;
  //start
  std::ofstream file_out;
  std::string file_name = "result_all_threads.txt";

  file_out.open(file_name.c_str(), std::ios::app);

  uint64_t last_commited_tx [thread_num_per_machine_]; // per thread last count and time 
  double last_comimted_usec[thread_num_per_machine_];


  uint64_t atomic_last_commited_tx [thread_num_per_machine_]; // per thread last count and time 
  double atomic_last_comimted_usec[thread_num_per_machine_];

  
  uint64_t start_tx_count = 0;
  for(int t = 0; t < thread_num_per_machine_ ; t++){
    start_tx_count += tx_commited[t]; 
    last_commited_tx[t] = tx_commited[t]; 
    last_comimted_usec[t] = window_curr_time[t];

    atomic_last_commited_tx[t] = tx_commited[t]; 
    atomic_last_comimted_usec[t] = window_curr_time[t];
  }

  clock_gettime(CLOCK_REALTIME, &timer_start);
  double start_time = (double) (timer_start.tv_sec *1000000) + (double)(timer_start.tv_nsec)/1000;
  
  uint64_t last_tx_count = start_tx_count;
  double last_usec = start_time; // micro seconds



  while(true){

      //check if any of the transactions are done or have reached the attemp txs.
        usleep(1000);
      

          uint64_t now_tx_count = 0;
          double tx_tput = 0;
          double atomic_tx_tput=0;

          uint64_t tx=0; double usec =0; // per thread;

           uint64_t tx_delta=0; double usec_delta=0;

          clock_gettime(CLOCK_REALTIME, &timer_end);
          double curr_time =  (double) timer_end.tv_sec *1000000 + (double)(timer_end.tv_nsec)/1000;

          bool all_thread_done=true;



            for(int t = 0; t < thread_num_per_machine_ ; t++){
              
              if (!thread_done[t]) {
                  all_thread_done &= false;;
                //file_out.close(); return;
              }else{
                continue;
              }

              now_tx_count += tx_commited[t]; // for all threads.

              //per thread. ERROR this is not an atomic actions. I need either std::atomics or 

              tx = tx_commited[t];
              usec =  window_curr_time[t];

               tx_delta = (tx - last_commited_tx[t]);
               usec_delta = (usec - last_comimted_usec[t]);    

              //tx tput - Mtps
              if(usec_delta != 0) tx_tput += (((double)tx_delta) / usec_delta);

              //assert( tx > last_commited_tx[t]);
              last_commited_tx[t] =  tx;
              last_comimted_usec[t] = usec;

              //CALCULATE TPUT USING 
              REC * new_record = record_ptrs[t]; // atomic action

              if(new_record != NULL) {

                tx = new_record->txs;
                usec =  new_record->usecs;
  
                //Mmemory fance
                  
                tx_delta = (tx - atomic_last_commited_tx[t]);
                usec_delta = (usec - atomic_last_comimted_usec[t]);
  

                //if(usec_delta != 0) atomic_tx_tput += (((double)tx_delta) / usec_delta);
                if(usec_delta > 0 && tx_delta >= 0) atomic_tx_tput += (((double)tx_delta) / usec_delta);
  
                //assert( tx > atomic_last_commited_tx[t]);
                atomic_last_commited_tx[t] =  tx;
                atomic_last_comimted_usec[t] = usec;
              }

              //free(new_record);

            }


         double tput = (double)(now_tx_count-last_tx_count)/(double)(curr_time-last_usec); // window  tp
          //file_out << (curr_time-start_time) << ", " << tput  << ", " << (tx_tput)  << " atomic tput : " << atomic_tx_tput << std::endl;
          if(crash_emu)
            file_out << "COMPUTE CRASHED , " << (curr_time-start_time)/100 << ", " << atomic_tx_tput << std::endl;
	  else if(mem_crash_enable)
            file_out << "MEMORY CRASHED , " << (curr_time-start_time)/100 << ", " << atomic_tx_tput << std::endl;
	  else
            file_out << "UN , " << (curr_time-start_time)/100 << ", " << atomic_tx_tput << std::endl; 
          
          last_tx_count = now_tx_count;
          last_usec = curr_time;


          #ifdef FD_INSIDE_STAT
            //Send messages to the fault detector.
            clock_gettime(CLOCK_REALTIME, &timer_end);
            double grpc_start_time =  (double) timer_end.tv_sec *1000000 + (double)(timer_end.tv_nsec)/1000;
                     
              std::string reply;
              if(crash_emu) reply = greeter.SayHello(machine_id_ + ", FAILED");
              else reply = greeter.SayHello(machine_id_ + ", ACTIVE");

                        
            if(crash_emu) std::cout << reply << std::endl;
  
            clock_gettime(CLOCK_REALTIME, &timer_end);
            double grpc_end_time =  (double) timer_end.tv_sec *1000000 + (double)(timer_end.tv_nsec)/1000;
          #endif 


            //TODO- wait for the detector. 5 ms. and update the. if crash_emu- update failed-id-list.

          //For GRPC round trips
          //std::cout << "Ack received: " << reply  << " Time spent(RTT) " << (grpc_end_time - grpc_start_time) << std::endl;

          if(all_thread_done){
            usleep(1000000);
            file_out.close(); 
            return;
          }
  }

  //at least one thread is done. we stop the stat counter. 
  file_out.close();
  return;
}


//new failure detector iterface
#ifdef REMOTE_FD

int fd_client(int machine_id_){
    std::cout << "task1 says: " << process_id;

    GreeterClient greeter(grpc::CreateChannel("10.10.1.8:50051", grpc::InsecureChannelCredentials()));
    std::string user("node "+ machine_id_);

    int fd_state=0; std::string recovery_coros; int start_coro, end_coro;
    

   while(true){
    //Send messages to the fault detector.
            clock_gettime(CLOCK_REALTIME, &timer_end);
            double grpc_start_time =  (double) timer_end.tv_sec *1000000 + (double)(timer_end.tv_nsec)/1000;

              std::string reply;
              
	      if(fd_state==0 && crash_emu){
	           reply = greeter.SayHello(machine_id_ +",FAILED,0-0");
		    std::vector<std::string> v;
                    split(reply, v, ',');
                        if(v.at(0) ==  "RECOVERY"){
                                //Do recovery.
				//fd_state = 1;
				recovery_coros = v.at(1);
				std::vector<std::string> vrec;
	                        split(recovery_coros, vrec, '-');
				start_coro = vrec.at(0);
				end_coro = vrec.at(1);
				std::cout << "start coro " << start_coro << " end coro " << end_coro<< std::endl;
				fd_state = 1;

                            }

	      }

	      else if(fd_state == 1){
	      	//do nothing; wait for it/
		////use recovery coros.
		//
		 //if (!crash_emu)  fd_state = 2;

	      }

	      else if(fd_state == 2){
			 std::string recovery_response(machine_id_ + "RECOVERED,"+recovery_coros);
                         reply = greeter.SayHello(recovery_response);
                         std::cout << "RECOVERED ACK: " << reply << std::endl;
			 fd_state = 0;
	      }

	      else {
              		reply = greeter.SayHello(machine_id_ + ", ACTIVE");
			std::cout << reply << std::endl;
	      }

            clock_gettime(CLOCK_REALTIME, &timer_end);
            double grpc_end_time =  (double) timer_end.tv_sec *1000000 + (double)(timer_end.tv_nsec)/1000;
            //std::cout << "Ack received: " << reply  << " Time spent(RTT) " << (grpc_end_time - grpc_start_time) << std::endl;
            usleep(1000);
    }

}

#endif


#ifdef FD
//Communicate with the failure detector
void HeartBeats(int machine_id_){
    //live beacoins.
    RDMA_LOG (INFO) << "Trying to connect to grpc FD server";
    GreeterClient greeter(grpc::CreateChannel("10.10.1.8:50051", grpc::InsecureChannelCredentials()));
    std::string user("node "+ machine_id_);
     struct timespec timer_start,timer_end;
     RDMA_LOG (INFO) << "FD server: Connected";
	
 	 #ifdef HEARTBEATS
    while(true){

    clock_gettime(CLOCK_REALTIME, &timer_end);

            usleep(200); //200 microseconds. heartneats?.

            double grpc_start_time =  (double) timer_end.tv_sec *1000000 + (double)(timer_end.tv_nsec)/1000;
                     
              std::string reply;
              if(crash_emu &&  (process_state==0)){ 
                
                reply = greeter.SayHello(machine_id_ +",FAILED,0-0");

                std::string machine_id = (reply).substr(0, reply.find(",")); 
                std::string cmd = (reply).substr(1, reply.find(","));

                if(cmd=="RECOVERY"){
                  std::string failed_coords = (reply).substr(2, reply.find(","));
                  std::string start_coord = (failed_coords).substr(0, failed_coords.find("-")); 
                  std::string end_coord = (failed_coords).substr(1, failed_coords.find("-"));
                  //recovery. call recovery.
                  process_state=1; //failed.
                }
              }

              else if(process_state==1) {
                  //Recovery. 

              }
              else if(process_state==2){

                  reply = greeter.SayHello(machine_id_ +",RECOVERY_DONE");
                  process_state==0; // need reconf here. 

              }

              else{
               reply = greeter.SayHello(machine_id_ +",ACTIVE");
              }

                        
            if(crash_emu) std::cout << reply << std::endl;
  
            clock_gettime(CLOCK_REALTIME, &timer_end);
            double grpc_end_time =  (double) timer_end.tv_sec *1000000 + (double)(timer_end.tv_nsec)/1000;


          //termination condition
          if(all_thread_done){
            usleep(1000000);
            file_out.close(); 
            return;
          }
    }

    #endif //HEARTBEATS

}

#endif




#ifdef FD
//ZOOkeeper distributed mode and failure detector - ZKRFD zookeeper reliable failure detector.
//Communicate with the failure detector
void HeartBeatsZK(struct thread_params* params){
    //live beacoins.
    GreeterClient greeter(grpc::CreateChannel("10.10.1.1:50051", grpc::InsecureChannelCredentials()));
    std::string user("node "+ machine_id_);

	 struct timespec timer_start,timer_end;



    #ifdef ZKRFD
    while(true){

    clock_gettime(CLOCK_REALTIME, &timer_end);

            usleep(200); //200 microseconds. heartneats?.

            double grpc_start_time =  (double) timer_end.tv_sec *1000000 + (double)(timer_end.tv_nsec)/1000;
                     
              std::string reply;
              if(crash_emu &&  (process_state==0)){ 
                
                reply = greeter.SayHello(machine_id_ +",FAILED,0-0");

                std::string machine_id = (reply).substr(0, reply.find(",")); 
                std::string cmd = (reply).substr(1, reply.find(","));

                if(cmd=="RECOVERY"){
                  std::string failed_coords = (reply).substr(2, reply.find(","));
                  std::string start_coord = (failed_coords).substr(0, failed_coords.find("-")); 
                  std::string end_coord = (failed_coords).substr(1, failed_coords.find("-"));
                  //recovery. call recovery.
                  process_state=1; //failed.
                }
              }

              else if(process_state==1) {
                  //Recovery. 

              }
              else if(process_state==2){

                  reply = greeter.SayHello(machine_id_ +",RECOVERY_DONE");
                  process_state==0; // need reconf here. 

              }

              else{
               reply = greeter.SayHello(machine_id_ +",ACTIVE");
              }

                        
            if(crash_emu) std::cout << reply << std::endl;
  
            clock_gettime(CLOCK_REALTIME, &timer_end);
            double grpc_end_time =  (double) timer_end.tv_sec *1000000 + (double)(timer_end.tv_nsec)/1000;


          //termination condition
          if(all_thread_done){
            usleep(1000000);
            file_out.close(); 
            return;
          }
    }

    #endif //HEARTBEATS

}

#endif



