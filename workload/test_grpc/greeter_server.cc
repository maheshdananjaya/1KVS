/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "helloworld.grpc.pb.h"
#endif

#include <thread>
#include "test_zk/zk_cpp_client.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using helloworld::Greeter;
using helloworld::HelloReply;
using helloworld::HelloRequest;

#define NUM_MAX_MACHINES 128
double previous_ts[NUM_MAX_MACHINES];

#define NUM_MAX_SERVERS 65525  
//TODO
enum ServerState{
  ACK=0,
  ACTIVE=1,
  FAILED=2,
  RECOVERY=3,
  RECOVERY_DONE=4,
  RECONFIGURE=5,
  RECONF_DONE=6,
  RESUME=7
};


//status vector got all machines or processes. .
int status_vector [NUM_MAX_SERVERS]; //all compute servers // 0 - inactive , 1-acttive, 2- suspicious, 3 -> removed.
double timeouts [NUM_MAX_SERVERS];


//configurations info
std::string failed_process_ids("");

std::string all_config("0-0");

std::string last_config("");

bool reconf_time=false;
int status_refconf [NUM_MAX_SERVERS]; 

// 0- no. 1- sent 2- acked
int tot_nume_config_sent=0;
//we need to drop messages
//
bool  marked_failed=false;

size_t split(const std::string &txt, std::vector<std::string> &strs, char ch)
{
    size_t pos = txt.find( ch );
    size_t initialPos = 0;
    strs.clear();

    // Decompose statement
    while( pos != std::string::npos ) {
        strs.push_back( txt.substr( initialPos, pos - initialPos ) );
        initialPos = pos + 1;

        pos = txt.find( ch, initialPos );
    }

    // Add the last one
    strs.push_back( txt.substr( initialPos, std::min( pos, txt.size() ) - initialPos + 1 ) );

    return strs.size();
}


// Logic and data behind the server's behavior.
class GreeterServiceImpl final : public Greeter::Service {
  Status SayHello(ServerContext* context, const HelloRequest* request,
                  HelloReply* reply) override {
    

    std::string prefix("ACK ");
    std::string req = request->name();

    std::vector<std::string> v;

    //std::string machine_id_str = (req).substr(0, req.find(",")); 
    //std::string status = (req).substr(1, req.find(",")); 
    // int machine_id = std::stoi(machine_id_str); 
    split(req, v, ',');

    int machine_id = std::stoi(v.at(0)); 
    std::string status = v.at(1);   

    //std::cout << "Sender " << machine_id << std::endl;
    //std::cout << "Message " << status << std::endl;

    if(status == "ACTIVE"){
      
      //std::cout << "ACTIVE" << machine_id << std::endl;
      
      if(reconf_time){  

        if(status_refconf[machine_id]==0){      
          status_refconf[machine_id] = 1;
          tot_nume_config_sent++;
          prefix.assign(std::to_string(machine_id)+",RECONFIGURE:"+last_config);
        }
        else
        prefix.assign(std::to_string(machine_id)+",ACK");

      }else{
        prefix.assign(std::to_string(machine_id)+",ACK");
      }

    }
    else if(status == "FAILED") {
      std::cout << "FAILED " << machine_id << std::endl;

      //failed_process_ids.assign ((req).substr(2, req.find(",")));
      failed_process_ids.assign(v.at(2));
	
      //delineated by 
      if(marked_failed){
	std::cout << "Marked Suspected Recovery ";
	prefix.assign(std::to_string(machine_id)+",RECOVERY,"+ failed_process_ids); // start-end
      }
      else{
      	prefix.assign(std::to_string(machine_id)+",WAIT_RECOVERY,"+ failed_process_ids); // start-end
       }
    }

    else if(status == "RECOVERY_DONE") {

      std::cout << "RECOVERY_DONE on " << machine_id << "  from to " <<  failed_process_ids << std::endl;
      //prefix.assign(machine_id+", RECONF [ids]");

      //enable next refoniguartions
      last_config.assign(failed_process_ids); // we need to  get all failed process-ids. start->end
      all_config.assign(all_config + "," + last_config);
      std::cout  << "New COnfiguration " << all_config << std::endl;
      reconf_time = true;

      //RECONFIGURE
      prefix.assign(std::to_string(machine_id)+",RESUME"); //includes reconf. or all reconfig. special message.

    }
    else if(status == "RECONF_DONE"){
        //similar to acks.
        if(status_refconf[machine_id]==1){ 
            status_refconf[machine_id]=0;
            tot_nume_config_sent--;
        }

        if(tot_nume_config_sent==0){ 
            //all set
          reconf_time = false;
          printf("CONFIGUED");

        }

        prefix.assign(std::to_string(machine_id)+",ACK"); // send a normal act
    }else{

      printf("Unknown"); // error.
      prefix.assign("Unknown"); // send a normal act
      //prefix.assign(machine_id+",ACK"); // send a normal act

    }

    //std::cout << "Becon "  << request->name() << std::endl;
    reply->set_message(prefix + ","+ request->name());    

    return Status::OK;
  }
};

//This is DAM Failure Detector
void RunServer() {
  std::string server_address("10.10.1.8:50051");
  GreeterServiceImpl service;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}


void init_zk_watch(std::string msg){
		
	 struct timespec timer_start,timer_end;
   //clock_gettime(CLOCK_REALTIME, &timer_start);
   double zk_start_time, zk_end_time;

        std::string urls = "10.10.1.3:2181,10.10.1.4:2181,10.10.1.7:2181"; //use command for quorums
        utility::zk_cpp zk;
        utility::zoo_rc ret = zk.connect(urls);

        if (ret != utility::z_ok) {
            printf("try connect zk server failed, code[%d][%s]\n",
                ret, utility::zk_cpp::error_string(ret));

        }

	printf("zookeeper connected!");

        std::string path, value;
        int32_t flag;
	 std::string rpath = path;
          //utility::zoo_rc ret = utility::z_ok;

        bool is_fd_alive=false;
        do{

                clock_gettime(CLOCK_REALTIME, &timer_start);
                zk_start_time =  (double) timer_start.tv_sec *1000000 + (double)(timer_start.tv_nsec)/1000;

                path="/fd";
                 utility::zoo_rc ret = zk.exists_node(path.c_str(), nullptr, true);
		   clock_gettime(CLOCK_REALTIME, &timer_end);
                 zk_end_time = (double) timer_end.tv_sec *1000000 + (double)(timer_end.tv_nsec)/1000;

		 std::cout  << "ZK ROun Trip Time " << (zk_end_time - zk_start_time);

                 printf("try_check path[%s] exist[%d], ret[%d][%s]\n",
                         path.c_str(), ret == utility::z_ok, ret, utility::zk_cpp::error_string(ret));

         if(ret == utility::z_ok) break;
	 else{
		 	path="/fd";
                        value= "0"; flag=0;
                        std::vector<utility::zoo_acl_t> acl;
                        acl.push_back(utility::zk_cpp::create_world_acl(utility::zoo_perm_all));
                        ret = zk.create_persistent_node(path.c_str(), value, acl);
                        printf("create path[%s] flag[%d] ret[%d][%s], rpath[%s]\n",
                           path.c_str(), flag, ret, utility::zk_cpp::error_string(ret), rpath.c_str());
	
	 }

        }while(1);

        while(true){
                usleep(2500);
                
                std::vector<std::string> children;

                path="/fd";

                utility::zoo_rc ret = zk.get_children(path.c_str(), children, true);
                //printf("try get path[%s]'s children's, children count[%d], ret[%d][%s]\n",
                   // path.c_str(), (int32_t)children.size(), ret, utility::zk_cpp::error_string(ret));

                std::string list;
                list.append("[");

                for (int32_t i = 0; i < (int32_t)children.size(); ++i) {
                    //printf("%s\n", children[i].c_str());
                    list.append(children[i]).append(", ");
                }

                list.append("]");

                //printf("%s\n", list.c_str());
		//
		if((int32_t)children.size() ==0){
			
			usleep(5000);
			marked_failed = true;
			__asm__ __volatile__("mfence":::"memory"); //NEEDED HERE

		}else{
			 marked_failed = false;
                        __asm__ __volatile__("mfence":::"memory"); //NEEDED HERE

		}	

        }



}

int main(int argc, char** argv) {
 
	//Init zookeeper
	std::thread t(init_zk_watch, "NOW");

	RunServer();

	t.join();

  return 0;
}
