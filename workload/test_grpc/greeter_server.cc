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

// Logic and data behind the server's behavior.
class GreeterServiceImpl final : public Greeter::Service {
  Status SayHello(ServerContext* context, const HelloRequest* request,
                  HelloReply* reply) override {
    
    
    std::string prefix("ACK ");
    std::string req = request->name();

    std::string machine_id_str = (req).substr(0, req.find(",")); 
    
    int machine_id = std::stoi(machine_id_str);

    std::string status = (req).substr(1, req.find(",")); 

    if(status == "ACTIVE"){
      
      std::cout << "ACTIVE" << machine_id << std::endl;
      
      if(reconf_time){  

        if(status_refconf[machine_id]==0){      
          status_refconf[machine_id] = 1;
          tot_nume_config_sent++;
          prefix.assign(machine_id+",RECONFIGURE:"+last_config);
        }
        else
        prefix.assign(machine_id+",ACK");

      }else{
        prefix.assign(machine_id+",ACK");
      }

    }
    else if(status == "FAILED") {
      std::cout << "FAILED " << machine_id << std::endl;
      failed_process_ids.assign ((req).substr(2, req.find(",")));
      //delineated by 
      prefix.assign(machine_id+",RECOVERY,"+ failed_process_ids); // start-end

    }

    else if(status == "RECOVERY_DONE") {

      std::cout << "RECOVERY_DONE on " << machine_id << "  from to " <<  failed_process_ids << std::endl;
      //prefix.assign(machine_id+", RECONF [ids]");

      //enable next refoniguartions
      last_config.assign(failed_process_ids); // we need to  get all failed process-ids. start->end
      all_config.assign(all_config + "," + last_config);
      reconf_time = true;

      //RECONFIGURE
      prefix.assign(machine_id+",RESUME"); //includes reconf. or all reconfig. special message.

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

        prefix.assign(machine_id+",ACK"); // send a normal act
    }else{

      printf("Unknown"); // error.
    }

    //std::cout << "Becon "  << request->name() << std::endl;
    reply->set_message(prefix + request->name());    

    return Status::OK;
  }
};

//This is DAM Failure Detector
void RunServer() {
  std::string server_address("0.0.0.0:50051");
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

int main(int argc, char** argv) {
  RunServer();

  return 0;
}
