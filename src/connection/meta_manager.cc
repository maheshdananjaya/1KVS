// Author: Ming Zhang
// Copyright (c) 2021

#include "connection/meta_manager.h"

#include "util/json_config.h"

MetaManager::MetaManager() {
  // Read config json file
  std::string config_filepath = "../../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto local_node = json_config.get("local_compute_node");
  local_machine_id = (node_id_t)local_node.get("machine_id").get_int64();
  // txn_system = local_node.get("txn_system").get_int64();
  // enable_commit_together = local_node.get("enable_commit_together").get_int64();
  // enable_read_backup = local_node.get("enable_read_backup").get_int64();
  enable_rdma_flush = local_node.get("enable_rdma_flush").get_int64();
  enable_selective_flush = local_node.get("enable_selective_flush").get_int64();
  // enable_batched_selective_flush = local_node.get("enable_batched_selective_flush").get_int64();

  auto pm_nodes = json_config.get("remote_pm_nodes");
  auto remote_ips = pm_nodes.get("remote_ips");                // Array
  auto remote_ports = pm_nodes.get("remote_ports");            // Array Used for RDMA exchanges
  auto remote_meta_ports = pm_nodes.get("remote_meta_ports");  // Array Used for transferring datastore metas

  // Get remote machine's memory store meta via TCP
  for (size_t index = 0; index < remote_ips.size(); index++) {
    std::string remote_ip = remote_ips.get(index).get_str();
    int remote_meta_port = (int)remote_meta_ports.get(index).get_int64();
    // RDMA_LOG(INFO) << "get hash meta from " << remote_ip;
    node_id_t remote_machine_id = GetMemStoreMeta(remote_ip, remote_meta_port);
    if (remote_machine_id == -1) {
      RDMA_FILE_LOG(FATAL, TID) << "client: GetMemStoreMeta() failed!, remote_machine_id = -1";
    }
    int remote_port = (int)remote_ports.get(index).get_int64();
    remote_nodes.push_back(RemoteNode{.node_id = remote_machine_id, .ip = remote_ip, .port = remote_port});
  }
  // RDMA_LOG(INFO) << "client: All hash meta received!";

  // RDMA setup
  int local_port = (int)local_node.get("local_port").get_int64();
  global_rdma_ctrl = std::make_shared<RdmaCtrl>(local_machine_id, local_port);

  // Using the first RNIC's first port
  RdmaCtrl::DevIdx idx;
  //idx.dev_id = 0; 
  idx.dev_id = 2; //DAM using the 2nd device port1
  idx.port_id = 1;
  RDMA_LOG(INFO) << "Set Device Id to 2"; //DAM
  // Open device
  opened_rnic = global_rdma_ctrl->open_device(idx);

  for (auto& remote_node : remote_nodes) {
    GetMRMeta(remote_node);
  }
  RDMA_LOG(INFO) << "client: All remote mr meta received!"; //DAM
}

node_id_t MetaManager::GetMemStoreMeta(std::string& remote_ip, int remote_port) {
  // Get remote memory store metadata for remote accesses, via TCP
  /* ---------------Initialize socket---------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  if (inet_pton(AF_INET, remote_ip.c_str(), &server_addr.sin_addr) <= 0) {
    RDMA_LOG(ERROR) << "MetaManager inet_pton error: " << strerror(errno);
    return -1;
  }
  server_addr.sin_port = htons(remote_port);
  int client_socket = socket(AF_INET, SOCK_STREAM, 0)
;
  // The port can be used immediately after restart
  int on = 1;
  setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (client_socket < 0) {
    RDMA_LOG(ERROR) << "MetaManager creates socket error: " << strerror(errno);
    close(client_socket);
    return -1;
  }
  if (connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    RDMA_LOG(ERROR) << "MetaManager connect error: " << strerror(errno);
    close(client_socket);
    return -1;
  }

  /* --------------- Receiving hash metadata ----------------- */
  size_t hash_meta_size = (size_t)1024 * 1024 * 1024;
  char* recv_buf = (char*)malloc(hash_meta_size);
  auto retlen = recv(client_socket, recv_buf, hash_meta_size, 0);
  if (retlen < 0) {
    RDMA_LOG(ERROR) << "MetaManager receives hash meta error: " << strerror(errno);
    free(recv_buf);
    close(client_socket);
    return -1;
  }
  char ack[] = "[ACK]hash_meta_received_from_client";
  send(client_socket, ack, strlen(ack) + 1, 0);
  close(client_socket);
  char* snooper = recv_buf;
  // Get number of meta
  size_t primary_meta_num = *((size_t*)snooper);
  snooper += sizeof(primary_meta_num);
  size_t backup_meta_num = *((size_t*)snooper);
  snooper += sizeof(backup_meta_num);
  node_id_t remote_machine_id = *((node_id_t*)snooper);
  if (remote_machine_id >= MAX_REMOTE_NODE_NUM) {
    RDMA_LOG(FATAL) << "remote machine id " << remote_machine_id << " exceeds the max machine number";
  }
  snooper += sizeof(remote_machine_id);
  // Get the `end of file' indicator: finish transmitting
  char* eof = snooper + sizeof(HashMeta) * (primary_meta_num + backup_meta_num);
  if ((*((uint64_t*)eof)) == MEM_STORE_META_END) {
    for (size_t i = 0; i < primary_meta_num; i++) {
      HashMeta meta;
      memcpy(&meta, (HashMeta*)(snooper + i * sizeof(HashMeta)), sizeof(HashMeta));
      primary_hash_metas[meta.table_id] = meta;
      primary_table_nodes[meta.table_id] = remote_machine_id;

      #ifdef MEM_FAILURES
        if (primary_hash_metas_by_node.find(remote_machine_id) == primary_hash_metas_by_node.end()) {
                 primary_hash_metas_by_node[remote_machine_id] = std::vector<HashMeta>();
        }
        primary_hash_metas_by_node[remote_machine_id].push_back(meta); //adding to map as a primary table
      #endif

      //RDMA_LOG(INFO) << "primary_node_ip: " << remote_ip << " table id: " << meta.table_id << " data_ptr: 0x" << std::hex << meta.data_ptr << " base_off: 0x" << meta.base_off << " bucket_num: " << std::dec << meta.bucket_num << " node_size: " << meta.node_size << " B";
    }
    snooper += sizeof(HashMeta) * primary_meta_num;
    for (size_t i = 0; i < backup_meta_num; i++) {
      HashMeta meta;
      memcpy(&meta, (HashMeta*)(snooper + i * sizeof(HashMeta)), sizeof(HashMeta));
      if (backup_hash_metas.find(meta.table_id) == backup_hash_metas.end()) {
        backup_hash_metas[meta.table_id] = std::vector<HashMeta>();
      }
      if (backup_table_nodes.find(meta.table_id) == backup_table_nodes.end()) {
        backup_table_nodes[meta.table_id] = std::vector<node_id_t>();
      }
      backup_hash_metas[meta.table_id].push_back(meta);
      backup_table_nodes[meta.table_id].push_back(remote_machine_id);


      #ifdef MEM_FAILURES
        if (backup_hash_metas_by_node.find(remote_machine_id) == backup_hash_metas_by_node.end()) {
                 backup_hash_metas_by_node[remote_machine_id] = std::vector<HashMeta>();
        }
        backup_hash_metas_by_node[remote_machine_id].push_back(meta); //adding to map as a primary table
      #endif

      // RDMA_LOG(INFO) << "backup_node_ip: " << remote_ip << " table id: " << meta.table_id << " data_ptr: " << std::hex << meta.data_ptr << " base_off: " << meta.base_off << " bucket_num: " << std::dec << meta.bucket_num << " node_size: " << meta.node_size;
    }
  } else {
    free(recv_buf);
    return -1;
  }
  free(recv_buf);
  return remote_machine_id;
}

void MetaManager::GetMRMeta(const RemoteNode& node) {
  // Get remote node's memory region information via TCP
  MemoryAttr remote_hash_mr{}, remote_log_mr{};

  while (QP::get_remote_mr(node.ip, node.port, SERVER_HASH_BUFF_ID, &remote_hash_mr) != SUCC) {
    usleep(2000);
  }
  while (QP::get_remote_mr(node.ip, node.port, SERVER_LOG_BUFF_ID, &remote_log_mr) != SUCC) {
    usleep(2000);
  }
  remote_log_mrs[node.node_id] = remote_log_mr;
  remote_hash_mrs[node.node_id] = remote_hash_mr;
}


#ifdef MEM_FAILURES
int MetaManager::removeMemServer(node_id_t failed_memnode_id){
  //remove the failed node from the metadata and promote the first backup as the primary for the table. 
  // All servers must agree on the machine
  //For more than 1 backup server we
   
   //also change global_meta_man->GetBackupNodeID, global_meta_man->remote_nodes.size(), hash and tables.
   //
  node_id_t failed_mem_node;
  //std::string node_ip ();
  //Find all failed machine ids and the primary of all tables of that machine id.
  auto primary_metas = primary_hash_metas_by_node[failed_memnode_id];


  for (auto& meta : primary_metas){
    table_id_t table_id = meta.table_id;

    //find the backup and promote to the primary.
    auto backup_metas = backup_hash_metas[table_id];

    node_id_t lowest_machine_id = 65525; //No backup remaining
    node_id_t lowest_machine_id_index = -1; //No backup remaining
    int index = 0;

    for(auto machine_id : backup_table_nodes[table_id]){
         
        if(machine_id < lowest_machine_id){
          lowest_machine_id = machine_id;
          lowest_machine_id_index = index;
        }

        index++;
    }

    //remove from primary
    if(lowest_machine_id != 65525){
       
      //RDMA_LOG(INFO) << "Promote Node " << lowest_machine_id << " for Table " << table_id <<  " which was primary of " << failed_memnode_id;  

      auto promote_meta =  backup_hash_metas[table_id].at(lowest_machine_id_index);
      auto promote_node =  backup_table_nodes[table_id].at(lowest_machine_id_index);

      backup_table_nodes[table_id].erase(backup_table_nodes[table_id].begin() + lowest_machine_id_index);
      backup_hash_metas[table_id].erase(backup_hash_metas[table_id].begin() + lowest_machine_id_index);


      auto downgrade_meta = primary_hash_metas[table_id];
      auto downgrade_node = primary_table_nodes[table_id];

      primary_hash_metas[table_id] = promote_meta;
      primary_table_nodes[table_id] = promote_node;
      
      //RDMA_LOG(INFO) << "Promoting backup_node_ip: " << promote_node << " table id: " << promote_meta.table_id << " data_ptr: " << std::hex << promote_meta.data_ptr << " base_off: " << promote_meta.base_off << " bucket_num: " << std::dec << promote_meta.bucket_num << " node_size: " << promote_meta.node_size;
     // RDMA_LOG(INFO) << "Downgrade backup_node_ip: " << downgrade_node << " table id: " << downgrade_meta.table_id << " data_ptr: " << std::hex << downgrade_meta.data_ptr << " base_off: " << downgrade_meta.base_off << " bucket_num: " << std::dec << downgrade_meta.bucket_num << " node_size: " << downgrade_meta.node_size;
    
    }else{

      RDMA_LOG(FATAL) << "cannot find a backup to upgrade";
    }

     //find the lowest machine id for the backups and protomote.

  }
  //remove from backup and add to priamry table. 
} 

#endif
