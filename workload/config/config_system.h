// Author: Ming Zhang
// Copyright (c) 2021

#pragma once

#include <unistd.h>
#include <assert.h>     /* assert */
#include <string>

void ModifyComputeNodeConfig(int argc, char* argv[]) {
  std::string config_file = "../../../config/compute_node_config.json";
  if (argc == 3) {
  	assert(argv[2] != NULL && argv[3] != NULL);
  	std::cout << std::string(argv[2]) << "  " << std::string(argv[3])  << std::endl;
    std::string s1 = "sed -i '5c \"thread_num_per_machine\": " + std::string(argv[2]) + ",' " + config_file;
    std::string s2 = "sed -i '6c \"coroutine_num\": " + std::string(argv[3]) + ",' " + config_file;
    system(s1.c_str());
    system(s2.c_str());
  }

  return;
}