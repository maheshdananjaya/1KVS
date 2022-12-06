// Author: Ming Zhang
// Copyright (c) 2021

#pragma once

#include <algorithm>
#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "common/common.h"
#include "dtx/dtx.h"
struct S {
  uint64_t x;
  double y;
};

//std::atomic<S *> globalS;



#define TEST_DURATION 1
void CollectStats(struct thread_params* params);
void InitCounters(node_id_t machine_num, node_id_t machine_id, t_id_t thread_num_per_machine);
void CollectResult(std::string workload_name, std::string system_name);
