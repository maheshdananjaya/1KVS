# Author: MAD
# Copyright (c) 2022


set(WORKLOAD_RUN server.cc client.cc)

add_executable(test_erpc ${WORKLOAD_RUN})

target_link_libraries(test_erpc ${WORKLOAD_LIB})