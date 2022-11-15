# Author: MAD
# Copyright (c) 2022


set(WORKLOAD_RUN server.cc)

add_executable(test_erpc_server ${WORKLOAD_RUN})

target_link_libraries(test_erpc_server ${WORKLOAD_LIB} ${ERPC_LIB})



set(WORKLOAD_RUN client.cc)

add_executable(test_erpc_client ${WORKLOAD_RUN})

target_link_libraries(test_erpc_client ${WORKLOAD_LIB} ${ERPC_LIB})