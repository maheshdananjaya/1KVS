# Author: Ming Zhang
# Copyright (c) 2021

set(WORKLOAD_INCLUDE
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/workload
        ${PROJECT_SOURCE_DIR}/thirdparty
        #${PROJECT_SOURCE_DIR}/thirdparty/eRPC/src
        #${PROJECT_SOURCE_DIR}/thirdparty/eRPC/third_party/asio/include
        )

include_directories(${WORKLOAD_INCLUDE})

set(WORKLOAD_LIB ford)

#set(ERPC_LIB erpc)
#add_subdirectory(test_erpc)
#grpc++

add_subdirectory(stat)

add_subdirectory(tpcc)

add_subdirectory(tatp)

add_subdirectory(smallbank)

add_subdirectory(micro)

add_subdirectory(validations)

if(GRPC_FD) 
	add_subdirectory(test_grpc)
endif()
