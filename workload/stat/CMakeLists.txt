# Author: Mahesh Dananjaya
# Copyright (c) 2021


if(GRPC_FD)
#get_filename_component(hw_proto "../test_grpc/protos/helloworld.proto" ABSOLUTE)
get_filename_component(hw_proto "protos/helloworld.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

# Generated sources
set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/helloworld.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/helloworld.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/helloworld.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/helloworld.grpc.pb.h")
add_custom_command(
      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
      DEPENDS "${hw_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

#include_directories(../test_zk)

add_library(hw_grpc_proto_client_stat
  ${hw_grpc_srcs}
  ${hw_grpc_hdrs}
  ${hw_proto_srcs}
  ${hw_proto_hdrs})

message(STATUS "---------------Using gRPC ${_GRPC_GRPCPP}")

target_link_libraries(hw_grpc_proto_client_stat
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

endif()


if(GRPC_FD)
add_library(client grpc_client.h)
target_link_libraries(client hw_grpc_proto_client_stat ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF} )
else()
	#	nothing
endif()

set(WORKLOAD_SOURCES result_collect.cc)
add_library(stat STATIC ${WORKLOAD_SOURCES})
set_target_properties(stat PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(stat client ${WORKLOAD_LIB}  ${ZK_CPP_CLIENT})

if(GRPC_FD)
	#target_link_libraries(stat ${WORKLOAD_LIB} hw_grpc_proto_client_stat ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF} )
else()
	#target_link_libraries(stat ${WORKLOAD_LIB})
endif()
