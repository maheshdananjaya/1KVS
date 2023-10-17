# Author: Mahesh Dananjaya

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -g -Wall  -DTHREADED -DHAVE_OPENSSL_H -lzookeeper_mt ")

link_directories(/usr/local/lib/zookeeper)

include_directories(
	/usr/local/include/zookeeper
)


#add_library(zookeeper /usr/local/lib/zookeeper)

set(WROKLOAD_SOURCES zk_cpp.cpp)

add_library(zk_cpp STATIC ${WROKLOAD_SOURCES})

set_target_properties(zk_cpp PROPERTIES LINKER_LANGUAGE CXX)


set(WORKLOAD_RUN zk_cpp_test.cpp)

add_executable(zk_client ${WORKLOAD_RUN})

target_link_libraries(zk_client zk_cpp zookeeper_mt)

#for stat
add_library(zk_cpp_client STATIC ${WORKLOAD_SOURCES} ${WORKLOAD_RUN})
set_target_properties(zk_cpp_client PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(zk_cpp_client zookeeper_mt)
#add_subdirectory(test_zk)
#set ()
