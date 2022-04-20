# Author: Ming Zhang
# Copyright (c) 2021

cmake_minimum_required(VERSION 3.3)

project(FORD)

set(CMAKE_CXX_STANDARD 11)

SET(CMAKE_INCLUDE_PATH ${CMAKE_INCLUDE_PATH} "/usr/local/include/boost")
SET(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} "/usr/local/lib")
#-DBoost_NO_BOOST_CMAKE=TRUE

set( boost_INCLUDE_DIR /usr/local/include )
set( boost_LIB_DIR/usr/local/lib )

FIND_PACKAGE(Boost)
IF (Boost_FOUND)
    INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})
    ADD_DEFINITIONS( "-DHAS_BOOST" )
ENDIF()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-result -fPIC")

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -DNDEBUG")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -DDEBUG -g")
endif()


link_directories(usr/local/lib)
include_directories(
        include
        thirdparty
	/usr/local/include
)

add_subdirectory(thirdparty/rlib)

add_subdirectory(src)

add_subdirectory(workload)

add_subdirectory(memory_pool)
