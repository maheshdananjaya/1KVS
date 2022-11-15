#!/bin/bash

# Author: Ming Zhang
# Copyright (c) 2021

if [[ -d build ]]; then
  echo "Remove existing build directory";
  rm -rf build
fi

BUILD_TARGET=client
BUILD_TYPE=Release

while getopts "sd" arg
do
  case $arg in
    s)
      echo "building server";
      BUILD_TARGET="server";
      ;;
    d)
      BUILD_TYPE=Debug;
      ;;
    ?)
      echo "unkonw argument"
  exit 1
  ;;
  esac
done

echo "Create build directory";
mkdir build
#-DBOOST_ROOT=/usr/local/include \
CMAKE_CMD="cmake \
      -DBoost_NO_SYSTEM_PATHS=TRUE \
      -DBoost_NO_BOOST_CMAKE=TRUE \
      -DBOOST_INCLUDEDIR=/usr/local/include \
      -DBOOST_LIBRARYDIR=/usr/local/lib \
      -DCMAKE_BUILD_TYPE=${BUILD_TYPE} ../ -DPERF=OFF -DTRANSPORT=infiniband -DROCE=on -DERPC_INFINIBAND=true"
echo ${CMAKE_CMD}
cd ./build
${CMAKE_CMD}

if [ "${BUILD_TARGET}" == "server" ];then
  echo "------------------- building server ------------------"
  make server -j32
else
  echo "------------------- building client + server ------------------"
  make -j32
fi
echo "-------------------- build finish ----------------------"
