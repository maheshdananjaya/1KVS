#compile flags
#use this :https://www.mail-archive.com/user@zookeeper.apache.org/msg09676.html
cd src/

LDFLAGS=''

g++ -g -Wall  -DTHREADED -DHAVE_OPENSSL_H  -I /usr/local/include/zookeeper -I /users/maheshd/DAM-RFD/src -L /usr/local/lib/zookeeper -o zk-client  zk_cpp_test.cpp zk_cpp.cpp -lzookeeper_mt
g++ -o test_uset  test_uset.cpp

#LDFLAGS=''
#g++ -g -Wall  -DTHREADED -DHAVE_OPENSSL_H  -I /usr/blocal/include/zookeeper -I /users/maheshd/DAM-RFD/src -L /usr/blocal/lib/zookeeper -o zk-client  zk_cpp_test.cpp zk_cpp.cpp -lzookeeper_mt


#we need to set an enviroment variable after this point. 

#LD_LIBRARY_PATH="/usr/blocal/lib:$LD_LIBRARY_PATH"
#export LD_LIBRARY_PATH


#gcc -g -Wall -Wl,-rpath -Wl,LIBDIR -LLIBDIR  -DTHREADED -DHAVE_OPENSSL_H  -I ~/zookeeper/zookeeper-client/zookeeper-client-c/include/ -I ~/zookeeper/zookeeper-client/zookeeper-client-c/generated/ -I zk_cpp.h -o -L /usr/local/lib -lzookeeper_mt -o zk-client zk_cpp_test.cpp

#/zookeeper/zookeeper-client/zookeeper-client-c/libzookeeper_mt.la
