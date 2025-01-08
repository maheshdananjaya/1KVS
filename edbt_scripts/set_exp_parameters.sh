#Initial Parameters before experiments
#We need to set this if required.

sed -i '96c #define CRASH_INTERVAL 500000' workload/micro/micro_bench.cc
sed -i '96c #define CRASH_INTERVAL 750000' workload/smallbank/smallbank_bench.cc
sed -i '96c #define CRASH_INTERVAL 750000' workload/tatp/tatp_bench.cc
sed -i '96c #define CRASH_INTERVAL 50000' workload/tpcc/tpcc_bench.cc

sed -i "82c #define EEL" include/common/common.h
sed -i "82c #define ELOG " include/common/common.h

sed -i '100c //#define CRASH_ENABLE' include/common/common.h
sed -i '101c //#define NORESUME' include/common/common.h
sed -i '108c //#define LATCH_STALL' include/common/common.h
sed -i '109c #define LATCH_STALL_RECOVERY' include/common/common.h
sed -i '103c //#define MEM_FAILURES' include/common/common.h
sed -i '104c //#define MEM_CRASH_ENABLE' include/common/common.h

sed -i "70c #define LATCH_TO_LOG_ORDER" include/common/common.h
sed -i "71c #define FIX_COVERT_LOCKS" include/common/common.h

sed -i "73c #define FIX_VALIDATE_ERROR" include/common/common.h
sed -i "74c #define FIX_ABORT_ISSUE" include/common/common.h
sed -i "75c //#define FIX_RO_READ" include/common/common.h
sed -i "76c #define FIX_INSERT_BUG" include/common/common.h
sed -i "77c //#define BLOCKING_RECOVERY" include/common/common.h

sed -i "79c #define FD" include/common/common.h
sed -i "78c #define HEARTBEATS" include/common/common.h
sed -i "81c #define ZK_HEARTBEATS" include/common/common.h

#only for naive logging scheme.
sed -i "91c //#define WITH_LATCH_LOGGING" include/common/common.h

#Sample Sizez. default 1ms
sed -i "229c usleep(1000);" workload/stat/result_collect.cc
        
