#Experiment

cd ../


sed -i '96c #define CRASH_INTERVAL 500000' workload/micro/micro_bench.cc
sed -i '96c #define CRASH_INTERVAL 750000' workload/smallbank/smallbank_bench.cc
sed -i '96c #define CRASH_INTERVAL 750000' workload/tatp/tatp_bench.cc
sed -i '96c #define CRASH_INTERVAL 50000' workload/tpcc/tpcc_bench.cc


sed -i '100c //#define CRASH_ENABLE' include/common/common.h
sed -i '101c //#define NORESUME' include/common/common.h
sed -i '108c //#define LATCH_STALL' include/common/common.h
sed -i '109c //#define LATCH_STALL_RECOVERY' include/common/common.h\
sed -i '103c //#define MEM_FAILURES' include/common/common.h
sed -i '104c //#define MEM_CRASH_ENABLE' include/common/common.h

rm -rf build
bash build.sh
cd edbt_scripts


for INDEX in 1 2 3 4 5;
do
        bash micro.sh
        bash smallbank.sh
        bash tatp.sh
        bash tpcc.sh

        bash run_extract_compute.sh

        mv ../build/workload/micro/result_all_threads.txt ../edbt_results/micro_no_crash_${INDEX}
        mv ../build/workload/smallbank/result_all_threads.txt ../edbt_results/smallbank_no_crash_${INDEX}
        mv ../build/workload/tatp/result_all_threads.txt ../edbt_results/tatp_no_crash_${INDEX}
        mv ../build/workload/tpcc/result_all_threads.txt ../edbt_results/tpcc_no_crash_${INDEX}


done
#Experiment
