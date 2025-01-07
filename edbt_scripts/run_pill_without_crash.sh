cd ../

#run all ILL with innifity, 500000(10s), 100000(2s), 50000(1s), 25000, 12000 (250ms)
#run also without EEL in common.h

for PILL in NO_EEL EEL;
do

sed -i "96c #define CRASH_INTERVAL ${MTTS}" workload/micro/micro_bench.cc
sed -i "95c #define WARMUP_COUNT 0" workload/micro/micro_bench.cc
sed -i "22c #define ATTEMPED_NUM 2000000" workload/micro/micro_bench.cc

sed -i "82c #define ${PILL}" include/common/common.h

sed -i '100c //#define CRASH_ENABLE' include/common/common.h
sed -i '101c //#define NORESUME' include/common/common.h
sed -i '108c //#define LATCH_STALL' include/common/common.h
sed -i '109c #define LATCH_STALL_RECOVERY' include/common/common.h
sed -i '103c //#define MEM_FAILURES' include/common/common.h
sed -i '104c //#define MEM_CRASH_ENABLE' include/common/common.h

rm -rf build
bash build.sh
cd edbt_scripts


for INDEX in 1 2 3 4 5;
do
        bash micro.sh
        #bash run_extract_compute.sh

        mv ../build/workload/micro/result_all_threads.txt ../edbt_results/micro_with_crash_${PILL}_${INDEX}

done

bash average.sh ../edbt_results/micro_with_crash_${PILL} 5
cd ..

done

sed -i "95c #define WARMUP_COUNT 0" workload/micro/micro_bench.cc
sed -i "22c #define ATTEMPED_NUM 1000000" workload/micro/micro_bench.cc

#Experiment