sed -i '96c #define CRASH_INTERVAL 500000' workload/micro/micro_bench.cc



sed -i '100c //#define CRASH_ENABLE' include/common/common.h
sed -i '101c //#define NORESUME' include/common/common.h
sed -i '109c //#define LATCH_STALL' include/common/common.h

bash build.sh

cd build/workload/micro/


parallel-ssh -i -H node-5 'bash setMICRO.sh'
parallel-ssh -i -H node-6 'bash setMICRO.sh'

parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'



sed -i '5c "thread_num_per_machine": 16,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 9,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
#sudo gdb --args ./micro_bench u-100
./micro_bench u-100
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_nocrash_1.txt
cp result_all_threads* ../../../bench_results/eurosys/micro/

cd ../../../





sed -i '100c #define CRASH_ENABLE' include/common/common.h
sed -i '101c //#define NORESUME' include/common/common.h
sed -i '109c //#define LATCH_STALL' include/common/common.h

bash build.sh

cd build/workload/micro/

parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'


sed -i '5c "thread_num_per_machine": 16,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 9,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
#sudo gdb --args ./micro_bench u-100
./micro_bench u-100
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_crash_enable_2.txt
cp result_all_threads* ../../../bench_results/eurosys/micro/

cd ../../../





sed -i '100c #define CRASH_ENABLE' include/common/common.h
sed -i '101c #define NORESUME' include/common/common.h
sed -i '109c //#define LATCH_STALL' include/common/common.h

bash build.sh

cd build/workload/micro/

parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'


sed -i '5c "thread_num_per_machine": 16,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 9,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
#sudo gdb --args ./micro_bench u-100
./micro_bench u-100
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_crash_enable_noresume_3.txt
cp result_all_threads* ../../../bench_results/eurosys/micro/

cd ../../../





sed -i '100c #define CRASH_ENABLE' include/common/common.h
sed -i '101c //#define NORESUME' include/common/common.h
sed -i '109c #define LATCH_STALL' include/common/common.h

bash build.sh

cd build/workload/micro/

parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

sed -i '5c "thread_num_per_machine": 16,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 9,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
#sudo gdb --args ./micro_bench u-100
./micro_bench u-100
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_crash_enable_latchstall_4.txt
cp result_all_threads* ../../../bench_results/eurosys/micro/

cd ../../../



