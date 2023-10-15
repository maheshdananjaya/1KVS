sed -i '96c #define CRASH_INTERVAL 400000000' workload/micro/micro_bench.cc
#bash build.sh

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

mv result_all_threads.txt result_all_threads_witheel.txt
cp result_all_threads* ../../../bench_results/eurosys/micro/

cd ../../../


sed -i '96c #define CRASH_INTERVAL 500000000' workload/tpcc/tpcc_bench.cc
#bash build.sh

cd build/workload/tpcc/


parallel-ssh -i -H node-5 'bash setTPCC.sh'
parallel-ssh -i -H node-6 'bash setTPCC.sh'

parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'



sed -i '5c "thread_num_per_machine": 16,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 9,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tpcc_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_tpcc_witheel.txt
cp result_all_threads* ../../../bench_results/eurosys/micro/



