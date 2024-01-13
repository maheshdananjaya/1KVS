sed -i '96c #define CRASH_INTERVAL 50000000' workload/tpcc/tpcc_bench.cc
#bash build.sh -d

cd build/workload/tpcc/


parallel-ssh -i -H node-5 'bash setTPCC.sh'
parallel-ssh -i -H node-6 'bash setTPCC.sh'

parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'



sed -i '5c "thread_num_per_machine": 1,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tpcc_bench 16 8 8 
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_4x8.txt




sed -i '5c "thread_num_per_machine": 1,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 9,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tpcc_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_8x8.txt




sed -i '5c "thread_num_per_machine": 2,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 9,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tpcc_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_16x8.txt

#cp result_all_threads* ../../../bench_results/crash_enable/tpcc-EEL/

#cd ../../../


sed -i '5c "thread_num_per_machine": 8,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 9,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tpcc_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_32x8.txt


sed -i '5c "thread_num_per_machine": 16,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 9,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tpcc_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_8threads.txt


