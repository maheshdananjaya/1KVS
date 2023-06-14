sed -i '96c #define CRASH_INTERVAL 100000' workload/tatp/tatp_bench.cc
bash build.sh

cd build/workload/tatp/


parallel-ssh -i -H node-5 'bash setTATP.sh'
parallel-ssh -i -H node-6 'bash setTATP.sh'

parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'


sed -i '5c "thread_num_per_machine": 4,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 9,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tatp_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_4x8.txt



sed -i '5c "thread_num_per_machine": 8,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 9,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tatp_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_8x8.txt




sed -i '5c "thread_num_per_machine": 16,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 9,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tatp_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_16x8.txt

#cp result_all_threads* ../../../bench_results/crash_enable/tatp-EEL/

#cd ../../../
sed -i '5c "thread_num_per_machine": 32,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 9,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tatp_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_32x8.txt



sed -i '5c "thread_num_per_machine": 8,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tatp_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_8threads.txt



sed -i '5c "thread_num_per_machine": 16,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tatp_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_16threads.txt



sed -i '5c "thread_num_per_machine": 32,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tatp_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_32threads.txt



sed -i '5c "thread_num_per_machine": 64,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tatp_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_64threads.txt




sed -i '5c "thread_num_per_machine": 128,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tatp_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_128threads.txt






cp result_all_threads* ../../../bench_results/crash_enable/tatp-EEL/I100000

cd ../../../

