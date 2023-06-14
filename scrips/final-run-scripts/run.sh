sed -i '96c #define CRASH_INTERVAL 1000' workload/micro/micro_bench.cc
bash build.sh

cd build/workload/micro/


prallel-ssh -i -H node-5 'bash setMICRO.sh'
parallel-ssh -i -H node-6 'bash setMICRO.sh'

parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'


sed -i '5c "thread_num_per_machine": 32,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./micro_bench u-100
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_32threads_1000interval.txt

sed -i '5c "thread_num_per_machine": 16,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./micro_bench u-100
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_16threads_1000interval.txt

cp result_all_threads* ../../../bench_results/crash_enable/micro-EEL/

cd ../../../





sed -i '96c #define CRASH_INTERVAL 10000' workload/micro/micro_bench.cc
bash build.sh

cd build/workload/micro/


prallel-ssh -i -H node-5 'bash setMICRO.sh'
parallel-ssh -i -H node-6 'bash setMICRO.sh'

parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'


sed -i '5c "thread_num_per_machine": 32,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./micro_bench u-100
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_32threads_10000interval.txt

sed -i '5c "thread_num_per_machine": 16,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./micro_bench u-100
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_16threads_10000interval.txt

cp result_all_threads* ../../../bench_results/crash_enable/micro-EEL/

cd ../../../






sed -i '96c #define CRASH_INTERVAL 100000' workload/micro/micro_bench.cc
bash build.sh

cd build/workload/micro/


prallel-ssh -i -H node-5 'bash setMICRO.sh'
parallel-ssh -i -H node-6 'bash setMICRO.sh'

parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'


sed -i '5c "thread_num_per_machine": 32,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./micro_bench u-100
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_32threads_100000interval.txt

sed -i '5c "thread_num_per_machine": 16,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./micro_bench u-100
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_16threads_100000interval.txt

cp result_all_threads* ../../../bench_results/crash_enable/micro-EEL/

cd ../../../





sed -i '96c #define CRASH_INTERVAL 500000' workload/micro/micro_bench.cc
bash build.sh

cd build/workload/micro/


prallel-ssh -i -H node-5 'bash setMICRO.sh'
parallel-ssh -i -H node-6 'bash setMICRO.sh'

parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'


sed -i '5c "thread_num_per_machine": 32,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./micro_bench u-100
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_32threads_500000interval.txt

sed -i '5c "thread_num_per_machine": 16,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./micro_bench u-100
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_16threads_500000interval.txt

cp result_all_threads* ../../../bench_results/crash_enable/micro-EEL/

cd ../../../






