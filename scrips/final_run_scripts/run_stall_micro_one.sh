sed -i '96c #define CRASH_INTERVAL 40000' workload/micro/micro_bench.cc
sed -i '22c #define ATTEMPED_NUM 1000000' workload/micro/micro_bench.cc
#sed -i '22c #define ATTEMPED_NUM 1000000' workload/micro/micro_bench.cc

bash build.sh

cd build/workload/micro/


parallel-ssh -i -H node-5 'bash setMICRO.sh'
parallel-ssh -i -H node-6 'bash setMICRO.sh'

parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'



sed -i '5c "thread_num_per_machine": 64,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
#sudo gdb --args ./micro_bench u-100
./micro_bench u-100
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'
 
#cd ../../../ && cp build/workload/micro/result_all_threads.txt bench_results/stalls/micro-normal-100000/
