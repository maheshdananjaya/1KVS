sed -i '96c #define CRASH_INTERVAL 40000' workload/validations/micro_bench.cc
bash build.sh -d

cd build/workload/validations/


parallel-ssh -i -H node-5 'bash setMICRO.sh'
parallel-ssh -i -H node-6 'bash setMICRO.sh'

parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'



sed -i '5c "thread_num_per_machine": 8,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./validations u-100
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'
