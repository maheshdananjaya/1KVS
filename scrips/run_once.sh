
sed -i '5c "thread_num_per_machine": 8,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 8,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tpcc_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'



parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tpcc_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'



parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tpcc_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'



parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tpcc_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'
