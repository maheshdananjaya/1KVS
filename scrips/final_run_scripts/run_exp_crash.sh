#!/bin/bash          
parallel-ssh -i -H node-5 'bash setSmallBank.sh'
parallel-ssh -i -H node-6 'bash setSmallBank.sh'

parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'


sed -i '5c "thread_num_per_machine": 1,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./smallbank_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_1.txt

#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'



#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'



#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'



sed -i '5c "thread_num_per_machine": 8,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./smallbank_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'


mv result_all_threads.txt result_all_threads_8.txt

#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'



#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'



#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'




sed -i '5c "thread_num_per_machine": 16,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./smallbank_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'


mv result_all_threads.txt result_all_threads_16.txt

#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'



#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'



#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'

sed -i '5c "thread_num_per_machine": 32,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./smallbank_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_32.txt

#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'



#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'



#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'


sed -i '5c "thread_num_per_machine": 64,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./smallbank_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_64.txt

##
#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'



#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'



#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'
##


sed -i '5c "thread_num_per_machine": 128,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 2,' ../../../config/compute_node_config.json
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./smallbank_bench 16 8 8
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

mv result_all_threads.txt result_all_threads_128.txt

#parallel-ssh -i -H node-5 'bash startexp.sh'
#5parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'



#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'



#parallel-ssh -i -H node-5 'bash startexp.sh'
#parallel-ssh -i -H node-6 'bash startexp.sh'
#sleep 10
#./smallbank_bench 16 8 8
#parallel-ssh -i -H node-5 'bash termexp.sh'
#parallel-ssh -i -H node-6 'bash termexp.sh'
