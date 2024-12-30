
#cd ../
#rm -rf build
#bash build.sh

cd ../build/workload/tatp

parallel-ssh -i -H node-5 'bash setTATP.sh'
parallel-ssh -i -H node-6 'bash setTATP.sh'
parallel-ssh -i -H node-6 'bash setMemId.sh'

parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'

sed -i '5c "thread_num_per_machine": 16,' ../../../config/compute_node_config.json
sed -i '6c "coroutine_num": 9,' ../../../config/compute_node_config.json

parallel-ssh -i -H node-8 'bash startfd.sh'
parallel-ssh -i -H node-5 'bash startexp.sh'
parallel-ssh -i -H node-6 'bash startexp.sh'
sleep 10
./tatp_bench u-100
parallel-ssh -i -H node-5 'bash termexp.sh'
parallel-ssh -i -H node-6 'bash termexp.sh'
parallel-ssh -i -H node-8 'bash termfd.sh'
