cp 1KVS/scrips/set* .
cp 1KVS/scrips/startexp.sh .
cp 1KVS/scrips/termexp.sh .
scp set* node-5:~/
scp startexp.sh  node-5:~/
scp termexp.sh  node-5:~/
scp set* node-6:~/
scp startexp.sh  node-6:~/
scp termexp.sh  node-6:~/
cd 1KVS
bash build.sh -s
cd ../
sed -i '4c "machine_id": 1,' ~/1KVS/config/memory_node_config.json
scp -r 1KVS/ node-5:~/
sed -i '4c "machine_id": 0,' ~/1KVS/config/memory_node_config.json
scp -r 1KVS/ node-6:~/
cd 1KVS
bash build.sh

