
#sed -i 's/"./tpcc_bench 16 8 8"/"program_name"/' run_exp.sh



cd build_with/workload/micro/
bash run_exp.sh
cd ../../../


cd build_without/workload/tpcc/
bash run_exp.sh
cd ../../../

cd build_without/workload/smallbank/
bash run_exp.sh
cd ../../../


cd build_without/workload/tatp/
bash run_exp.sh
cd ../../../


cd build_without/workload/micro/
bash run_exp.sh
cd ../../../


cd build_with/workload/tpcc/
bash run_exp.sh
cd ../../../

cd build_with/workload/smallbank/
bash run_exp.sh
cd ../../../


cd build_with/workload/tatp/
bash run_exp.sh
cd ../../../


cd build_with/workload/micro/
bash run_exp.sh
cd ../../../
