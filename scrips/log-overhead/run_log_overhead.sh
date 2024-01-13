sed -i '91c #define WITH_LATCH_LOGGING' include/common/common.h
bash build.sh


bash log_overhead_micro.sh
bash log_overhead_smallbank.sh
bash log_overhead_tatp.sh
bash log_overhead_tpcc.sh

sed -i '91c //#define WITH_LATCH_LOGGING' include/common/common.h
bash build.sh

bash log_overhead_micro.sh
bash log_overhead_smallbank.sh
bash log_overhead_tatp.sh
bash log_overhead_tpcc.sh

