#Experiment: PILL Steady-State Overhead
bash run_pill_without_crash.sh
bash run_pill_with_crash.sh

#Experiment: Fail-Over Throughout
bash run_failover_tput.sh

#Experiment: Naive Logging
bash run_naive_logging.sh

#Experiment: Stall
#todo: move scripts from old scrips