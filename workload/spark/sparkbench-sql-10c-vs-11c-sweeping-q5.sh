#!/bin/bash
# This script demonstrates how to sweep a parameter
#
# Export these variables to execute a sweep
# - RUNDIR        to collect all files in 1 directory (see below)
# - RUN_ID        needs to be unique for each measurement
# - WORKLOAD_CMD  actual workload to call with the parameter
#

export WORKLOAD_NAME=sparkbench_sql_11c_q5_sweeping
export DESCRIPTION="sparkbench sql 11c q5 sweeping"
export WORKLOAD_DIR="."      # The workload working directory
export MEAS_DELAY_SEC=1      # Delay between each measurement
export RUNDIR=$(./setup-run.sh $WORKLOAD_NAME)

export PMH=/root/Github/pid_monitor
unset SLAVES
SLAVES="opbriggs1.aus.stglabs.ibm.com"
export SLAVES

declare -a TESTS=(
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q4_exec2_vcore22 q4_exec2_vcore22_it0 RD UC"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q4_exec2_vcore22 q4_exec2_vcore22_it1 NA NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q4_exec2_vcore33 q4_exec2_vcore33_it0 RD NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q4_exec2_vcore33 q4_exec2_vcore33_it1 NA NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q4_exec2_vcore40 q4_exec2_vcore40_it0 RD NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q4_exec2_vcore40 q4_exec2_vcore40_it1 NA NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q4_exec2_vcore44 q4_exec2_vcore44_it0 RD NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q4_exec2_vcore44 q4_exec2_vcore44_it1 NA NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q3_exec2_vcore22 q3_exec2_vcore22_it0 RD NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q3_exec2_vcore22 q3_exec2_vcore22_it1 NA NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q3_exec2_vcore33 q3_exec2_vcore33_it0 RD NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q3_exec2_vcore33 q3_exec2_vcore33_it1 NA NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q3_exec2_vcore40 q3_exec2_vcore40_it0 RD NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q3_exec2_vcore40 q3_exec2_vcore40_it1 NA NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q3_exec2_vcore44 q3_exec2_vcore44_it0 RD NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q3_exec2_vcore44 q3_exec2_vcore44_it1 NA NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q1_exec2_vcore22 q1_exec2_vcore22_it0 RD NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q1_exec2_vcore22 q1_exec2_vcore22_it1 NA NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q1_exec2_vcore33 q1_exec2_vcore33_it0 RD NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q1_exec2_vcore33 q1_exec2_vcore33_it1 NA NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q1_exec2_vcore40 q1_exec2_vcore40_it0 RD NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q1_exec2_vcore40 q1_exec2_vcore40_it1 NA NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q1_exec2_vcore44 q1_exec2_vcore44_it0 RD NA"
"${PMH}/workload/spark/test_case/run_sweeping.sh opbriggs3.aus.stglabs.ibm.com /home/felix/spark-1.6.1 q1_exec2_vcore44 q1_exec2_vcore44_it1 NA NA"

)

for cmd in "${TESTS[@]}"
do
    echo ${cmd}
    export RUN_ID=`echo ${cmd} | awk '{print $5}'`
    export WORKLOAD_CMD=${cmd}
    ./run-workload.sh
done

exit


