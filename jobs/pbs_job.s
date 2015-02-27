#!/bin/bash
#PBS -l nodes=2:ppn=1
#PBS -l walltime=00:30:00
#PBS -N flow_job
#PBS -q debug
#PBS -V

if [ -z $FLOW_HOME ] ; then 
	echo "[ERROR: FLOW_HOME Env variable is not found..]"
	exit
fi

if [ -z $PBS_NODEFILE ] ; then 
	echo "[ERROR: PBS_NODEFILE Env variable is not found!]"
	exit
fi

LOG_FILE=flow.log
echo "[starting flow job...]"
echo "[FLOW HOME directory is : $FLOW_HOME]" > $LOG_FILE

fanout=2
depth=1
hostfile=$FLOW_HOME/jobs/hosts.txt
topofile=$FLOW_HOME/top_file
binprog=$FLOW_HOME/front

cat $PBS_NODEFILE > $hostfile
echo "[host file written to $FLOW_HOME/jobs/hosts.txt ]" >> $LOG_FILE

#MRNet topgen has bugs when using different configurations
#mrnet_topgen -t b:${fanout}^${depth} \
# -h $hostfile -o $topofile > topgen.log  2>&1

num_hosts=`expr $fanout + 1`
simple_topgen $hostfile $num_hosts $topofile  

#execute flow
cd $FLOW_HOME;$binprog

echo "[topology infromation file generated]" >> $LOG_FILE
echo "[topology infromation file copied to  $FLOW_HOME/jobs/topgen.log ]" >> $LOG_FILE

echo "[ending flow job...]"
