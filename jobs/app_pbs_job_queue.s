#!/bin/bash
#PBS -l nodes=4:ppn=1
#PBS -l walltime=00:30:00
#PBS -N flow_job
#PBS -q debug
#PBS -V

fanout=22

#You should define FLOW_HOME enviornement variable prior to running this script
#FLOW_HOME should point to directory where FLOW engine is installed.
#FLOW_HOME can be inserted to either your local bash profile ~/.bashrc or global
#profile or in shell itself prior to running qsub/script.
#
#if you are using anthing other than MOAB/PBS schduler use respective hostlist 
#file environement variable (ie:- replace var PBS_NODEFILE) instead
#
#make sure to modify 'fanout' variable which gives number of leaf/be nodes on 
#MRNet. If u assigned 'n' nodes for this job usually fanout => n - 1
#

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

depth=1
hostfile=$FLOW_HOME/jobs/hosts.txt
topofile=$FLOW_HOME/apps/histogram/top_file
binprog=$FLOW_HOME/apps/histogram/front

cat $PBS_NODEFILE > $hostfile
echo "[host file written to $FLOW_HOME/jobs/hosts.txt ]" >> $LOG_FILE

#MRNet topgen has bugs when using different configurations
#mrnet_topgen -t b:${fanout}^${depth} \
# -h $hostfile -o $topofile > topgen.log  2>&1

files=($FLOW_HOME/jobs/props/*.properties.*)
 
# find total number of files in an array
echo "Total jobs scheduled : ${#files[*]}"
total=${#files[*]}
 
if [ $total -eq 1 ] ; then 
	echo "[no app config files for scheduled jobs were found in $FLOW_HOME/jobs/props folder!]"
	echo "[Note app config files in $FLOW_HOME/jobs/props folder must be in the format --> app.properties.[num_nodes] ]"
	echo ""
	echo "[ending flow job...]"
	exit
fi


echo
echo "***************************************************************************************"
echo " Starting FLOW job queue. [properties] : $FLOW_HOME/jobs/props/*.properties.num files "
echo "***************************************************************************************"
# Use for loop iterate through an array
# $f stores current value 
for f in "${files[@]}"
do
	fanout="${f#*.properties.}"
	cp $f $FLOW_HOME/app.properties
	echo -n "executing FLOW with properties file -> $f  num backend nodes -> $fanout"
	num_hosts=`expr $fanout + 1`
	simple_topgen $hostfile $num_hosts $topofile  
	cp $topofile $FLOW_HOME/top_file 
	cd $FLOW_HOME/apps/histogram;$binprog
	echo ""
	echo "===============================================================================================" 
	echo ""
	echo ""
	sleep 2
done


echo "[topology infromation file generated]" >> $LOG_FILE
echo "[topology infromation file copied to  $FLOW_HOME/jobs/topgen.log ]" >> $LOG_FILE

echo "[ending flow job...]"
