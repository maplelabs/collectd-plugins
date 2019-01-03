#!/bin/bash
sparkJobsCollector="processSparkApps.py"
install_dir=$(pwd)

parentdir="$(dirname "$install_dir")"

export PYTHONPATH=$PYTHONPATH:$parentdir

echo $PYTHONPATH

num_of_processes=`ps -ef | grep $sparkJobsCollector | grep -v grep | awk '{print $2}' | wc -l`
if [ $num_of_processes -ne 0 ]
then
   echo "process prossSparkApps is already running"
else
   python $install_dir/$sparkJobsCollector > $install_dir/processSparkApps.err 2>&1 &
   if [ $? -eq 0 ]
   then
     echo "processSparkApps started in the background"
   else
     echo "Failed to start processSparkApps in the background"
   fi
fi

