#!/bin/bash
yarnStats="yarn_stats.py"
install_dir=$(pwd)

parentdir="$(dirname "$install_dir")"

export PYTHONPATH=$PYTHONPATH:$parentdir

echo $PYTHONPATH

num_of_processes=`ps -ef | grep $yarnStats | grep -v grep | awk '{print $2}' | wc -l`
if [ $num_of_processes -ne 0 ]
then
   echo "process yarnStats is already running"
else
   python $install_dir/yarn_stats.py > $install_dir/yarnStats.err 2>&1 &
   if [ $? -eq 0 ]
   then
     echo "yarnStats started in the background"
   else
     echo "Failed to start yarnStats in the background"
   fi
fi
