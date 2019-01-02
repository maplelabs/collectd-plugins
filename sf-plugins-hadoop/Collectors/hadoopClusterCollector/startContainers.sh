#!/bin/bash
containers="containers.py"
install_dir=$(pwd)

parentdir="$(dirname "$install_dir")"

export PYTHONPATH=$PYTHONPATH:$parentdir

echo $PYTHONPATH

num_of_processes=`ps -ef | grep $containers | grep -v grep | awk '{print $2}' | wc -l`
if [ $num_of_processes -ne 0 ]
then
   echo "process containers is already running"
else
   python $install_dir/containers.py > $install_dir/containers.err 2>&1 &
   if [ $? -eq 0 ]
   then
     echo "containers started in the background"
   else
     echo "Failed to start containers in the background"
   fi
fi
