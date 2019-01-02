#!/bin/bash
nameNode="name_node.py"
install_dir=$(pwd)

parentdir="$(dirname "$install_dir")"

export PYTHONPATH=$PYTHONPATH:$parentdir

echo $PYTHONPATH

num_of_processes=`ps -ef | grep $nameNode | grep -v grep | awk '{print $2}' | wc -l`
if [ $num_of_processes -ne 0 ]
then
   echo "process nameNode is already running"
else
   python $install_dir/name_node.py > $install_dir/nameNode.err 2>&1 &
   if [ $? -eq 0 ]
   then
     echo "nameNode started in the background"
   else
     echo "Failed to start nameNode in the background"
   fi
fi
