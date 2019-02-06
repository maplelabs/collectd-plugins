#!/bin/bash
hadoopClusterStats="getHadoopClusterStats.py"
install_dir=$(pwd)

parentdir="$(dirname "$install_dir")"

export PYTHONPATH=$PYTHONPATH:$parentdir

#echo $PYTHONPATH

if [ $# -eq 0 ]
then
   echo "Usage ./startHadoopClusterStats.sh config-file"
   exit 1
fi

config_file=$1

num_of_processes=`ps -ef | grep $hadoopClusterStats | grep -v grep | awk '{print $2}' | wc -l`
if [ $num_of_processes -ne 0 ]
then
   echo "process hadoopClusterStats is already running"
else
   python "$install_dir/$hadoopClusterStats" --config "$config_file"  > $install_dir/hadoopClusterStats.err 2>&1 &
   if [ $? -eq 0 ]
   then
     echo "hadoopClusterStats started in the background"
   else
     echo "Failed to start hadoopClusterStats in the background"
   fi
fi
