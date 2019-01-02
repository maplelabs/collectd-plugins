#!/bin/bash
oozieWorkflow="processOzzieWorkflows.py"
elasticWorkflow="processElasticWorkflows.py"
install_dir=$(pwd)
parentdir="$(dirname "$install_dir")"

export PYTHONPATH=$PYTHONPATH:$parentdir

echo $PYTHONPATH

num_of_processes=`ps -ef | grep $oozieWorkflow | grep -v grep | awk '{print $2}' | wc -l`
if [ $num_of_processes -ne 0 ]
then
   echo "process processOzzieWorkflows is already running"
else
   python $install_dir/$oozieWorkflow > $install_dir/processOzzieWorkflows.err 2>&1 &
   if [ $? -eq 0 ]
   then
     echo "processOzzieWorkflows started in the background"
   else
     echo "Failed to start processOzzieWorkflows in the background"
   fi
fi

num_of_elastic_processes=`ps -ef | grep $elasticWorkflow | grep -v grep | awk '{print $2}' | wc -l`
if [ $num_of_elastic_processes -ne 0 ]
then
   echo "process processElasticWorkflows already running"
else
   python $install_dir/$elasticWorkflow > $install_dir/processElasticWorkflows.err 2>&1 &
   if [ $? -eq 0 ]
   then
     echo "processElasticWorkflows started in the background"
   else
     echo "Failed to start processOzzieWorkflows in the background"
   fi
fi
