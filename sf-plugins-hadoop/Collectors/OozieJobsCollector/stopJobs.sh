#!/bin/bash

oozieWorkflow="processOzzieWorkflows.py"
elasticWorkflow="processElasticWorkflows.py"

num_of_processes=`ps -ef | grep $oozieWorkflow | grep -v grep | awk '{print $2}' | wc -l`

if [ $num_of_processes -eq 0 ]
then
   echo "process processOzzieWorkflows is not running"
else
   ps -ef | grep $oozieWorkflow | grep -v grep | awk '{print $2}' | xargs kill -9
   if [ $? -eq 0 ]
   then
      echo "Sent TERM signal to processOzzieWorkflows. It will exit gracefully "
    else
      echo "Failed to send processOzzieWorkflows a TERM signal"
   fi
fi 

num_of_elastic_processes=`ps -ef | grep $elasticWorkflow | grep -v grep | awk '{print $2}' | wc -l`
if [ $num_of_elastic_processes -eq 0 ]
then
   echo "process processElasticWorkflows is not running"
else
   ps -ef | grep $elasticWorkflow | grep -v grep | awk '{print $2}' | xargs kill -9
   if [ $? -eq 0 ]
   then
     echo "Sent TERM signal to processElasticWorkflows. It will exit gracefully "
   else
     echo "Failed to send processElasticWorkflows a TERM signal"
   fi
fi
