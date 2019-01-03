#!/bin/bash

sparkApps="processSparkApps.py"

num_of_processes=`ps -ef | grep $sparkApps | grep -v grep | awk '{print $2}' | wc -l`

if [ $num_of_processes -eq 0 ]
then
   echo "process processSparkApps is not running"
else
   ps -ef | grep $sparkApps | grep -v grep | awk '{print $2}' | xargs kill -9
   if [ $? -eq 0 ]
   then
      echo "Sent TERM signal to processSparkApps. It will exit gracefully "
    else
      echo "Failed to send processSparkApps a TERM signal"
   fi
fi 

