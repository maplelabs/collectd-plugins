#!/bin/bash

hadoopClusterStats="getHadoopClusterStats.py"

num_of_processes=`ps -ef | grep $hadoopClusterStats | grep -v grep | awk '{print $2}' | wc -l`

if [ $num_of_processes -eq 0 ]
then
   echo "process hadoopClusterStats is not running"
else
   ps -ef | grep $hadoopClusterStats | grep -v grep | awk '{print $2}' | xargs kill -9
   if [ $? -eq 0 ]
   then
      echo "Sent TERM signal to hadoopClusterStats. It will exit gracefully "
    else
      echo "Failed to send hadoopClusterStats a TERM signal"
   fi
fi
