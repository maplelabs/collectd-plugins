#!/bin/bash

nameNode="name_node.py"

num_of_processes=`ps -ef | grep $nameNode | grep -v grep | awk '{print $2}' | wc -l`

if [ $num_of_processes -eq 0 ]
then
   echo "process nameNode is not running"
else
   ps -ef | grep $nameNode | grep -v grep | awk '{print $2}' | xargs kill -9
   if [ $? -eq 0 ]
   then
      echo "Sent TERM signal to nameNode. It will exit gracefully "
    else
      echo "Failed to send nameNode a TERM signal"
   fi
fi

