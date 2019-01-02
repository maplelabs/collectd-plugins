#!/bin/bash

containers="containers.py"

num_of_processes=`ps -ef | grep $containers | grep -v grep | awk '{print $2}' | wc -l`

if [ $num_of_processes -eq 0 ]
then
   echo "process containers is not running"
else
   ps -ef | grep $containers | grep -v grep | awk '{print $2}' | xargs kill -9
   if [ $? -eq 0 ]
   then
      echo "Sent TERM signal to containers. It will exit gracefully "
    else
      echo "Failed to send containers a TERM signal"
   fi
fi

