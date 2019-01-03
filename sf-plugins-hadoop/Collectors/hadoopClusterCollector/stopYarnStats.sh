#!/bin/bash

yarnStats="yarn_stats.py"

num_of_processes=`ps -ef | grep $yarnStats | grep -v grep | awk '{print $2}' | wc -l`

if [ $num_of_processes -eq 0 ]
then
   echo "process yarnStats is not running"
else
   ps -ef | grep $yarnStats | grep -v grep | awk '{print $2}' | xargs kill -9
   if [ $? -eq 0 ]
   then
      echo "Sent TERM signal to yarnStats. It will exit gracefully "
    else
      echo "Failed to send yarnStats a TERM signal"
   fi
fi
