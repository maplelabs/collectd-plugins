#!/bin/bash

if [ $# == 0 ]; then
  echo "Since no option is provided , will build all"
elif [ $# == 1 ]; then
  type="$1"
fi

rm -rf ./production # does not throw error if directory is not there
rm -f ./*.zip
rm -rf ./dependencies/*

mkdir -p dependencies 

if [ $? -eq 0 ]; then
  echo "Downloading the packages with pip download"
  pip download -d dependencies -r requirements.txt
else
  echo "Failed to create dependencies directory"
  exit 1
fi

if [ $? -eq 0 ]; then
  if [[ $type == "oozie" ]]; then
    zip -r Collectors.zip OozieJobsCollector dependencies library configuration.py requirements.txt
  elif [[ $type == "hadoop" ]]; then 
    zip -r Collectors.zip hadoopClusterCollector dependencies library configuration.py requirements.txt
  elif [[ $type == "spark" ]]; then
    zip -r Collectors.zip sparkJobsCollector dependencies library configuration.py requirements.txt
  else
    zip -r Collectors.zip OozieJobsCollector hadoopClusterCollector sparkJobsCollector dependencies library configuration.py requirements.txt
  fi
else
  echo "Failed to download packages"
  exit 1
fi

if [ $? -eq 0 ]; then
  echo "Build Successful"
else
  echo "Failed to successfully build"
fi


