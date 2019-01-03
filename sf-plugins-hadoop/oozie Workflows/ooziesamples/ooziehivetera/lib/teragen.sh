export HADOOP_USER_NAME=pnda
if $(hadoop fs -test -d /tmp/teragen/test) ;
    then echo `hadoop fs -rm -r /tmp/teragen/test`
fi
hadoop jar /usr/hdp/2.6.4.0-91/hadoop-mapreduce/hadoop-mapreduce-examples.jar teragen -D mapred.map.task=5 1000 /tmp/teragen/test 1>/tmp/oozie_stdout.txt 2>/tmp/oozie_stderr.txt
exit 0
