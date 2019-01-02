export HADOOP_USER_NAME=pnda
if $(hadoop fs -test -d /tmp/terasort/test) ;
then echo `hadoop fs -rm -r /tmp/terasort/test`
fi
hadoop jar /usr/hdp/2.6.4.0-91/hadoop-mapreduce/hadoop-mapreduce-examples.jar terasort -D mapred.map.task=5 -Dmapred.reduce.slowstart.completed.maps=0.95 -Dmapred.reduce.tasks=15 /tmp/teragen/test /tmp/terasort/test 1>/tmp/oozie_stdout.txt 2>/tmp/oozie_stderr.txt
exit 0
