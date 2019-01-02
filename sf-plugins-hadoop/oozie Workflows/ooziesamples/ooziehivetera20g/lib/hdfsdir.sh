export HADOOP_USER_NAME=pnda
hadoop fs -mkdir -p /tmp/teragen
hadoop fs -chmod -R 777 /tmp/teragen
hadoop fs -mkdir -p /tmp/terasort
hadoop fs -chmod -R 777 /tmp/terasort
