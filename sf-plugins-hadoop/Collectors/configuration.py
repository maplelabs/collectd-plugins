"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""

import json
import os

oozie = {'host': '10.81.1.98', 'scheme': 'http', 'port': '11000', 'jobs_size': 250}

elastic = {'host': '10.81.1.212', 'scheme': 'http', 'port': '9200'}

timeline_server = {'host': '10.81.1.98', 'scheme': 'http', 'port': '8188'}

job_history_server = {'host': '10.81.1.183', 'scheme': 'http', 'port': '19888'}

resource_manager = {'scheme': 'http', 'hosts': ['10.81.1.98', '10.81.1.104'], 'port': '8088'}

name_node = {'scheme': 'http', 'hosts': ['10.81.1.98', '10.81.1.104'], 'port': '50070'}

indices = {'namenode': 'snappyflow_write', 'spark': 'snappyflow_write', 'workflowmonitor': 'workflowmonitornew', 'yarn': 'snappyflow_write', 'workflow': 'snappyflow_write'}

tag_app_name = {'namenode': 'hadoopapp1', 'spark': 'hadoopapp1', 'yarn': 'hadoopapp1', 'oozie': 'hadoopapp1'}

plugin_name = {
   "oozie" : "oozie",
   "yarn" : "yarn",
   "namenode": "namenode",
    "spark": "spark"
}

reduce_start = 70.0
out_file = "./var/log/hueristics/hadoop-plugin-test"

logging_config = {
    "ozzieWorkflows" : "./var/log/processOzzieWorkflows.log",
    "elasticWorkflows" : "./var/log/processElasticWorkflows.log",
    "sparkJobs": "./var/log/sparkJobs.log",
    "hadoopCluster": "./var/log/hadoopClusterStats.log",
    "yarn": "./var/log/yarnStats.log",
    "namenode": "./var/log/namenode.log"
}

previous_json_yarn = "/var/previous_json_yarn"
previous_json_nn = "/var/previous_json_nn"


sleep_time_in_secs = {
    "oozieWorkflows": 30,
    "elasticWorkFlows": 30
}

tasks_by_time = {
    "numOfDataPoints": 25,
    "minimumInterval": 5  # seconds
}

read_timeout = None
write_timeout = None

kerberos = {
    "enabled" : False,
    "principal" : "",
    "keytab_file": ""
}

hdfs_jobhistory_directory = "/mr-history/done"
jobhistory_copy_dir = "/var/jhist"
hueristics_out_dir = "./var/hueristics"
use_rest_api = 0

hdfs = {'url': 'http://10.81.1.154:50070;http://10.81.1.98:50070', 'timezone': 'US/Eastern', 'user': 'root'}

modify_user_agent_header = True

spark2_history_server = {
    "scheme": "http",
    "host": "10.81.1.183",
    "port": 18081
}

cluster_timezones = {
    'EST': "US/Eastern",
    "EDT": "US/Eastern",
    "CST": "US/Central",
    "CDT": "US/Central",
    "PST": "US/Pacific",
    "PDT": "US/Pacific"
}

time_before_in_seconds = 30000
yarn_stats_time_interval = 30
container_stats_time_interval = 120
name_node_stats_time_interval = 120

tasks_by_time = {
    'numOfDataPoints': 25,
    'minimumInterval': 5
}

app_status = {
    'use_redis' : True,
    'oozie-key': "oozieStatus",
    'spark-key': "sparkStatus"

}

redis_server = {
    "host": "127.0.0.1",
    "port": 6379,
    "password": None
}

hdfs_spark2history_directory = "/spark2-history/"
sparkhistory_copy_dir = "/tmp"
spark_job_history = True

update_old_wf_status = {'update': 0}

def initialize_configuration(file):
    if file:
        if not os.path.isfile(file):
            print("Error reading from config file:{0} Exiting".format(file))
            exit(1)

        with open(file, 'r') as f:
            config = json.load(f)

        for key, value in config.iteritems():
            globals()[key] = value

        print(globals())



