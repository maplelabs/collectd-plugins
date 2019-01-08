"""Python plugin for collectd to fetch oozie workflow statistics information."""

#!/usr/bin/python
import sys
import requests
from os import path
import signal # pylint: disable=unused-import
import time
from datetime import datetime # pylint: disable=W
import json
import subprocess
from subprocess import check_output
from copy import deepcopy
import collectd
import os
import math
from utils import *
import write_json
from constants import * # pylint: disable=W
sys.path.append(path.dirname(path.abspath("/opt/collectd/plugins/sf-plugins-hadoop/Collectors/configuration.py")))
sys.path.append(path.dirname(path.abspath("/opt/collectd/plugins/sf-plugins-hadoop/Collectors/OozieJobsCollector/processOzzieWorkflows.py")))
sys.path.append(path.dirname(path.abspath("/opt/collectd/plugins/sf-plugins-hadoop/Collectors/OozieJobsCollector/processElasticWorkflows.py")))
sys.path.append(path.dirname(path.abspath("/opt/collectd/plugins/sf-plugins-hadoop/Collectors/requirements.txt")))

from configuration import *
from processOzzieWorkflows import run_application, initialize_app
from processElasticWorkflows import run_application as run_application_elastic
from processElasticWorkflows import initialize_app as initialize_app_elastic


class Oozie:
    """Plugin object will be created only once and collects oozie statistics info every interval."""
    def __init__(self):
        """Initializes interval, oozie server, Job history and Timeline server details"""
        self.retries = 3
        self.url_knox = "https://localhost:8443/gateway/default/ambari/api/v1/clusters"
        self.cluster_name = None
        self.is_config_updated = 0
        self.knox_username = "admin"
        self.knox_password = "admin"

    def check_fields(self, line, dic_fields):
        for field in dic_fields:
            if (field+"=" in line or field+" =" in line):
                return field
        return None

    def update_config_file(self, use_rest_api, jobhistory_copy_dir):
        file_name = "/opt/collectd/plugins/sf-plugins-hadoop/Collectors/configuration.py"
        lines = []
        flag = 0
        jobhistory_copy_dir = jobhistory_copy_dir.strip(".")
        logging_config["ozzieWorkflows"] = logging_config["ozzieWorkflows"].strip(".")
        logging_config["elasticWorkflows"] = logging_config["elasticWorkflows"].strip(".")
        logging_config["hadoopCluster"] = logging_config["hadoopCluster"].strip(".")
        dic_fields = {"oozie": oozie, "job_history_server": job_history_server, "timeline_server": timeline_server, "elastic": elastic, "indices": indices, "use_rest_api": use_rest_api, "hdfs": hdfs, "jobhistory_copy_dir": jobhistory_copy_dir, "tag_app_name": tag_app_name, "logging_config": logging_config}
        with open(file_name, "r") as read_config_file:
            for line in read_config_file.readlines():
                field = self.check_fields(line, dic_fields)
                if field and ("{" in line and "}" in line):
                    lines.append("%s = %s\n" %(field, dic_fields[field]))
                elif field or flag:
                    if field:
                        if field == "jobhistory_copy_dir":
                            lines.append('%s = "%s"\n' %(field, dic_fields[field]))
                        else:
                            lines.append("%s = %s\n" %(field, dic_fields[field]))
                    if field and "{" in line:
                        flag = 1
                    if "}" in line:
                        flag = 0
                else:
                    lines.append(line)
        read_config_file.close()
        with open(file_name, "w") as write_config:
            for line in lines:
                write_config.write(line)
        write_config.close()

    def get_app_name(self):
        try:
            with open("/opt/collectd/conf/filters.conf", "r") as file_obj:
                for line in file_obj.readlines():
                    if 'MetaData "_tag_appName"' not in line:
                        continue
                    return line.split(" ")[2].strip('"')
        except IOError:
            collectd.error("Could not read file: /opt/collectd/conf/filters.conf")

    def get_elastic_search_details(self):
        try:
            with open("/opt/collectd/conf/elasticsearch.conf", "r") as file_obj:
                for line in file_obj.readlines():
                    if "URL" not in line:
                        continue
                    elastic_search = line.split("URL")[1].split("//")[1].split("/")
                    index = elastic_search[1].strip("/").strip("_doc")
                    elastic_search = elastic_search[0].split(":")
                    return elastic_search[0], elastic_search[1], index
        except IOError:
            collectd.error("Could not read file: /opt/collectd/conf/elasticsearch.conf")

    def get_cluster(self):
        res_json = requests.get(self.url_knox, auth=(self.knox_username, self.knox_password), verify=False)
        if res_json.status_code != 200:
            return None
        cluster_name = res_json.json()["items"][0]["Clusters"]["cluster_name"]
        return cluster_name


    def get_hadoop_service_details(self, url):
        res_json = requests.get(url, auth=(self.knox_username, self.knox_password), verify=False)
        if res_json.status_code != 200:
            collectd.error("Couldn't get server details")
            return None
        lst_servers = []
        res_json = res_json.json()
        for host_component in res_json["host_components"]:
            lst_servers.append(host_component["HostRoles"]["host_name"])
        return lst_servers

    def read_config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            elif children.key == USE_REST_API:
                use_rest_api = int(children.values[0])

        host, port, index = self.get_elastic_search_details()
        elastic["host"] = host
        elastic["port"] = port
        indices["workflow"] = index
        appname = self.get_app_name()
        tag_app_name['oozie'] = appname
        self.cluster_name = self.get_cluster()

        job_history_server["port"] = "19888"
        timeline_server["port"] = "8188"
        oozie["port"] = "11000"
        self.hdfs_port = "50070"
        if not os.path.isdir(jobhistory_copy_dir):
            try:
                os.mkdir(jobhistory_copy_dir)
            except:
                collectd.error("Unable to create job history directory %s" %jobhistory_copy_dir)

        if self.cluster_name:
            job_history_host = self.get_hadoop_service_details(self.url_knox+"/"+self.cluster_name+"/services/MAPREDUCE2/components/HISTORYSERVER")
            if job_history_host:
                job_history_server["host"] = job_history_host[0]
            else:
                collectd.error("Unable to get Job_history ip")
            timeline_host = self.get_hadoop_service_details(self.url_knox+"/"+self.cluster_name+"/services/YARN/components/APP_TIMELINE_SERVER")
            if timeline_host:
                timeline_server["host"] = timeline_host[0]
            else:
                collectd.error("Unable to get timeline_server ip")
            oozie_host = self.get_hadoop_service_details(self.url_knox+"/"+self.cluster_name+"/services/OOZIE/components/OOZIE_SERVER")
            if oozie_host:
                oozie["host"] = oozie_host[0]
            else:
                collectd.error("Unable to get oozie ip")
            self.hdfs_hosts = self.get_hadoop_service_details(self.url_knox+"/"+self.cluster_name+"/services/HDFS/components/NAMENODE")
            if self.hdfs_hosts:
                if len(self.hdfs_hosts) == 2:
                    hdfs["url"] = "http://{0}:{1};http://{2}:{3}" .format(self.hdfs_hosts[0], self.hdfs_port, self.hdfs_hosts[1], self.hdfs_port)
                else:
                    hdfs["url"] = "http://{0}:{1}" .format(self.hdfs_hosts[0], self.hdfs_port)
            else:
                collectd.error("Unable to get oozie ip")
            if job_history_host and timeline_host and oozie_host and self.hdfs_hosts:
                self.update_config_file(use_rest_api, jobhistory_copy_dir)
                self.is_config_updated = 1
                initialize_app()
                initialize_app_elastic()
        else:
            collectd.error("Unable to get cluster name")

    def add_common_params(self, oozie_dict, doc_type):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        hostname = gethostname()
        timestamp = int(round(time.time()))

        oozie_dict[HOSTNAME] = hostname
        oozie_dict[TIMESTAMP] = timestamp
        oozie_dict[PLUGIN] = 'oozie'
        oozie_dict[ACTUALPLUGINTYPE] = 'oozie'
        oozie_dict[PLUGINTYPE] = doc_type

    def collect_data(self):
        """Collects all data."""
        if self.is_config_updated:
            data = run_application()
            data = run_application_elastic(index=0)
        collectd.info("oozie workflow collection successful")
        docs = [{"wfId": 0, "wfaId": 0, "wfName": 0, "wfaName": 0, "time": int(math.floor(time.time())), "jobId": 0, 'timePeriodStart': 0, 'timePeriodEnd': 0, "mapTaskCount": 0, "reduceTaskCount": 0, 'duration': 0, "_plugin": plugin_name['oozie'], "_documentType": "taskCounts","_tag_appName": tag_app_name['oozie']}]
        for doc in docs:
            self.add_common_params(doc, doc['_documentType'])
            write_json.write(doc)

    def read(self):
        self.collect_data()

    def read_temp(self):
        """
        Collectd first calls register_read. At that time default interval is taken,
        hence temporary function is made to call, the read callback is unregistered
        and read() is called again with interval obtained from conf by register_config callback.
        """
        collectd.unregister_read(self.read_temp) # pylint: disable=E1101
        collectd.register_read(self.read, interval=int(self.interval)) # pylint: disable=E1101

oozieinstance = Oozie()
collectd.register_config(oozieinstance.read_config) # pylint: disable=E1101
collectd.register_read(oozieinstance.read_temp) # pylint: disable=E1101
