"""Python plugin for collectd to fetch oozie workflow statistics information."""

#!/usr/bin/python
import sys
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
        dic_fields = {"oozie": oozie, "job_history_server": job_history_server, "timeline_server": timeline_server, "elastic": elastic, "indices": indices, "use_rest_api": use_rest_api, "hdfs": hdfs, "jobhistory_copy_dir": jobhistory_copy_dir, "tag_app_name": tag_app_name}
        with open(file_name, "r") as read_config_file:
            for line in read_config_file.readlines():
                field = self.check_fields(line, dic_fields)
                if field and ("{" in line and "}" in line):
                    lines.append("%s = %s\n" %(field, dic_fields[field]))
                elif field or flag:
                    if field:
                        if field == "jobhistory_copy_dir":
                            collectd.info("%s %s" %(field,dic_fields[field]))
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

    def run_cmd(self, cmd, shell, ignore_err=False, print_output=False):
        """
        return output and status after runing a shell command
        :param cmd:
        :param shell:
        :param ignore_err:
        :param print_output:
        :return:
        """
        for i in xrange(self.retries):
            try:
                output = subprocess.check_output(cmd, shell=shell)
                if print_output:
                    print output
                    return output
                return
            except subprocess.CalledProcessError as error:
                if not ignore_err:
                    print >> sys.stderr, "ERROR: {0}".format(error)
                    sleep(0.05)
                    continue
                else:
                    print >> sys.stdout, "WARNING: {0}".format(error)
                    return
        sys.exit(1)

    def read_config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            elif children.key == OOZIEHOST:
                oozie["host"] = children.values[0]
            elif children.key == OOZIEPORT:
                oozie["port"] = children.values[0]
            elif children.key == JOB_HISTORY_SERVER:
                job_history_server["host"]  = children.values[0]
            elif children.key == JOB_HISTORY_PORT:
                job_history_server["port"] = children.values[0]
            elif children.key == TIMELINE_SERVER:
                timeline_server["host"]  = children.values[0]
            elif children.key == TIMELINE_PORT:
                timeline_server["port"] = children.values[0]
            elif children.key == USE_REST_API:
                use_rest_api = int(children.values[0])
            elif children.key == HDFS_HOSTS:
                self.hdfs_hosts = children.values[0].split(",")
            elif children.key == HDFS_PORT:
                self.hdfs_port = children.values[0]
        host, port, index = self.get_elastic_search_details()
        elastic["host"] = host
        elastic["port"] = port
        indices["workflow"] = index
        appname = self.get_app_name()
        tag_app_name['oozie'] = appname
        if not os.path.isdir(jobhistory_copy_dir):
            try:
                os.mkdir(jobhistory_copy_dir)
            except:
                collectd.error("Unable to create job history directory %s" %jobhistory_copy_dir)
        if len(self.hdfs_hosts) == 2:
            hdfs["url"] = "http://{0}:{1};http://{2}:{3}" .format(self.hdfs_hosts[0], self.hdfs_port, self.hdfs_hosts[1], self.hdfs_port)
        else:
            hdfs["url"] = "http://{0}:{1}" .format(self.hdfs_hosts[0], self.hdfs_port)
        self.update_config_file(use_rest_api, jobhistory_copy_dir)
        cmd = "pip install -r /opt/collectd/plugins/sf-plugins-hadoop/Collectors/requirements.txt"
        self.run_cmd(cmd, shell=True, ignore_err=True)
        initialize_app()
        initialize_app_elastic()

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
        data = run_application()
        data = run_application_elastic(index=0)
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
