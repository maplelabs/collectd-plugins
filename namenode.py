import time
import requests
from copy import deepcopy
import subprocess
from subprocess import check_output
import collectd
from utils import * # pylint: disable=W
from constants import * # pylint: disable=W
import sys
from os import path
import os
from utils import * # pylint: disable=W
import write_json
sys.path.append(path.dirname(path.abspath("/opt/collectd/plugins/sf-plugins-hadoop/Collectors/configuration.py")))
sys.path.append(path.dirname(path.abspath("/opt/collectd/plugins/sf-plugins-hadoop/Collectors/hadoopClusterCollector/name_node.py")))
sys.path.append(path.dirname(path.abspath("/opt/collectd/plugins/sf-plugins-hadoop/Collectors/requirements.txt")))

from configuration import *
from name_node import run_application, initialize_app

class Namenode:
    def __init__(self):
        self.retries = 3
        self.url_knox = "https://localhost:8443/gateway/default/ambari/api/v1/clusters"
        self.cluster_name = None
        self.knox_username = "admin"
        self.knox_password = "admin"

    def check_fields(self, line, dic_fields):
        for field in dic_fields:
            if (field+"=" in line or field+" =" in line):
                return field
        return None

    def update_config_file(self, previous_json_nn):
        file_name = "/opt/collectd/plugins/sf-plugins-hadoop/Collectors/configuration.py"
        lines = []
        flag = 0
        previous_json_nn = previous_json_nn.strip(".")

        logging_config["namenode"] = logging_config["namenode"].strip(".")
        logging_config["hadoopCluster"] = logging_config["hadoopCluster"].strip(".")
        dic_fields = { "name_node": name_node,"elastic": elastic, "indices": indices, "tag_app_name": tag_app_name, "previous_json_nn": previous_json_nn, "logging_config": logging_config}
        with open(file_name, "r") as read_config_file:
            for line in read_config_file.readlines():
                field = self.check_fields(line, dic_fields)
                if field and ("{" in line and "}" in line):
                    lines.append("%s = %s\n" %(field, dic_fields[field]))
                elif field or flag:
                    if field:
                        if field == "previous_json_nn":
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
            collectd.error("Couldn't get cluster name")
            return None
        cluster_name = res_json.json()["items"][0]["Clusters"]["cluster_name"]
        return cluster_name

    def get_hadoop_service_details(self, url):
        res_json = requests.get(url, auth=(self.knox_username, self.knox_password), verify=False)
        if res_json.status_code != 200:
            collectd.error("Couldn't get history_server details")
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

        host, port, index = self.get_elastic_search_details()
        elastic["host"] = host
        elastic["port"] = port
        name_node["port"] = "50070"
        indices["namenode"] = index
        appname = self.get_app_name()
        tag_app_name['namenode'] = appname
        cluster_name = self.get_cluster()
        name_node["hosts"] = self.get_hadoop_service_details(self.url_knox+"/"+cluster_name+"/services/HDFS/components/NAMENODE")
        self.update_config_file(previous_json_nn)
        initialize_app()

    @staticmethod
    def add_common_params(namenode_dic, doc_type):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        hostname = gethostname()
        timestamp = int(round(time.time()))

        namenode_dic[HOSTNAME] = hostname
        namenode_dic[TIMESTAMP] = timestamp
        namenode_dic[PLUGIN] = 'namenode'
        namenode_dic[ACTUALPLUGINTYPE] = 'namenode'
        namenode_dic[PLUGINTYPE] = doc_type

    def collect_data(self):
        """Collects all data."""
        data = run_application(index=0)
        docs = [{"nameNodeStatsRpcActivity": "_tag_hostname: manager-1", "_tag_context": "rpc", "RpcQueueTimeAvgTime": 0.066667, "RpcProcessingTimeAvgTime": 0.244444, "SentBytes": 0, "RpcAuthenticationSuccesses": 0, "_tag_numopenconnectionsperuser": 0, "modelerType": "RpcActivityForPort8020", "RpcClientBackoff": 0, "RpcAuthenticationFailures": 0, "_tag_port": 8020, "_documentType": "nameNodeStatsRpcActivity", "_plugin": "namenode", "RpcSlowCalls": 0, "ReceivedBytes": 0, "RpcAuthorizationFailures": 0, "NumOpenConnections": 0, "RpcAuthorizationSuccesses": 0, "name": "Hadoop:service=NameNode,name=RpcActivityForPort8020", "RpcProcessingTimeNumOps": 0, "RpcQueueTimeNumOps": 0, "time": 0, "CallQueueLength": 0}]
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

namenodeinstance = Namenode()
collectd.register_config(namenodeinstance.read_config) # pylint: disable=E1101
collectd.register_read(namenodeinstance.read_temp) # pylint: disable=E1101
