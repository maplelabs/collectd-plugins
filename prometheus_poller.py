"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
"""Python plugin for collectd to fetch kafkaStats and topicStats for kafka_topic process"""

#!/usr/bin/python
import json
import time
import collectd
import requests
from copy import deepcopy
import re
import traceback

# user imports
import utils
from constants import *

class PrometheusStat(object):
    """Plugin object will be created only once and collects JMX statistics info every interval."""

    def __init__(self, conf):
        """Initializes interval and previous dictionary variable."""
        self.interval = conf["interval"]
        self.plugin_name = conf["name"]
        self.host = 'localhost'
        self.port = conf["port"]

    def connection_available(self):
        """

        :return:
        """
        url = "http://{}:{}".format(self.host, self.port)
        headers = {'content-type': 'application/json'}
        try:
            resp = requests.get(url, headers=headers, timeout=60)
        except Exception as err:
            collectd.info("Error in connecting service to %s in %s due to %s" %(self.host, self.port, str(err)))
            return False
        if resp.status_code == 200:
            return True

    def get_elastic_search_details(self):
        try:
            with open("/opt/collectd/conf/elasticsearch.conf", "r") as file_obj:
                for line in file_obj.readlines():
                    if "URL" not in line:
                        continue
                    elastic_search = line.split("URL")[1].split("//")[1].split("/")
                    index = elastic_search[1].strip("/").strip("_doc").split('_')
                    elastic_search = elastic_search[0].split(":")
                    return elastic_search[0], elastic_search[1], index[0]
        except IOError:
            collectd.error("Could not read file: /opt/collectd/conf/elasticsearch.conf")

    def add_common_params(self, prometheus_dict):
        """Adds TIMESTAMP, PLUGIN, PLUGITYPE to dictionary."""
        timestamp = int(round(time.time()))
        for details_type, details in prometheus_dict.items():
            details[TIMESTAMP] = timestamp
            details[PLUGIN] = self.plugin_name
            details[PLUGINTYPE] = details_type
            details[ACTUALPLUGINTYPE] = self.plugin_name

        collectd.info("Plugin Prometheus: Added common parameters successfully")

    def poll_metrics(self):
        '''
        func: Poll prometheus metrics from exporter running on the endpoint
        :return: prometheus metrics exposed by the exporter.
        '''
        try:
            collectd.info("Establishing connection with prometheus exporter %s " % (self.plugin_name))
            url = "http://{}:{}/metrics".format(self.host, self.port)
            headers = {'content-type': 'application/json'}
        except Exception as err:
            collectd.error("Error constructing URL for prometheus exporter %s" % (self.plugin_name))
            return

        try:
            collectd.info("Sending GET request for prometheus exporter %s." % (self.plugin_name))
            resp = requests.get(url, headers=headers, timeout=60)
        except Exception as err:
            collectd.error("Error getting metrics for prometheus server %s." % (self.plugin_name))
            collectd.error("%s", str(err))
            return

        if resp.status_code == 200:
            self.prom_response = resp.content
            collectd.info("Successfully polled for metric %s" % (self.plugin_name))
            return resp.content

        collectd.error("Response code for metrics request for prometheus server %s is %s."
                       % (self.plugin_name, resp.status_code))

    def convert_metrics(self, prometheus_metrics):
        """
        func: Convert the metrics collected from the endpoint to a readable Json object
        :param plugin_detail:
        :return:
        """
        # TBA : Convert only those metrics to Json object which is mentioned
        # in prometheus_regex.json.
        doc_type = self.plugin_name.split('prometheus')[1] + 'Stats'
        collectd.info("Converting the metrics to Json format for further processing for plugin %s." % (self.plugin_name))
        prometheus_json = {}

        prometheus_json[doc_type] = {}
        metric_dict= {}
        metric_dict["metrics"] = []
        document_name = None
        count = 0
        int_pattern = re.compile(r'\d+')
        float_pattern = re.compile(r'(\d+\.\d+(e\+\d+|e\-\d+)?|\d+(e\+\d+|e\-\d+))')
        for line in prometheus_metrics.splitlines():
            try:
                count += 1
                if "# HELP" in line:
                    document_name = line.split(" ")[2]
                    collectd.info("Converting metrics to json for plugin %s for document %s"
                                  % (self.plugin_name, document_name))
                    prometheus_json[doc_type][document_name] = {}
                    metric_dict["HELP"] = " ".join(line.split(" ")[3:])
                    continue

                elif "# TYPE" in line:
                    metric_dict["TYPE"] = line.split(" ")[3]
                    continue

                else:
                    metric = {}
                    if "{" in line:
                        if "}" in line:
                            items = line.split("{")[1].split("} ")[0].split(",")
                        else:
                            items = line.split("{")[1].split(" ")[0].strip("}").split(",")

                        for item in items:
                            # metric[re.sub('[^A-Za-z0-9.]+', '', item.split("=")[0])] = \
                            #     re.sub('[^A-Za-z0-9.]+', '', item.split("=")[1])
                            metric[re.sub('[^A-Za-z0-9/()-_.]+', '', item.split("=")[0])] = \
                                re.sub('[^A-Za-z0-9/()-_.]+', '', item.split("=")[1])
                    if "}" in line:
                        value = line.split("} ")[1]
                        if float_pattern.search(value):
                            value = float(value)
                        elif int_pattern.search(value):
                            value = int(value)
                        metric["value"] = value
                    else:
                        value = line.split(" ")[1]
                        if float_pattern.search(value):
                            value = float(value)
                        elif int_pattern.search(value):
                            value = int(value)
                        metric["value"] = value
                    metric_dict["metrics"].append(metric)

                    if count == len(prometheus_metrics.splitlines()):
                        prometheus_json[doc_type][document_name] = metric_dict
                        break

                    elif "# HELP" in prometheus_metrics.splitlines()[count]:
                        prometheus_json[doc_type][document_name] = metric_dict
                        metric_dict = {}
                        metric_dict["metrics"] = []

            except Exception as err:
                collectd.error("Error converting file based metrics for \
                                prometheus plugin %s to dictionary format." % (self.plugin_name))
                return

        collectd.info("Successfully converted the metrics collected \
                        for plugin %s to dictionary format" % (self.plugin_name))
        return prometheus_json

    def generate_es_mapping(self, es_host, es_port, es_index):

        url = "http://{}:{}/metrics".format(self.host, self.port)
        headers = {'content-type': 'application/json'}
        index_avail_check = 0
        try:
            resp = requests.get(url, headers=headers, timeout=60)
        except Exception as err:
            collectd.error("Error in Retrieving metrics from prometheus server for generating mapping of nested datatypes")
        if resp.status_code == 200:
            collectd.info("Successfully polled metrics for host %s for generating mapping of nested type" % (self.host))
        response = resp.content
        es_url = "http://{}:{}/{}/_mapping".format(es_host, es_port, es_index)
        try:
            es_resp = requests.get(es_url, headers=headers, timeout=60)
        except Exception as err:
            collectd.error("Error in Retrieving es index info")
        custommap = {}
        prevmetrickey = ""
        check = 0

        for line in response.splitlines():
            if "{" in line:
                metrickey = line.split('{', 1)[0]
                tempmap = {metrickey: {"properties": {"metrics": {"type": "nested"}}}}
                if check == 0:
                    custommap["properties"] = tempmap
                    check = 1
                if(prevmetrickey != metrickey):
                    custommap["properties"].update(tempmap)
                prevmetrickey = metrickey
                test_metric = metrickey
        if es_resp.status_code == 200:
            es_response = json.loads(es_resp.content)
            try:
                for res_metrics in es_response[es_index]["mappings"]["_doc"]["properties"]:
                    if res_metrics == test_metric:
                        index_avail_check = 1
                        collectd.info("Nested Index Already Mapped")

            except KeyError as error:
                index_avail_check = 0
        else:
            index_avail_check = 0

        if index_avail_check == 0:
            headers = {'content-type': 'application/json'}
            es_url_doc = "http://{}:{}/{}/_mappings/_doc".format(es_host, es_port,es_index)
            resp = requests.post(es_url_doc, data=json.dumps(custommap), headers=headers, timeout=60)
            collectd.info("Nested Datatype Mapped to ES")

    def collect_data(self):
        """

        :return:
        """
        try:

            prometheus_metrics = self.poll_metrics()
            if not prometheus_metrics:
                collectd.error("Plugin prometheus: Unable to fetch data for Prometheus.")
                return

            self.exporter_metrics = self.convert_metrics(prometheus_metrics)
            self.add_common_params(self.exporter_metrics)
            es_host, es_port, es_index = self.get_elastic_search_details()
            self.generate_es_mapping(es_host, es_port, es_index)
            target_setting_url = "http://{}:{}/{}/_settings".format(es_host, es_port, es_index)
            sett_data = {"index.mapping.nested_fields.limit": "5000", "index.mapping.total_fields.limit": "8000"}
            headers = {'content-type': 'application/json'}
            sett_resp = requests.put(target_setting_url, data=json.dumps(sett_data), headers=headers, timeout=60)
            return self.exporter_metrics
        except Exception as err:
            collectd.error("Plugin Prometheus: Error in collecting stats : %s" % (str(err)))


    def read(self):
        """Collects stats and spawns process for each pids."""
        dict_prometheus = {}
        try:
            if self.connection_available():
                dict_prometheus = self.collect_data()
            if dict_prometheus:
                self.dispatch_data(deepcopy(dict_prometheus))
        except Exception as err:
            collectd.error("Error in collecting stats for prometheus %s due to %s" %(str(err), traceback.format_exc()))

    def dispatch_data(self, result):
        for details_type, details in result.items():
            utils.dispatch(details)
