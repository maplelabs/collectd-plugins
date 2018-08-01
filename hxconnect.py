"""
A collectd plugin for analysing hx controller cluster 
test results
"""

import collectd
import time
import json
import os
import requests

from constants import *
from utils import *
from copy import deepcopy

class Hx_controllerResults:

    def __init__(self):
        self.interval = HX_INTERVAL
        self.pollCounter = 0
        self.end_time = int(time.time())
        self.start_time = self.end_time - self.interval
        self.timestamp_format = '%H:%M_%Y%m%d'

    def read_config(self, cfg):
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == HX_CONNECT_IP:
                self.hx_cluster_ip = children.values[0]

    @staticmethod
    def add_common_params(hx_cluster_data, doc_type):
        hostname = gethostname()
        timestamp = int(round(time.time()))

        hx_cluster_data[HOSTNAME] = hostname
        hx_cluster_data[TIMESTAMP] = timestamp
        hx_cluster_data[PLUGIN] = HXCONNECT
        hx_cluster_data[ACTUALPLUGINTYPE] = HXCONNECT
        hx_cluster_data[PLUGINTYPE] = HXCONNECT


    @staticmethod
    def dispatch_data(hx_cluster_data):
        collectd.info("Plugin HX_CONNECT: Values: " + json.dumps(hx_cluster_data))
        dispatch(hx_cluster_data)

    def format_time(self):
        cur_time = time.strftime(self.timestamp_format,time.localtime(self.end_time))
        prev_time = time.strftime(self.timestamp_format,time.localtime(self.start_time))
        self.fromTime = prev_time
        self.until = cur_time


    def collect_hx_cluster_data(self):
        """
        # Collecting the hx_cluster data
        """
        try:
            basic_url = "https://" + self.hx_cluster_ip + "/render"
            data_dict = {}
            self.format_time()
            date_time = subprocess.check_output("timedatectl", shell=True)
            date_time = date_time.split("\n")
            for each_line in date_time:
                if "Time zone" in each_line:
                    time_zone = (each_line.strip()).split(" ")
            tz = time_zone[2]
            for each_val in PARAMS_LIST:
                params = {"format":"json"}
                params["tz"] = tz
                params["from"] = self.fromTime
                params["until"] = self.until
                params["target"] = APIVALS_DICT[each_val]["target"]
                res = requests.get(basic_url, params=params, verify=False)
                res = res.json()
                if res:
                    for each_datapoint in res[0]["datapoints"]:
                        if each_datapoint[0]:
                            #data_dict[APIVALS_DICT[each_val]["key"]] = res[0]["datapoints"][0][0]
                            data_dict[APIVALS_DICT[each_val]["key"]] = each_datapoint[0]
                        else:
                            pass
            self.start_time = self.start_time + int(self.interval)
            self.end_time = self.end_time + int(self.interval)
            return data_dict
        except Exception as e:
            collectd.debug("Couldn't collect hx controller data for the host %s due to %s" % (self.hx_cluster_ip, e))


    def collect_results(self):
        hx_cluster_data = self.collect_hx_cluster_data()
        self.add_common_params(hx_cluster_data, "hx_cluster")
        self.dispatch_data(hx_cluster_data)


    def read(self):
        self.pollCounter += 1
        self.collect_results() 
        
    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))

obj = Hx_controllerResults()
collectd.register_config(obj.read_config)
collectd.register_read(obj.read_temp)

