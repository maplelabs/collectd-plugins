""" A collectd-python plugin for downloading log file 
     from ESA cluster. """

import collectd
import urllib
import json
import time
import os

# user imports
from constants import *
from utils import *
from esa_conf import *

log_name_mapping = {"authentication": "authentication",
                    "gui_logs": "gui",
                    "httplogsMlabs": "gui.text",
                    "mail_logs": "mail",
                    "system_logs": "system"}

class ESALogs:
    def __init__(self):
        self.interval = DEFAULT_INTERVAL
        self.host = ESA_HOST
        self.user = ESA_USER
        self.password = ESA_PASSWORD
        self.log_name = None
        self.download_path = CALLER_LOG_PATH
        self.previousLogName = []

    def read_config(self, cfg):
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == "log_name":
                self.log_name = children.values[0]

    def checkLogDest(self):
        try:
            if not os.path.exists(self.download_path):
                os.makedirs('esa_logs')
        except Exception as err:
            collectd.error("Error in creating esa log folder %s" %(str(err)))
            return False
        return True

    def collect_log(self):
        final_details = {}
        try:
            if (self.checkLogDest()):
                esa_url = 'ftp://{0}:{1}@{2}/{3}/{4}.current'.format(self.user, self.password, self.host, self.log_name, log_name_mapping[self.log_name])
                download_path = self.download_path + self.log_name + '.txt'
                urllib.urlretrieve(esa_url, download_path)
                #urllib.urlretrieve('ftp://admin:ironport@10.11.100.82/'+ self.log_name +'/'+ log_name_mapping[self.log_name] +'.current', '/var/log/esa_logs/'+ self.log_name+'.txt')
                urllib.urlcleanup()
            else:
                final_details["status"] = "failed"
                collectd.info("Couldn't store the data")
                return final_details
        except Exception as e:
            collectd.error("Error in downloading log files due to %s" % (str(e)))
            return
        final_details["status"] = "success"
        collectd.info("Downloaded Log files from ESA cluster %s " %(str(data_dict)))
        return final_details

    @staticmethod
    def add_common_params(result_dict):
        hostname = gethostname()
        timestamp = int(round(time.time()))
        result_dict[HOSTNAME] = hostname
        result_dict[TIMESTAMP] = timestamp
        result_dict[PLUGIN] = "esa"
        result_dict[ACTUALPLUGINTYPE] = "esa"
        result_dict[PLUGINTYPE] = self.log_name
        collectd.info("Plugin ESALogs: Added common parameters successfully")

    @staticmethod
    def dispatch_data(result_dict):
        collectd.debug("Plugin ESALogs: Values dispatched = " + json.dumps(result_dict))
        dispatch(result_dict)

    def read(self):
        self.previousLogName.append(self.log_name)
        # collect data
        result_dict = self.collect_log()
        if not result_dict:
            collectd.error("Plugin ESA Logs download: Unable to fetch information of ESA Logs")
            return

        self.add_common_params(result_dict)
        # dispatch data to collectd
        self.dispatch_data(result_dict)

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))

obj = ESALogs()
collectd.register_config(obj.read_config)
collectd.register_read(obj.read_temp)
