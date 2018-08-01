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
        self.host = None
        self.user = None
        self.password = None
        self.log_name = None
        self.download_path = ESA.DOWNLOAD_PATH
        self.previousLogName = []

    def read_host_config(self):
        """
        Get the details of ESA clusters from the json file
        File format:
            {
                "password": "pswd",
                "hosts": [
                    "host_1",
                    "1host_2"
                ],
                "user": "uname"
            }
        """
        if os.path.exists(ESA.HOST_CONFIG_FILE):
            with open(ESA.HOST_CONFIG_FILE) as data_file:
                data = json.load(data_file)
            self.password = data[PASSWORD]
            self.hosts = data['hosts']
            self.user = data[USER]

    def read_config(self, cfg):
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == "log_name":
                self.log_name = children.values[0]
                self.dummy_path = ESA.DUMMY_PATH +self.log_name
        self.read_host_config()

    def checkLogDest(self, log_path=None):
        try:
            if not log_path:
                log_path = self.download_path
            if not os.path.exists(log_path):
                os.makedirs(log_path)
        except Exception as err:
            collectd.error("Error in creating esa log folder %s" %(str(err)))
            return False
        return True

    @staticmethod
    def fileCompare(esa_log_file, dummy_file):
        try:
            f=open(esa_log_file, "a")
            line = subprocess.check_output(['tail', '-1', esa_log_file])
            line_num = 0
            with open(dummy_file) as f1:
                dwl_log_line = f1.readlines()
            for i in range(len(dwl_log_line)):
                if dwl_log_line[i] == line:
                    line_num = i+1
                    break
            with open(dummy_file) as f2:
                for i in xrange(line_num):
                    f2.next()
                for log_line in f2:
                    f.write(log_line)
            f.close()
            return True
        except Exception as err:
            collectd.error("Error in copying the log file due to %s" %(str(err)))
            return False

    def download_log(self, host_ip):
        data_dict = {}
        if host_ip:
            log_path = self.download_path + host_ip + "/"
            try:
                if self.checkLogDest(log_path) and self.log_name:
                    esa_url = 'ftp://{0}:{1}@{2}/{3}/{4}.current'.format(self.user, self.password, host_ip, self.log_name, log_name_mapping[self.log_name])
                    urllib.urlretrieve(esa_url, self.dummy_path+"_"+host_ip)
                    #urllib.urlretrieve('ftp://admin:ironport@10.11.100.82/'+ self.log_name +'/'+ log_name_mapping[self.log_name] +'.current', '/var/log/esa_logs/'+ self.log_name+'.txt')
                    urllib.urlcleanup()
                    if not self.fileCompare(esa_log_file = log_path+self.log_name, dummy_file=self.dummy_path+"_"+host_ip):
                        return
                else:
                    collectd.info("Couldn't store the data")
                    return
            except Exception as e:
                collectd.error("Error in downloading log files due to %s" % (str(e)))
                data_dict["status"] = "Failed"
                data_dict["message"] = e
                return data_dict
            data_dict["status"] = "success"
            collectd.info("Downloaded Log files from ESA cluster %s " %(str(data_dict)))
            return data_dict
        else:
            collectd.error("ESA Host details are absent...!!!")
            return

    @staticmethod
    def add_common_params(result_dict, host):
        timestamp = int(round(time.time()))
        result_dict[HOSTNAME] = host
        result_dict[TIMESTAMP] = timestamp
        result_dict[PLUGIN] = "esa"
        result_dict[ACTUALPLUGINTYPE] = "esa"
        result_dict[PLUGINTYPE] = "esa"
        collectd.info("Plugin ESALogs: Added common parameters successfully")

    @staticmethod
    def dispatch_data(result_dict):
        collectd.info("Plugin ESALogs: Values dispatched %s" %(str(result_dict)))
        dispatch(result_dict)

    def read(self):
        #self.previousLogName.append(self.log_name)
        # collect data
        for host in self.hosts:
            result_dict = self.download_log(host)
            if not result_dict:
                collectd.error("Plugin ESA Logs download: Unable to fetch information of ESA Logs for the host %s" %(str(host)))
                return

            self.add_common_params(result_dict, host)
            # dispatch data to collectd
            self.dispatch_data(result_dict)

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))

obj = ESALogs()
collectd.register_config(obj.read_config)
collectd.register_read(obj.read_temp)
