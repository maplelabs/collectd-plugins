""" A collectd-python plugin for downloading log file 
     from ESA cluster. """

import collectd
import urllib
import json
import time
import os
import multiprocessing
import ast

# user imports
from constants import *
from utils import *

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
        self.log_names = []
        self.download_path = ESA.DOWNLOAD_PATH
        self.previousLogName = []

    def read_host_config(self):
        """
        Get the details of ESA clusters from the json file
        File format:
            {
                "password": "pswd",
                "hosts": [
                    {
                        "name": "host_name",
                        "ip": "host_ip",
                        "uuid": "host_uuid"
                    }
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
                self.log_names = children.values[0]
                self.log_names = ast.literal_eval(self.log_names)
                self.dummy_path = ESA.DUMMY_PATH
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

    def download_log(self, host_ip, host_name, host_uuid, log_name):
        data_dict = {}
        download_status = True
        log_path = self.download_path + host_name + "_" + host_uuid + "/"
        tmp_file = self.dummy_path+log_name+"_"+host_ip
        try:
            if self.checkLogDest(log_path) and log_name:
                esa_url = 'ftp://{0}:{1}@{2}/{3}/{4}.current'.format(self.user, self.password, host_ip, log_name, log_name_mapping[log_name])
                urllib.urlretrieve(esa_url, tmp_file)
                #urllib.urlretrieve('ftp://admin:ironport@10.11.100.82/'+ self.log_name +'/'+ log_name_mapping[self.log_name] +'.current', '/var/log/esa_logs/'+ self.log_name+'.txt')
                urllib.urlcleanup()
                if not self.fileCompare(esa_log_file = log_path+log_name.replace('_',''), dummy_file=tmp_file):
                    download_status = False
                else:
                    data_dict["status"] = "success"
                    collectd.info("Downloaded Log files from ESA cluster %s %s" %(str(host_name), str(data_dict)))
            else:
                collectd.info("Couldn't store the data")
                download_status = False
        except Exception as e:
            collectd.error("Error in downloading log files from the ESA cluster %s due to %s" % (str(host_name), str(e)))
            data_dict["status"] = "Failed"
            data_dict["message"] = e
        if download_status:
            self.add_common_params(data_dict, host_ip)
            # dispatch data to collectd
            self.dispatch_data(data_dict)

    def start_download(self, host_detail):
        host_ip = host_detail.get('ip', None)
        host_name = host_detail.get('name', host_ip)
        host_uuid = host_detail.get('uuid', None)
        if host_ip:
            for log_name in self.log_names:
                download_process = multiprocessing.Process(target=self.download_log, args=(host_ip, host_name, host_uuid, log_name))
                download_process.start()
        else:
            collected.error("Host Ip is absent...!!!")
            collectd.error("Plugin ESA Logs download: Unable to fetch information of ESA Logs for the host %s" %(str(host)))

    @staticmethod
    def add_common_params(result_dict, esa_host):
        timestamp = int(round(time.time()))
        result_dict[HOSTNAME] = esa_host
        result_dict[TIMESTAMP] = timestamp
        result_dict[PLUGIN] = "esalogstore"
        result_dict[ACTUALPLUGINTYPE] = "esalogstore"
        result_dict[PLUGINTYPE] = "esalogstore"
        collectd.info("Plugin ESALogs: Added common parameters successfully")

    @staticmethod
    def dispatch_data(result_dict):
        collectd.info("Plugin ESALogs: Values dispatched %s" %(str(result_dict)))
        dispatch(result_dict)

    def read(self):
        #self.previousLogName.append(self.log_name)
        # collect data
        for host in self.hosts:
            process = multiprocessing.Process(target=self.start_download, args=(host,))
            process.start()

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))

obj = ESALogs()
collectd.register_config(obj.read_config)
collectd.register_read(obj.read_temp)
