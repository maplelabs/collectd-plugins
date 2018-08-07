#!/usr/bin/env python
""" A python plugin for downloading log file
     from ESA cluster. """

import urllib
import json
import time
import os
import sys
import multiprocessing
import ast
import subprocess

HOST_CONFIG_FILE = '/opt/esa_conf.json'
ESA_INTERVAL = 60
DOWNLOAD_PATH = '/var/log/td-agent/esa_logs/'
DUMMY_PATH = '/tmp/'
LOG_NAME_MAPPING = {"authentication": "authentication",
                    "gui_logs": "gui",
                    "mail_logs": "mail",
                    "system_logs": "system"}

class ESALogs:
    def __init__(self, log_names):
        self.interval = ESA_INTERVAL
        self.host = None
        self.user = None
        self.password = None
        self.log_names = log_names
        self.download_path = DOWNLOAD_PATH
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
        if os.path.exists(HOST_CONFIG_FILE):
            with open(HOST_CONFIG_FILE) as data_file:
                data = json.load(data_file)
            self.password = data['password']
            self.hosts = data['hosts']
            self.user = data['user']

    def checkLogDest(self, log_path=None):
        try:
            if not log_path:
                log_path = self.download_path
            if not os.path.exists(log_path):
                os.makedirs(log_path)
        except Exception as err:
            print("Error in creating esa log folder %s" %(str(err)))
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
            print("Error in copying the log file due to %s" %(str(err)))
            return False

    def download_log(self, host_ip, host_name, host_uuid, log_name):
        data_dict = {}
        data_dict['host'] = host_name
        data_dict['log_name'] = log_name
        data_dict['host_ip'] = host_ip
        log_path = self.download_path + host_name + "_" + host_uuid + "/"
        tmp_file = DUMMY_PATH+log_name+"_"+host_ip
        try:
            if self.checkLogDest(log_path) and log_name:
                esa_url = 'ftp://{0}:{1}@{2}/{3}/{4}.current'.format(self.user, self.password, host_ip, log_name, LOG_NAME_MAPPING[log_name])
                urllib.urlretrieve(esa_url, tmp_file)
                #urllib.urlretrieve('ftp://admin:ironport@10.11.100.82/'+ self.log_name +'/'+ log_name_mapping[self.log_name] +'.current', '/var/log/esa_logs/'+ self.log_name+'.txt')
                urllib.urlcleanup()
                if self.fileCompare(esa_log_file = log_path+log_name.replace('_',''), dummy_file=tmp_file):
                    data_dict["status"] = "success"
                    print("Downloaded Log files from ESA cluster %s " %(str(host_name)))
            else:
                print("Couldn't store the data")
        except Exception as e:
            print("Error in downloading log files from the ESA cluster %s due to %s" % (str(host_name), str(e)))
            data_dict["status"] = "Failed"
            data_dict["message"] = e
        print(data_dict)

    def start_download(self, host_detail):
        host_ip = host_detail.get('ip', None)
        host_name = host_detail.get('name', host_ip)
        host_uuid = host_detail.get('uuid', None)
        if host_ip:
            for log_name in self.log_names:
                download_process = multiprocessing.Process(target=self.download_log, args=(host_ip, host_name, host_uuid, log_name))
                download_process.start()
        else:
            print("Plugin ESA Logs download: Unable to fetch information of ESA clusters")

    def start(self):
        # Get the host details from the esa_conf.json file
        self.read_host_config()
        for host in self.hosts:
            process = multiprocessing.Process(target=self.start_download, args=(host,))
            process.start()

log_names = ast.literal_eval(sys.argv[-1])
obj = ESALogs(log_names)
obj.start()
