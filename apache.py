"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
""" A collectd-python plugin for retrieving
    metrics from Apache status module. """
# Apache 2.4.18 metric comapatibility although it is forward and backward compatible

import collectd
import json
import time
import requests
import subprocess
from requests.packages.urllib3 import Retry

# user imports
from constants import *
from utils import *

key_map = {
    "ServerVersion": "apacheVersion",
    "ServerMPM": "apacheServerMPM",
    "CurrentTime": "apacheServerTime",
    "RestartTime": "resetTime",
    "Uptime": "upTime",
    "CPULoad": "CPULoad",
    "CPUSystem": "CPUSystem",
    "CPUUser": "CPUUser",
    "Total Accesses": "accessCount",
    "ReqPerSec": "requestsPerSecond",
    "BytesPerSec": "bytesPerSecond",
    "BytesPerReq": "bytesPerRequest",
    "Total kBytes": "accessSize",
    "BusyWorkers": "activeWorkers",
    "IdleWorkers": "idleWorkers"
}


class ApachePerf:
    def __init__(self):
        self.interval = DEFAULT_INTERVAL
        self.port = 80
        self.location = DEFAULT_LOCATION
        self.secure = False
        self.pollCounter = 0
        self.previousData = {}

    def read_config(self, cfg):
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == LOCATION:
                self.location = children.values[0]
            if children.key == PORT:
                self.port = children.values[0]
            if children.key == SECURE:
                self.secure = children.values[0]

    def poll(self):
        global key_map
        port = self.port
        secure = self.secure
        try:
            if (secure.lower() == "true"):
                url = "https://localhost:{}/{}?auto".format(port, self.location)
            else:
                url = "http://localhost:{}/{}?auto".format(port, self.location)
            collectd.info("Constructed URL for apache monitoring :" + str(url))
            session = requests.Session()
            session.mount('http://', requests.adapters.HTTPAdapter(max_retries=Retry(3)))
            session.mount('https://', requests.adapters.HTTPAdapter(max_retries=Retry(3)))
            session.verify = False
            response = session.get(url, timeout=30)
            response.raise_for_status()
            data = response.text.split("\n")
            if data:
                result = dict([(key_map[str((i.split(":", 1)[0]).strip())], str((i.split(":", 1)[1]).strip()))
                               for i in data if (i.split(":", 1)[0]).strip() in key_map.keys()])
                OScheck = subprocess.check_output(["lsb_release", "-d"])
                for line in OScheck.splitlines():
                    if "Ubuntu" in line:
                        serverDetails = subprocess.check_output(["apache2", "-v"]).split()
                    elif ("CentOS" in line):
                        serverDetails = subprocess.check_output(["httpd", "-v"]).split()
            else:
                collectd.info("Couldn't get the data")
                return

            data_dict = {}
            for key, value in result.items():
                try:
                    if key == 'apacheVersion':
                        continue

                    if(key == 'accessCount' and self.pollCounter <= 1):
                        self.previousData['accessCount'] = float(value)
                        continue
                    elif(key == 'accessCount' and not self.pollCounter <= 1):
                        data_dict[key] = float(value) - float(self.previousData['accessCount'])
                        self.previousData['accessCount'] = float(value)
                        continue

                    if(key == 'accessSize' and self.pollCounter <= 1):
                        self.previousData['accessSize'] = float(value)/1024
                        continue
                    elif(key == 'accessSize' and not self.pollCounter <= 1):
                        data_dict[key] = round(float(value)/1024 - float(self.previousData['accessSize']), 2)
                        self.previousData['accessSize'] = float(value)/1024
                        continue

                    if(self.pollCounter <= 1 and key in["bytesPerSecond","requestsPerSecond","bytesPerRequest"]):
                        continue

                    if(key in ["apacheDomainName","apacheServerTime","apacheIPAddress","apacheTag"]):
                        continue

                    if(key == "apacheServerMPM"):
                        data_dict["serverMPM"] = value
                        continue

                    if (key == "upTime"):
                        data_dict["upTime"] = int(value)
                        continue

                    if(key in ["CPUSystem","CPULoad","CPUUser"]):
                        data_dict[key] = float(value)
                        continue

                    data_dict[key] = float(value)
                except ValueError:
                    continue
            try:
                data_dict["apacheVersion"] = serverDetails[2].strip('Apache/')
                data_dict["apacheOS"] = serverDetails[3][1:-1]
            except KeyError:
                data_dict["apacheVersion"] = None
                data_dict["apacheOS"] = None

            data_dict["idleWorkers"] = int(data_dict["idleWorkers"])
            data_dict["activeWorkers"] = int(data_dict["activeWorkers"])
            data_dict["totalWorkers"] = int(data_dict["idleWorkers"]) + int(data_dict["activeWorkers"])
            if (self.pollCounter > 1):
                data_dict["bytesPerSecond"] = result["bytesPerSecond"]
                data_dict["requestsPerSecond"] = result["requestsPerSecond"]
                data_dict["bytesPerRequest"] = result["bytesPerRequest"]
            session.close()
        except requests.exceptions.RequestException as e:
            collectd.error("Plugin apache_perf : Couldn't connect to apache server")
            return
        collectd.info("Collected data for apache metrics :" + str(data_dict))
        return data_dict

    @staticmethod
    def add_common_params(result_dict):
        hostname = gethostname()
        timestamp = int(round(time.time() * 1000))
        result_dict[PLUGIN] = "apache"
        result_dict[HOSTNAME] = hostname
        result_dict[TIMESTAMP] = timestamp
        result_dict[PLUGINTYPE] = APACHE
        result_dict[ACTUALPLUGINTYPE] = APACHE
        collectd.info("Plugin apache_perf: Added common parameters successfully")

    @staticmethod
    def dispatch_data(result_dict):
        collectd.debug("Plugin apache_perf: Values dispatched = " + json.dumps(result_dict))
        dispatch(result_dict)

    def read(self):
        self.pollCounter += 1
        # collect data
        result_dict = self.poll()
        if not result_dict:
            collectd.error("Plugin apache_perf: Unable to fetch information of apache")
            return

        self.add_common_params(result_dict)
        # dispatch data to collectd
        self.dispatch_data(result_dict)

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


obj = ApachePerf()
collectd.register_config(obj.read_config)
collectd.register_read(obj.read_temp)
