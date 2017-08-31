""" A collectd-python plugin for retrieving
    metrics from Apache status module. """

import collectd
import json
import time
import requests
from requests.packages.urllib3 import Retry

# user imports
from constants import *
from utils import *

key_map = {
    "ServerVersion": "apacheVersion",
    "ServerMPM": "serverMPM",
    "CurrentTime": "currentTime",
    "RestartTime": "resetTime",
    "ServerUptimeSeconds": "upTime",
    "CPULoad": "CPULoad",
    "CPUSystem": "CPUSystem",
    "CPUUser": "CPUUser",
    "Total Accesses": "totalAccessCount",
    "ReqPerSec": "reqPerSec",
    "BytesPerSec": "bytesPerSec",
    "BytesPerReq": "bytesPerReq",
    "Total kBytes": "totalAccessSize",
    "BusyWorkers": "activeWorkers",
    "IdleWorkers": "idleWorkers"
}


class ApachePerf:
    def __init__(self):
        self.interval = DEFAULT_INTERVAL
        self.port = 80
        self.location = DEFAULT_LOCATION
        self.secure = False

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
            if("true" == secure.lower()):
                port = 443
                url = "https://localhost:{}/{}?auto".format(port, self.location)
            else:
                url = "http://localhost:{}/{}?auto".format(port, self.location)
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
            else:
                collectd.info("Couldn't get the data")
                return
            for key, value in result.items():
                try:
                    result[key] = float(value)
                except ValueError:
                    continue
            try:
                t_str = result["apacheVersion"]
                t_str = [j.strip() for i in t_str.split("(") for j in i.split(")")]
                result["apacheVersion"] = t_str[0]
                result["apacheOS"] = t_str[1]
            except KeyError:
                result["apacheVersion"] = None
                result["apacheOS"] = None

            result["idleWorkers"] = int(result["idleWorkers"])
            result["activeWorkers"] = int(result["activeWorkers"])
            result["totalWorkers"] = int(result["idleWorkers"]) + int(result["activeWorkers"])
            session.close()
        except requests.exceptions.RequestException as e:
            collectd.error("Plugin apache_perf : Couldn't connect to apache server")
            return
        return result

    @staticmethod
    def add_common_params(result_dict):
        hostname = gethostname()
        timestamp = time.time()
        result_dict[PLUGIN] = "apache"
        result_dict[HOSTNAME] = hostname
        result_dict[TIMESTAMP] = timestamp
        result_dict[PLUGINTYPE] = APACHE_PERF
        collectd.info("Plugin apache_perf: Added common parameters successfully")

    @staticmethod
    def dispatch_data(result_dict):
        collectd.debug("Plugin apache_perf: Values dispatched = " + json.dumps(result_dict))
        dispatch(result_dict)

    def read(self):
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
