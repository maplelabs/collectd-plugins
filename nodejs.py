"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
"""
Collectd Python plugin to get nodejs
"""

import collectd
import ast
import json
import time
import requests
from utils import *

HTTP_STATS = "httpstats"
SYS_DETAILS = "systemstats"

docs = [HTTP_STATS, SYS_DETAILS]


class Nodejs():

    def __init__(self):
        self.host = None
        self.hosts = []
        self.conn = None
        self.cur = None
        self.status = {}
        self.previousData = {}
        self.port = 8080
        self.first_poll = {}
        self.interval = None
        self.version = None
        self.last_read_bucket_index = None
        # Instantiating the status variables for all the hosts during init

    def read_config(self, cfg):
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == PORT:
                self.port = children.values[0]

    @staticmethod
    def add_common_params(result_dict, doc_type):
        hostname = gethostname()
        timestamp = int(round(time.time() * 1000))
        result_dict[PLUGIN] = "nodejs"
        result_dict[HOSTNAME] = hostname
        result_dict[TIMESTAMP] = timestamp
        result_dict[PLUGINTYPE] = doc_type
        result_dict["_documentType"] = doc_type

        result_dict[ACTUALPLUGINTYPE] = "nodejs"
        collectd.info("Plugin nodejs: Added common parameters successfully")

    @staticmethod
    def dispatch_data(result_dict):
        collectd.info("Plugin nodejs: Values dispatched = " + json.dumps(result_dict))
        dispatch(result_dict)

    def poll(self):
        node_stats = dict()
        last_read_bucket_index = self.last_read_bucket_index
        try:
            # node_stats = self.get_node_http_stats()
            url = "http://localhost:{}/swagger-stats/stats?fields=timeline".format(self.port)
            collectd.info("Request URL")
            response = requests.get(url)
            if response.status_code == 200:
                collectd.info('Response code 200 received')
                content = response.content
                #response_json = json.loads(content)
                response_json = ast.literal_eval(content)
                timeline_stats = response_json.get("timeline")
                bucket_current = timeline_stats.get("settings").get("bucket_current")

                if not last_read_bucket_index and bucket_current:
                    last_read_bucket_index = bucket_current - 1
                else:
                    last_read_bucket_index += 1

                if last_read_bucket_index :
                    if(timeline_stats.get("data").get(str(last_read_bucket_index))):
                        last_value = timeline_stats.get("data").get(str(last_read_bucket_index))

                        http_stats = last_value.get("stats")
                        node_stats[HTTP_STATS] = http_stats

                        sys_stats = last_value.get("sys")
                        node_stats[SYS_DETAILS] = sys_stats


        except Exception as ex:
            collectd.error('Error collecting nodejs application stats : %s ' % ex.message)
        return node_stats


    def read(self):
        result_dicts = self.poll()
        if not result_dicts:
            collectd.error("Plugin nodejs: Unable to fetch information ")
        else:
            collectd.info("Plugin nodejs: Success fetching information")
            for doc in docs:
                result_dict = result_dicts[doc]
                self.add_common_params(result_dict, doc)
                # dispatch data to collectd
                self.dispatch_data(result_dict)


    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


def init(self):
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)


obj = Nodejs()
collectd.register_config(obj.read_config)
collectd.register_read(obj.read_temp)
