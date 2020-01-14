"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
"""
Collectd Python plugin to get nodejsapi
"""


import collectd
import ast
import json
import time
import copy
import requests
from utils import *

LONG_REQUEST_STATS = "long_requests_stats"
ERROR_STATS = "error_stats"
API_STATS = "api_stats"

docs = [LONG_REQUEST_STATS, ERROR_STATS, API_STATS]

class Nodejsapi():

    def __init__(self):
        self.host = None
        self.hosts = []
        self.conn = None
        self.cur = None
        self.status = {}
        self.port = 8080
        self.first_poll = {}
        self.interval = None
        self.version = None
        self.previous_data = dict()
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
        timestamp = int(round(time.time()))
        result_dict[PLUGIN] = "nodejsapi"
        result_dict[HOSTNAME] = hostname
        result_dict[TIMESTAMP] = timestamp
        result_dict[PLUGINTYPE] = doc_type
        result_dict[ACTUALPLUGINTYPE] = "nodejsapi"
        collectd.info("Plugin nodejsapi: Added common parameters successfully")


    @staticmethod
    def dispatch_data(result_dict):
        dispatch(result_dict)

    def poll(self):
        node_stats = dict()
        try:
            endTime = int(time.time() * 1000.0) #swagger-stats time is represented in nano seconds
            startTime = endTime - (int(self.interval)*1000)
            error_response = False

            url_last_errors = "http://localhost:{}/swagger-stats/stats?fields=lasterrors".format(self.port)
            response = requests.get(url_last_errors)
            error_stats = list()
            if response.status_code == 200:
                collectd.info('Plugin nodejsapi: Response code 200 received for lasterrors')
                content = response.content
                #response_json = json.loads(content)
                response_json = ast.literal_eval(content)
                swagger_error_stats = response_json.get("lasterrors")
                for error in swagger_error_stats:
                    if (error.get("startts") <= endTime and error.get("startts") >= startTime):
                        error_stat = {}
                        error_stat["path"] = error.get("path")
                        error_stat["method"] = error.get("method")
                        error_stat["error_code"] = ((error.get("http")).get("response")).get("code")
                        error_stats.append(error_stat)
            else:
                error_response = True
            node_stats[ERROR_STATS] = error_stats

            url_long_request = "http://localhost:{}/swagger-stats/stats?fields=longestreq".format(self.port)
            response = requests.get(url_long_request)
            long_req_stats = list()
            if response.status_code == 200:
                collectd.info('Plugin nodejsapi: Response code 200 received for longestreq')
                content = response.content
                #response_json = json.loads(content)
                response_json = ast.literal_eval(content)
                swagger_long_req_stats = response_json.get("longestreq")
                for long_req in swagger_long_req_stats:
                    if (long_req.get("startts") <= endTime and long_req.get("startts") >= startTime):
                        long_req_stat = {}
                        long_req_stat["path"] = long_req.get("path")
                        long_req_stat["method"] = long_req.get("method")
                        long_req_stat["responsetime"] = long_req.get("responsetime")
                        long_req_stats.append(long_req_stat)
            else:
                error_response = True
            node_stats[LONG_REQUEST_STATS] = long_req_stats

            url_api_request = "http://localhost:{}/swagger-stats/stats?fields=apistats".format(self.port)
            response = requests.get(url_api_request)
            final_json_to_be_dispatched = list()
            if response.status_code == 200:
                collectd.info('Plugin nodejsapi: Response code 200 received for apistats')
                content = response.content
                #response_json = json.loads(content)
                response_json = ast.literal_eval(content)
                swagger_api_stats = response_json.get("apistats")
                api_req_stats = dict()
                for path_key in swagger_api_stats.keys():
                    path = path_key
                    method_info = swagger_api_stats[path]
                    method_level_info = dict()
                    for method in method_info.keys():
                        req_method = method
                        req_method_details = method_info[method]
                        api_req = {}
                        api_req["requests"] = req_method_details.get("requests")
                        api_req["responses"] = req_method_details.get("responses")
                        api_req["redirect"] = req_method_details.get("redirect")
                        api_req["total_time"] = req_method_details.get("total_time")
                        api_req["success"] = req_method_details.get("success")
                        api_req["errors"] = req_method_details.get("errors")
                        api_req["total_req_clength"] = req_method_details.get("total_req_clength")
                        api_req["total_res_clength"] = req_method_details.get("total_res_clength")
                        method_level_info[req_method] = api_req
                    api_req_stats[path] = method_level_info

                if self.previous_data:
                    for key_path in api_req_stats.keys():
                        api_stats = dict()
                        if key_path in self.previous_data.keys():
                            method_info = api_req_stats[key_path]
                            for method, method_details in method_info.items():
                                if method in self.previous_data[key_path].keys():
                                    api_stats["requests"] = method_details.get("requests") - self.previous_data[key_path][method]["requests"]
                                    api_stats["responses"] = method_details.get("responses") - self.previous_data[key_path][method]["responses"]
                                    api_stats["redirect"] = method_details.get("redirect") - self.previous_data[key_path][method]["redirect"]
                                    api_stats["total_time"] = method_details.get("total_time") - self.previous_data[key_path][method]["total_time"]
                                    api_stats["success"] = method_details.get("success") - self.previous_data[key_path][method]["success"]
                                    api_stats["errors"] = method_details.get("errors") - self.previous_data[key_path][method]["errors"]
                                    api_stats["total_req_clength"] = method_details.get("total_req_clength") - self.previous_data[key_path][method]["total_req_clength"]
                                    api_stats["method"] = method
                                    api_stats["path"] = key_path
                                else:
                                    api_stats = copy.deepcopy(method_details)
                                    api_stats["method"] = method
                                    api_stats["path"] = key_path
                                if int(api_stats["requests"]) != 0:
                                    final_json_to_be_dispatched.append(api_stats)

                        else:
                            method_info = api_req_stats[key_path]
                            for method, method_details in method_info.items():
                                api_stats = copy.deepcopy(method_details)
                                api_stats["method"] = method
                                api_stats["path"] = key_path
                            final_json_to_be_dispatched.append(api_stats)


                self.previous_data = copy.deepcopy(api_req_stats)

            else:
                error_response = True
            node_stats[API_STATS] = final_json_to_be_dispatched

            #send empty value if no data present for given time interval
            if not error_response and len(error_stats) == 0 and len(long_req_stats) == 0 and len(final_json_to_be_dispatched) == 0:
                long_req_stats = list()
                long_req_stat = {}
                long_req_stat["path"] = None
                long_req_stat["method"] = None
                long_req_stat["responsetime"] = None
                long_req_stats.append(long_req_stat)
                node_stats[LONG_REQUEST_STATS] = long_req_stats


        except Exception as ex:
            collectd.error('Error collecting nodejsapi application stats : %s ' % ex.message)
        return node_stats

    def read(self):
        result = self.poll()
        if not result:
            collectd.error("Plugin nodejsapi: Unable to fetch information ")
        else:
            collectd.info("Plugin nodejsapi: Success fetching information")
            for node_stat in result.keys():
                doc = node_stat
                for stats in result[node_stat]:
                    self.add_common_params(stats, doc)
                    self.dispatch_data(stats)

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))

obj = Nodejsapi()
collectd.register_config(obj.read_config)
collectd.register_read(obj.read_temp)
