""" A collectd-python plugin for retrieving
    metrics from nginx status module. """

import collectd
import re
import time
import requests
import platform
import json
from constants import *
from utils import *

SERVER_STATS = "serverStats"
SERVER_DETAILS = "serverDetails"

docs = [SERVER_STATS, SERVER_DETAILS]
#docs = ["nginxDetails"]

class Nginx(object):
    def __init__(self):
        self.interval = DEFAULT_INTERVAL
        self.port = 80
        self.location = DEFAULT_LOCATION
        self.host = 'localhost'
        self.secure = False
        self.pollCounter = 0
        self.previousData = {'previousTotal':0}

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

    def get_server_details(self):
        # Get the version of the NGINX server running
        server_details = {}
        try:
            url='http://'+self.host+'/api/3/nginx'
            resp = requests.get(url, verify=False)
            if resp and resp.status_code == 200:
                content = json.loads(resp.content)
                server_details['nginxVersion'] = content['version']
                server_details['nginxBuild'] = content['build']
            return server_details
        except Exception as ex:
            raise ex
        # Get the status of the NGINX server
        running = False
        try:
            stdout, stderr = get_cmd_output("ps -ef | grep nginx")
            if stdout:
                tokens = str(stdout).strip().split('\n')
                for token in tokens:
                   attributes =  token.strip().split()
                   if len(attributes) >= 8:
                       if 'nginx' in attributes[7]:
                           running = True
                           break
        except Exception as ex:
            raise ex
        # Get the uptime of the NGINX server
        try:
            uptime_v = 0
            t =0
            stdout, stder = get_cmd_output('ps -eo comm,etime,user | grep nginx | grep root')
            #for val in stdout.split():
            #    print(val)
            data = re.findall('([0-9\:][^-].[^\s]*)', stdout)
            if data:
                for u in data[0].split(':'):
                    t = 60 * t + int(u)
                uptime_v = t #uptime value in 'seconds'
        except Exception as err:
            raise err
        server_details.update({'processRunning': running, 'upTime': uptime_v})
        return server_details


    def get_server_stats(self):
        data_dict={}
        try:
            url='http://'+self.host+'/api/3/'
            con_url='connections'
            resp = requests.get(url+con_url, verify=False)
            if resp and resp.status_code == 200:
                content = json.loads(resp.content)
                data_dict['activeConnections'] = content['active']
                data_dict['acceptedConnections'] = content['accepted']
                data_dict['activeWaiting'] = content['idle']
                data_dict['droppedConnections'] = content['dropped']
            con_url='http/requests'
            resp = requests.get(url+con_url, verify=False)
            if resp and resp.status_code == 200:
                content = json.loads(resp.content)
                data_dict['currentRequests'] = content['current']
                data_dict['totalRequests'] = content['total']
                data_dict["requestsPerSecond"] = round((content['total'] - self.previousData['previousTotal'])/ float(self.interval), 2)
                self.previousData['previousTotal'] = content['total']
            con_url='ssl'
            resp = requests.get(url+con_url, verify=False)
            if resp and resp.status_code == 200:
                content = json.loads(resp.content)
                data_dict['handshakes'] = content['handshakes']
                data_dict['handshakesFailed'] = content['handshakes_failed']
                data_dict["sessionReuses"] = content['session_reuses']
            con_url='processes'
            resp = requests.get(url+con_url, verify=False)
            if resp and resp.status_code == 200:
                content = json.loads(resp.content)
                data_dict['processRespawned'] = content['respawned']
            return data_dict
        except Exception as ex:
            raise ex


    def poll(self, type):
        
        nginx_doc = {}
        
        if type == SERVER_STATS:
            server_stats = None
            try:
                server_stats = self.get_server_stats()
            except Exception as ex:
                err = 'Error collecting nginx server stats : {0}'.format(ex.message)
                collectd.error(err)
            if server_stats:
                nginx_doc = server_stats
                collectd.info('nginx doc updated with server stats')
        else:
            server_details = None
            try:
                server_details = self.get_server_details()
            except Exception as ex:
                err = 'Error collecting nginx server details : {0}'.format(ex.message)
                collectd.error(err)
            if server_details:
                nginx_doc = server_details
                collectd.info('nginx doc updated with server details')
        return nginx_doc

    @staticmethod
    def add_common_params(result_dict, doc_type):
        hostname = gethostname()
        timestamp = int(round(time.time()))
        result_dict[PLUGIN] = "nginxplus"
        result_dict[HOSTNAME] = hostname
        result_dict[TIMESTAMP] = timestamp
        result_dict[PLUGINTYPE] = doc_type
        result_dict[ACTUALPLUGINTYPE] = "nginxplus"
        collectd.info("Plugin nginx: Added common parameters successfully")

    @staticmethod
    def dispatch_data(result_dict):
        collectd.info("Plugin nginx: Values dispatched = " + json.dumps(result_dict))
        dispatch(result_dict)

    def read(self):
        self.pollCounter += 1
        # collect data
        for doc in docs:
            result_dict = self.poll(doc)
            #result_dict = self.nginxplusstats()
            if not result_dict:
                collectd.error("Plugin nginx: Unable to fetch information of nginx for document Type: " + doc)
            else:
                collectd.info("Plugin nginx:Success fetching information of nginx for document Type: " + doc)
                self.add_common_params(result_dict, doc)
                # dispatch data to collectd
                self.dispatch_data(result_dict)

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


obj = Nginx()
collectd.register_config(obj.read_config)
collectd.register_read(obj.read_temp)
