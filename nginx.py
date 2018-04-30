""" A collectd-python plugin for retrieving
    metrics from nginx status module. """

import collectd
import re
import time
import requests
import json
from constants import *
from utils import *

SERVER_STATS = "serverStats"
SERVER_DETAILS = "serverDetails"

docs = [SERVER_STATS, SERVER_DETAILS]

class Nginx(object):
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

    def get_version(self):
        try:
            stdout, version = get_cmd_output('nginx -v')
            tokens = re.split(' |/', str(version))
            for token in tokens:
                if re.match('[0-99].*', token):
                    if '\n' in token:
                        token = token.rstrip()
                    dbg = 'nginx version: {0}'.format(token)
                    collectd.debug(dbg)
                    return token
        except Exception as ex:
            raise ex

    def get_server_details(self):
        server_details = {}
        version = ''
        try:
            version = self.get_version()
            collectd.debug('nginx version retrieved successfully!')
        except Exception as ex:
            raise ex

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
        server_details.update({'serverVersion': version,
                               'processRunning': running})
        return server_details

    def get_server_stats(self, ip, port, location, secure):
        parameters = 'ip: {0}, port: {1}, location: {2}, secure: {3}'.format(ip, port, location, secure)
        collectd.debug(parameters)
        if secure == "false":
            url = 'http://{0}:{1}/{2}'.format(str(ip), str(port), location)
        else:
            url = 'https://{0}/{1}'.format(ip, location)
        collectd.debug('url: {0}'.format(url))
        active_con = None
        accepts = None
        handled = None
        waiting = None
        reading = None
        writing = None
        server_requests = None
        try:
            resp = requests.get(url, verify=False)
            if isinstance(resp, requests.models.Response):
                if resp.status_code == 200:
                    collectd.debug('Response code 200 received')
                    content = resp.content

                    tokens = content.split('\n')
                    
                    active_con = int(tokens[0].split(':')[1])

                    server = str(tokens[2].strip()).split(' ')
                    accepts = int(server[0])
                    handled = int(server[1])
                    server_requests = int(server[2])

                    operations = re.split(': | ', str(tokens[3].strip()))
                    reading = int(operations[1])
                    writing = int(operations[3])
                    waiting = int(operations[5])
                else:
                    err = 'Error collecting nginx stats : {0}'.format(resp.status_code)
                    collectd.error(err)
                    return None
            else:
                err = 'Error collecting nginx stats : {0}'.format(resp)
                collectd.error(err)
                return None
        except Exception as ex:
            raise ex

        handled_in_interval = None
        if handled:
            if not 'handled' in self.previousData:
                self.previousData.update({'handled': int(handled)})
                handled_in_interval = handled
            else: 
                handled_in_interval = int(handled) - int(self.previousData['handled'])
                if int(handled_in_interval) < 0:
                    handled_in_interval = handled
                self.previousData['handled'] = handled
        else:
            collectd.warning('No stats available for server_handled. Using stats from previous poll')
            handled_in_interval = self.previousData['handled']
        
        accepts_in_interval = None
        if accepts:
            if not 'accepts' in self.previousData:
                self.previousData.update({'accepts': accepts})
                accepts_in_interval = accepts
            else: 
                accepts_in_interval = accepts - self.previousData['accepts']
                if accepts_in_interval < 0:
                    accepts_in_interval = accepts
                self.previousData['accepts'] = accepts
        else:
            collectd.warning('No stats available for server_accepts. Using stats from previous poll.')
            accepts_in_interval = self.previousData['accepts']
        
        requests_in_interval = None
        if server_requests:
            if not 'requests' in self.previousData:
                self.previousData.update({'requests': server_requests})
                requests_in_interval = server_requests
            else:
                requests_in_interval = server_requests - self.previousData['requests']
                if requests_in_interval < 0:
                    requests_in_interval = server_requests
                self.previousData['requests'] = server_requests
        else:
            collectd.warning('No stats available for server_requests. Using stats from previous poll.')
            requests_in_interval = self.previousData['requests']

        data = {'activeConnections': active_con,
                'activeReading': reading,
                'activeWriting': writing,
                'activeWaiting': waiting,
                'acceptedConnections': accepts_in_interval,
                'handledConnections': handled_in_interval,
                'totalRequests': requests_in_interval
               }
        return data

    def poll(self, type):
        
        nginx_doc = {}
        
        if type == SERVER_STATS:
            server_stats = None
            try:
                server_stats = self.get_server_stats('127.0.0.1', self.port, self.location, self.secure)
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
        result_dict[PLUGIN] = "nginx"
        result_dict[HOSTNAME] = hostname
        result_dict[TIMESTAMP] = timestamp
        result_dict[PLUGINTYPE] = doc_type
        result_dict[ACTUALPLUGINTYPE] = "nginx"
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
