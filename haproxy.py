""" A collectd-python plugin for retrieving
    metrics from haproxy server.
"""
import json
import time
import collectd
import subprocess
import re
import traceback
import signal
from collections import defaultdict
from copy import deepcopy

# user imports
import utils
from constants import *

HAPROXY_DOCS = ["frontendStats", "backendStats"]

class haproxyStats(object):
    """Plugin object will be created only once and collects utils
           and available CPU/RAM usage info every interval."""

    def __init__(self, interval=1, utilize_type="CPU", maximum_grep=5, process_name='*'):
        """Initializes interval."""
        self.interval = DEFAULT_INTERVAL
        self.documentsTypes = []

    def config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == DOCUMENTSTYPES:
                self.documentsTypes = children.values[0]

    def get_keys(self, key_mapping, lines):
        key_buf = lines[0].strip("# \n").split(",")[2:]
        for key in key_buf:
            if key:
                key_mapping.append(key)

    def format_stats(self, dict_stats, lines):
        for line in lines:
            line = line.strip("\n")
            if line != b'':
                # print line
                if line[0] == "#":
                    continue

                buf = line.split(",")
                name = buf[0] + buf[1]
                dict_stats[name] = buf[:-1]

    def get_frontend_data(self, key_mapping, dict_stats, haproxy_data):
        try:
            metrics_tmp = {}
            for index in dict_stats.keys():
                if dict_stats[index][1] == "FRONTEND" and dict_stats[index][0] == "http":
                    i = 2
                    for key in key_mapping:
                        name = dict_stats[index][0] + "_" + key
                        metrics_tmp[name] = dict_stats[index][i]
                        i += 1

            if metrics_tmp:
                haproxy_data['frontendStats']['httpReqRate'] = metrics_tmp['http_req_rate']
                haproxy_data['frontendStats']['httpRate'] = metrics_tmp['http_rate']
                haproxy_data['frontendStats']['httpEreq'] = metrics_tmp['http_ereq']
                haproxy_data['frontendStats']['httpDreq'] = metrics_tmp['http_dreq']
                haproxy_data['frontendStats']['httpHrsp_4xx'] = metrics_tmp['http_hrsp_4xx']
                haproxy_data['frontendStats']['httpHrsp_5xx'] = metrics_tmp['http_hrsp_5xx']
                haproxy_data['frontendStats']['httpBin'] = metrics_tmp['http_bin']
                haproxy_data['frontendStats']['httpBout'] = metrics_tmp['http_bout']
                haproxy_data['frontendStats']['httpSutil'] = (float(metrics_tmp['http_scur']) / float(metrics_tmp['http_slim'])) * 100
                haproxy_data['frontendStats']['_documentType'] = 'frontendStats'

            else:
                collectd.error("Plugin haproxy: Error collecting frontend stats for http proxy")
                return

            self.add_common_params(haproxy_data['frontendStats'], 'frontendStats')

        except Exception as err:
            collectd.error("Plugin haproxy: Exception in get_frontend_data due to %s" % err)
            return

    def get_backend_data(self, key_mapping, dict_stats, haproxy_data):
        try:
            metrics_tmp = {}
            for index in dict_stats.keys():
                if dict_stats[index][1] == "BACKEND" and dict_stats[index][0] == "http":
                    i = 2
                    for key in key_mapping:
                        name = dict_stats[index][0] + "_" + key
                        metrics_tmp[name] = dict_stats[index][i]
                        i += 1

            if metrics_tmp:
                haproxy_data['backendStats']['httpRtime'] = metrics_tmp['http_rtime']
                haproxy_data['backendStats']['httpEcon'] = metrics_tmp['http_econ']
                haproxy_data['backendStats']['httpDresp'] = metrics_tmp['http_dresp']
                haproxy_data['backendStats']['httpEresp'] = metrics_tmp['http_eresp']
                haproxy_data['backendStats']['httpQcur'] = metrics_tmp['http_qcur']
                haproxy_data['backendStats']['httpQtime'] = metrics_tmp['http_qtime']
                haproxy_data['backendStats']['httpWredis'] = metrics_tmp['http_wredis']
                haproxy_data['backendStats']['httpWretr'] = metrics_tmp['http_wretr']
                haproxy_data['backendStats']['_documentType'] = 'backendStats'

            else:
                collectd.error("Plugin haproxy: Error collecting backend stats for http proxy")
                return

            self.add_common_params(haproxy_data['backendStats'], 'backendStats')

        except Exception as err:
            collectd.error("Plugin haproxy: Exception in get_backend_data due to %s" % err)

    def collect_haproxy_data(self):
        try:
            dict_stats = defaultdict(list)
            key_mapping = []
            haproxy_data = defaultdict(dict)

            cmnd = "echo 'show stat' | nc -U /var/lib/haproxy/stats"
            process = subprocess.Popen(cmnd, shell=True, stdout=subprocess.PIPE)
            lines = process.stdout.readlines()

            self.get_keys(key_mapping, lines)
            self.format_stats(dict_stats, lines)

            self.get_frontend_data(key_mapping, dict_stats, haproxy_data)
            self.get_backend_data(key_mapping, dict_stats, haproxy_data)

            return haproxy_data

        except Exception as err:
            collectd.error("Plugin haproxy: Exception in collect_haproxy_data due to %s" % err)

    def add_common_params(self, dict_stats, doc):
        """Adds TIMESTAMP, PLUGIN, PLUGITYPE to dictionary."""
        timestamp = int(round(time.time()))
        dict_stats[TIMESTAMP] = timestamp
        dict_stats[PLUGIN] = HAPROXY
        dict_stats[PLUGINTYPE] = doc
        dict_stats[ACTUALPLUGINTYPE] = HAPROXY
        # dict_jmx[PLUGIN_INS] = doc
        collectd.info("Plugin haproxy: Added common parameters successfully for %s doctype" % doc)

    def read(self):
        """Collects all data."""
        try:
            haproxy_stats = self.collect_haproxy_data()

            if not haproxy_stats:
                collectd.error("Plugin haproxy: Unable to fetch data for haproxy.")
                return
            else:
                for document in haproxy_stats.keys():
                    if haproxy_stats[document]['_documentType'] not in self.documentsTypes:
                        del haproxy_stats[document]

            self.dispatch_data(deepcopy(haproxy_stats))

        except Exception as err:
            collectd.error("Plugin haproxy: Couldn't read and gather the metrics due to the exception %s in %s" % (err, traceback.format_exc()))

    def dispatch_data(self, result):
        """Dispatch data to collectd."""
        for details_type, details in result.items():
            collectd.info("Plugin haproxy: Succesfully sent %s doctype to collectd." % details_type)
            collectd.info("Plugin haproxy: Values dispatched =%s" % json.dumps(details))

            utils.dispatch(details)

    def read_temp(self):
        """Collectd first calls register_read. At that time default interval is taken,
        hence temporary function is made to call, the read callback is unregistered
        and read() is called again with interval obtained from conf by register_config callback."""
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


def init():
    """When new process is formed, action to SIGCHLD is reset to default behavior."""
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)


OBJ = haproxyStats()
collectd.register_init(init)
collectd.register_config(OBJ.config)
collectd.register_read(OBJ.read_temp)
                                            
