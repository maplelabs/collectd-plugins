import json
import time
import collectd
import subprocess
import re
import signal
import traceback
from collections import defaultdict
from copy import deepcopy

# user imports
import utils
from constants import *

HAPROXY_DOCS = ["frontendStats", "backendStats", "haproxyStats"]

class haproxyStats(object):
    """Plugin object will be created only once and collects utils
           and available CPU/RAM usage info every interval."""

    def __init__(self, interval=1, utilize_type="CPU", maximum_grep=5, process_name='*'):
        """Initializes interval."""
        self.interval = DEFAULT_INTERVAL
        self.documentsTypes = []
        self.pollCounter = 0
        self.prev_frontend_data = {}
        self.prev_backend_data = {}
        self.prev_haproxy_data = {}

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
                haproxy_data['frontendStats']['httpReqRate'] = float(metrics_tmp['http_req_rate'])
                haproxy_data['frontendStats']['httpRate'] = float(metrics_tmp['http_rate'])
                haproxy_data['frontendStats']['httpEreq'] = int(metrics_tmp['http_ereq'])
                haproxy_data['frontendStats']['httpDreq'] = int(metrics_tmp['http_dreq'])
                haproxy_data['frontendStats']['httpHrsp_4xx'] = int(metrics_tmp['http_hrsp_4xx'])
                haproxy_data['frontendStats']['httpHrsp_5xx'] = int(metrics_tmp['http_hrsp_5xx'])
                haproxy_data['frontendStats']['httpBin'] = float(metrics_tmp['http_bin']) / 1024
                haproxy_data['frontendStats']['httpBout'] = float(metrics_tmp['http_bout']) / 1024
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
                haproxy_data['backendStats']['httpRtime'] = int(metrics_tmp['http_rtime'])
                haproxy_data['backendStats']['httpEcon'] = int(metrics_tmp['http_econ'])
                haproxy_data['backendStats']['httpDresp'] = int(metrics_tmp['http_dresp'])
                haproxy_data['backendStats']['httpEresp'] = int(metrics_tmp['http_eresp'])
                haproxy_data['backendStats']['httpQcur'] = int(metrics_tmp['http_qcur'])
                haproxy_data['backendStats']['httpQtime'] = int(metrics_tmp['http_qtime'])
                haproxy_data['backendStats']['httpWredis'] = int(metrics_tmp['http_wredis'])
                haproxy_data['backendStats']['httpWretr'] = int(metrics_tmp['http_wretr'])
                haproxy_data['backendStats']['_documentType'] = 'backendStats'



            else:
                collectd.error("Plugin haproxy: Error collecting backend stats for http proxy")
                return

            self.add_common_params(haproxy_data['backendStats'], 'backendStats')

        except Exception as err:
            collectd.error("Plugin haproxy: Exception in get_backend_data due to %s" % err)

    def get_haproxy_data(self, lines, haproxy_data):
        dict_stats = {}

        try:
            for line in lines:
                line = line.strip("\n")
                if line != b'':
                    buf = line.split(":")
                    dict_stats[buf[0]] = buf[1].strip(" ")

            haproxy_data['haproxyStats']['version'] = dict_stats['Version']
            haproxy_data['haproxyStats']['upTime'] = int(dict_stats['Uptime_sec'])
            haproxy_data['haproxyStats']['currConns'] = int(dict_stats['CurrConns'])
            haproxy_data['haproxyStats']['connRate'] = int(dict_stats['ConnRate'])
            haproxy_data['haproxyStats']['nbproc'] = int(dict_stats['Nbproc'])
            haproxy_data['haproxyStats']['pipesUsed'] = int(dict_stats['PipesUsed'])
            haproxy_data['haproxyStats']['sslCacheMisses'] = int(dict_stats['SslCacheMisses'])
            haproxy_data['haproxyStats']['sslCacheLookups'] = int(dict_stats['SslCacheLookups'])
            haproxy_data['haproxyStats']['sessRate'] = int(dict_stats['SessRate'])
            haproxy_data['haproxyStats']['_documentType'] = "haproxyStats"

            self.add_common_params(haproxy_data['haproxyStats'], 'haproxyStats')

        except Exception as err:
            collectd.error("Plugin haproxy: Exception in get_haproxy_data due to %s" % err)

    def collect_haproxy_data(self, doc):
        try:
            haproxy_data = defaultdict(dict)
            if doc == "frontendStats" or doc == "backendStats":
                dict_stats = defaultdict(list)
                key_mapping = []

                cmnd = "echo 'show stat' | nc -U /var/lib/haproxy/stats"
                process = subprocess.Popen(cmnd, shell=True, stdout=subprocess.PIPE)
                lines = process.stdout.readlines()

                self.get_keys(key_mapping, lines)
                self.format_stats(dict_stats, lines)

                self.get_frontend_data(key_mapping, dict_stats, haproxy_data)
                self.get_backend_data(key_mapping, dict_stats, haproxy_data)

            if doc == "haproxyStats":
                cmnd = "echo 'show info' | nc -U /var/lib/haproxy/stats"
                process = subprocess.Popen(cmnd, shell=True, stdout=subprocess.PIPE)
                lines = process.stdout.readlines()

                self.get_haproxy_data(lines, haproxy_data)

            return haproxy_data

        except Exception as err:
            collectd.error("Plugin haproxy: Exception in collect_haproxy_data due to %s" % err)

    def add_default_diff_value(self, doc_stats, doc):
        if doc == 'frontendStats':
            keylist = ['httpEreq', 'httpDreq', 'httpHrsp_4xx', 'httpHrsp_5xx', 'httpBin', 'httpBout']

        elif doc == 'backendStats':
            keylist = ['httpEcon', 'httpDresp', 'httpEresp', 'httpWredis', 'httpWretr']

        elif doc == 'haproxyStats':
            keylist = ['sslCacheMisses', 'sslCacheLookups']

        for key in keylist:
            doc_stats[key] = 0

    def get_diff(self, key, curr_data, prev_data):
        diff = NAN
        if not prev_data:
            return diff

        if key not in prev_data:
            collectd.error("%s key not in previous data. Shouldn't happen." % key)
            return diff

        if TIMESTAMP not in curr_data or TIMESTAMP not in prev_data:
            collectd.error("%s key not in previous data. Shouldn't happen." % key)
            return diff

        curr_time = curr_data[TIMESTAMP]
        prev_time = prev_data[TIMESTAMP]

        if curr_time <= prev_time:
            collectd.error("Current data time: %s is less than previous data time: %s. "
                           "Shouldn't happen." % (curr_time, prev_time))
            return diff

        diff = curr_data[key] - prev_data[key]
        # rate can get negative if the topic(s) are deleted and created again with the same name
        # intializing rate to 0 if rates are negative value.
        # if diff < 0:
        #    rate = 0.0
        return diff

    def add_diff(self, doc_stats, doc):
        if doc == 'frontendStats':
            diff = self.get_diff('httpEreq', doc_stats, self.prev_frontend_data)
            if diff != NAN:
                doc_stats['httpEreq'] = diff

            diff = self.get_diff('httpDreq', doc_stats, self.prev_frontend_data)
            if diff != NAN:
                doc_stats['httpDreq'] = diff

            diff = self.get_diff('httpHrsp_4xx', doc_stats, self.prev_frontend_data)
            if diff != NAN:
                doc_stats['httpHrsp_4xx'] = diff

            diff = self.get_diff('httpHrsp_5xx', doc_stats, self.prev_frontend_data)
            if diff != NAN:
                doc_stats['httpHrsp_5xx'] = diff

            diff = self.get_diff('httpBin', doc_stats, self.prev_frontend_data)
            if diff != NAN:
                doc_stats['httpBin'] = round(diff, 2)

            diff = self.get_diff('httpBout', doc_stats, self.prev_frontend_data)
            if diff != NAN:
                doc_stats['httpBout'] = round(diff, 2)

        if doc == 'backendStats':
            diff = self.get_diff('httpEcon', doc_stats, self.prev_backend_data)
            if diff != NAN:
                doc_stats['httpEcon'] = diff

            diff = self.get_diff('httpDresp', doc_stats, self.prev_backend_data)
            if diff != NAN:
                doc_stats['httpDresp'] = diff

            diff = self.get_diff('httpEresp', doc_stats, self.prev_backend_data)
            if diff != NAN:
                doc_stats['httpEresp'] = diff

            diff = self.get_diff('httpWredis', doc_stats, self.prev_backend_data)
            if diff != NAN:
                doc_stats['httpWredis'] = diff

            diff = self.get_diff('httpWretr', doc_stats, self.prev_backend_data)
            if diff != NAN:
                doc_stats['httpWretr'] = diff

        if doc == 'haproxyStats':
            diff = self.get_diff('sslCacheMisses', doc_stats, self.prev_haproxy_data)
            if diff != NAN:
                doc_stats['sslCacheMisses'] = diff

            diff = self.get_diff('sslCacheLookups', doc_stats, self.prev_haproxy_data)
            if diff != NAN:
                doc_stats['sslCacheLookups'] = diff

    def add_dispatch_haproxy(self, doc_stats, doc):
        if doc == 'frontendStats':
            if self.pollCounter == 1:
                self.prev_frontend_data = deepcopy(doc_stats)
                self.add_default_diff_value(doc_stats, doc)
            else:
                self.add_diff(doc_stats, doc)
                self.prev_frontend_data = deepcopy(doc_stats)

        elif doc == 'backendStats':
            if self.pollCounter == 1:
                self.prev_backend_data = deepcopy(doc_stats)
                self.add_default_diff_value(doc_stats, doc)
            else:
                self.add_diff(doc_stats, doc)
                self.prev_backend_data = deepcopy(doc_stats)

        elif doc == 'haproxyStats':
            if self.pollCounter == 1:
                self.prev_haproxy_data = deepcopy(doc_stats)
                self.add_default_diff_value(doc_stats, doc)
            else:
                self.add_diff(doc_stats, doc)
                self.prev_haproxy_data = deepcopy(doc_stats)

        self.dispatch_data(deepcopy(doc_stats), doc)

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
            self.pollCounter += 1
            for doc in HAPROXY_DOCS:
                haproxy_stats = self.collect_haproxy_data(doc)

                if not haproxy_stats:
                    collectd.error("Plugin haproxy: Unable to fetch data for document type: %s." % doc)
                    return
                else:
                    #self.documentsTypes = ['frontendStats', 'backendStats', 'haproxyStats']
                    if haproxy_stats[doc]['_documentType'] not in self.documentsTypes:
                            del haproxy_stats[doc]

                    self.add_dispatch_haproxy(haproxy_stats[doc], doc)

        except Exception as err:
            collectd.error("Plugin haproxy: Couldn't read and gather the metrics due to the exception %s in %s" % (err, traceback.format_exc()))

    def dispatch_data(self, result, doc):
        """Dispatch data to collectd."""
        collectd.info("Plugin haproxy: Succesfully sent %s doctype to collectd." % doc)
        collectd.info("Plugin haproxy: Values dispatched =%s" % json.dumps(result))

        utils.dispatch(result)

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