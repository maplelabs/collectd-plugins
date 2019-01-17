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
        self.socket_path = None
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
            if children.key == "socket_path":
                self.socket_path = children.values[0]

    def get_keys(self, key_mapping, lines):
        """Gets keys of metrics as specified by the haproxy csv stats exposed"""
        key_buf = lines[0].strip("# \n").split(",")[2:]
        for key in key_buf:
            if key:
                key_mapping.append(key)

    def format_stats(self, dict_stats, lines):
        """Formats the stats exposed by haproxy stats socket to form a dictionary"""
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
        """Get data for frontend metrics"""
        try:
            metrics_tmp = {}
            for index in dict_stats.keys():
                if dict_stats[index][1] == "FRONTEND":
                    i = 2
                    for key in key_mapping:
                        metrics_tmp[key] = dict_stats[index][i]
                        if metrics_tmp[key] == '':
                            metrics_tmp[key] = 0
                        i += 1

                    pxname = dict_stats[index][0]
                    if metrics_tmp:
                        haproxy_data['frontendStats'][pxname] = {}
                        haproxy_data['frontendStats'][pxname]['pxName'] = dict_stats[index][0]
                        haproxy_data['frontendStats'][pxname]['reqRate'] = float(metrics_tmp['req_rate'])
                        haproxy_data['frontendStats'][pxname]['rate'] = float(metrics_tmp['rate'])
                        haproxy_data['frontendStats'][pxname]['ereq'] = int(metrics_tmp['ereq'])
                        haproxy_data['frontendStats'][pxname]['dreq'] = int(metrics_tmp['dreq'])
                        haproxy_data['frontendStats'][pxname]['hrsp_4xx'] = int(metrics_tmp['hrsp_4xx'])
                        haproxy_data['frontendStats'][pxname]['hrsp_5xx'] = int(metrics_tmp['hrsp_5xx'])
                        haproxy_data['frontendStats'][pxname]['bin'] = float(metrics_tmp['bin']) / 1024
                        haproxy_data['frontendStats'][pxname]['bout'] = float(metrics_tmp['bout']) / 1024
                        haproxy_data['frontendStats'][pxname]['sutil'] = (float(metrics_tmp['scur']) / float(metrics_tmp['slim'])) * 100
                        haproxy_data['frontendStats'][pxname]['_documentType'] = 'frontendStats'

                        self.add_common_params(haproxy_data['frontendStats'][pxname], 'frontendStats')

                    else:
                        collectd.info("Plugin haproxy: Error collecting frontend stats for %s proxy" % pxname)
                        return

        except Exception as err:
            collectd.error("Plugin haproxy: Exception in get_frontend_data due to %s" % err)
            collectd.error("Traceback: %s" % traceback.format_exc())
            return

    def get_backend_data(self, key_mapping, dict_stats, haproxy_data):
        """Get data for backend metrics"""
        try:
            metrics_tmp = {}
            for index in dict_stats.keys():
                if dict_stats[index][1] == "BACKEND":
                    i = 2
                    for key in key_mapping:
                        metrics_tmp[key] = dict_stats[index][i]
                        if metrics_tmp[key] == '':
                            metrics_tmp[key] = 0
                        i += 1

                    pxname = dict_stats[index][0]
                    if metrics_tmp:
                        haproxy_data['backendStats'][pxname] = {}
                        haproxy_data['backendStats'][pxname]['pxName'] = pxname
                        haproxy_data['backendStats'][pxname]['rtime'] = int(metrics_tmp['rtime'])
                        haproxy_data['backendStats'][pxname]['econ'] = int(metrics_tmp['econ'])
                        haproxy_data['backendStats'][pxname]['dresp'] = int(metrics_tmp['dresp'])
                        haproxy_data['backendStats'][pxname]['eresp'] = int(metrics_tmp['eresp'])
                        haproxy_data['backendStats'][pxname]['qcur'] = int(metrics_tmp['qcur'])
                        haproxy_data['backendStats'][pxname]['qtime'] = int(metrics_tmp['qtime'])
                        haproxy_data['backendStats'][pxname]['wredis'] = int(metrics_tmp['wredis'])
                        haproxy_data['backendStats'][pxname]['wretr'] = int(metrics_tmp['wretr'])
                        haproxy_data['backendStats'][pxname]['hrsp_4xx'] = int(metrics_tmp['hrsp_4xx'])
                        haproxy_data['backendStats'][pxname]['hrsp_5xx'] = int(metrics_tmp['hrsp_5xx'])
                        haproxy_data['backendStats'][pxname]['_documentType'] = 'backendStats'

                        self.add_common_params(haproxy_data['backendStats'][pxname], 'backendStats')

                    else:
                        collectd.info("Plugin haproxy: Error collecting backend stats for %s proxy" % pxname)
                        return



        except Exception as err:
            collectd.error("Plugin haproxy: Exception in get_backend_data due to %s" % err)
            collectd.error("Traceback: %s" % traceback.format_exc())

    def get_haproxy_data(self, lines, haproxy_data):
        """Get general haproxy stats"""
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
        """Collect haproxy data for various doc types"""
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
        """Add default diff values for first poll"""
        if doc == 'frontendStats':
            keylist = ['ereq', 'dreq', 'hrsp_4xx', 'hrsp_5xx', 'bin', 'bout']

        elif doc == 'backendStats':
            keylist = ['econ', 'dresp', 'eresp', 'wredis', 'wretr', 'hrsp_4xx', 'hrsp_5xx']

        elif doc == 'haproxyStats':
            keylist = ['sslCacheMisses', 'sslCacheLookups']

        for key in keylist:
            doc_stats[key] = 0

    def get_diff(self, key, curr_data, prev_data):
        """Get diff values"""
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

    def add_diff(self, doc_stats, doc, pxname):
        """Add the diff values to metrics dictionary"""
        if doc == 'frontendStats':
            diff = self.get_diff('ereq', doc_stats, self.prev_frontend_data[pxname])
            if diff != NAN:
                doc_stats['ereq'] = diff

            diff = self.get_diff('dreq', doc_stats, self.prev_frontend_data[pxname])
            if diff != NAN:
                doc_stats['dreq'] = diff

            diff = self.get_diff('hrsp_4xx', doc_stats, self.prev_frontend_data[pxname])
            if diff != NAN:
                doc_stats['hrsp_4xx'] = diff

            diff = self.get_diff('hrsp_5xx', doc_stats, self.prev_frontend_data[pxname])
            if diff != NAN:
                doc_stats['hrsp_5xx'] = diff

            diff = self.get_diff('bin', doc_stats, self.prev_frontend_data[pxname])
            if diff != NAN:
                doc_stats['bin'] = round(diff, 2)

            diff = self.get_diff('bout', doc_stats, self.prev_frontend_data[pxname])
            if diff != NAN:
                doc_stats['bout'] = round(diff, 2)

        if doc == 'backendStats':
            diff = self.get_diff('econ', doc_stats, self.prev_backend_data[pxname])
            if diff != NAN:
                doc_stats['econ'] = diff

            diff = self.get_diff('dresp', doc_stats, self.prev_backend_data[pxname])
            if diff != NAN:
                doc_stats['dresp'] = diff

            diff = self.get_diff('eresp', doc_stats, self.prev_backend_data[pxname])
            if diff != NAN:
                doc_stats['eresp'] = diff

            diff = self.get_diff('wredis', doc_stats, self.prev_backend_data[pxname])
            if diff != NAN:
                doc_stats['wredis'] = diff

            diff = self.get_diff('wretr', doc_stats, self.prev_backend_data[pxname])
            if diff != NAN:
                doc_stats['wretr'] = diff

            diff = self.get_diff('hrsp_4xx', doc_stats, self.prev_backend_data[pxname])
            if diff != NAN:
                doc_stats['hrsp_4xx'] = diff

            diff = self.get_diff('hrsp_5xx', doc_stats, self.prev_backend_data[pxname])
            if diff != NAN:
                doc_stats['hrsp_5xx'] = diff

        if doc == 'haproxyStats':
            diff = self.get_diff('sslCacheMisses', doc_stats, self.prev_haproxy_data)
            if diff != NAN:
                doc_stats['sslCacheMisses'] = diff

            diff = self.get_diff('sslCacheLookups', doc_stats, self.prev_haproxy_data)
            if diff != NAN:
                doc_stats['sslCacheLookups'] = diff

    def add_dispatch_haproxy(self, doc_stats, doc):
        """Add difference values to haproxyStats and dispatch values"""
        if doc == 'haproxyStats':
            if self.pollCounter == 1:
                self.prev_haproxy_data = deepcopy(doc_stats)
                self.add_default_diff_value(doc_stats, doc)
            else:
                self.add_diff(doc_stats, doc, 'haproxy')
                self.prev_haproxy_data = deepcopy(doc_stats)

            self.dispatch_data(deepcopy(doc_stats), doc)

    def add_dispatch_fbstats(self, doc_stats, doc):
        """Add difference values to frontend and backend stats and dispatch values"""
        try:
            if doc == 'frontendStats':
                for pxname in doc_stats.keys():
                    if self.pollCounter==1 or pxname not in self.prev_frontend_data.keys():
                        self.prev_frontend_data[pxname] = deepcopy(doc_stats)
                        self.add_default_diff_value(doc_stats[pxname], doc)
                    else:
                        self.add_diff(doc_stats[pxname], doc, pxname)
                        self.prev_frontend_data[pxname] = deepcopy(doc_stats[pxname])

            elif doc == 'backendStats':
                for pxname in doc_stats.keys():
                    if self.pollCounter == 1 or pxname not in self.prev_backend_data.keys():
                        self.prev_backend_data[pxname] = deepcopy(doc_stats)
                        self.add_default_diff_value(doc_stats[pxname], doc)
                    else:
                        self.add_diff(doc_stats[pxname], doc, pxname)
                        self.prev_backend_data[pxname] = deepcopy(doc_stats[pxname])

            self.dispatch_data(deepcopy(doc_stats), doc)

        except Exception as err:
            collectd.error("Plugin haproxy: Error in add_dispatch_fbstats due to %s" % err)

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
                    if doc not in self.documentsTypes:
                            del haproxy_stats[doc]

                    if doc == 'haproxyStats' and doc in self.documentsTypes:
                        self.add_dispatch_haproxy(haproxy_stats[doc], doc)

                    elif (doc == 'frontendStats' or doc == 'backendStats') and (doc in self.documentsTypes):
                        self.add_dispatch_fbstats(haproxy_stats[doc], doc)

        except Exception as err:
            collectd.error("Plugin haproxy: Couldn't read and gather the metrics due to the exception %s in %s" % (err, traceback.format_exc()))

    def dispatch_data(self, result, doc):
        """Dispatch data to collectd."""
        if doc == "haproxyStats":
            collectd.info("Plugin haproxy: Succesfully sent %s doctype to collectd." % doc)
            collectd.debug("Plugin haproxy: Values dispatched =%s" % json.dumps(result))

            utils.dispatch(result)

        elif doc == "frontendStats" or doc == "backendStats":
            for pxname in result.keys():
                collectd.info("Plugin haproxy: Succesfully sent %s of %s to collectd." % (doc, pxname))
                collectd.debug("Plugin haproxy: Values dispatched =%s" % json.dumps(result[pxname]))
                utils.dispatch(result[pxname])

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