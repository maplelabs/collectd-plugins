"""Python plugin for collectd to fetch tomcatstats for tomcat process"""

# !/usr/bin/python
import os
import re
import signal
import json
import time
import collectd
import Queue
import multiprocessing
from copy import deepcopy
import psutil
import socket
import subprocess
from pyjolokia import Jolokia
# user imports
import utils
from constants import *
from libtomcatjolokia import JolokiaClient

TOMCAT_DOCS = ["tomcatStats", "requestProcessorStats", "jvmStats"]
DEFAULT_GC = ['PS MarkSweep', 'MarkSweepCompact']


class JmxStat(object):
    """Plugin object will be created only once and collects JMX statistics info every interval."""

    def __init__(self):
        """Initializes interval and previous dictionary variable."""
        self.interval = DEFAULT_INTERVAL
        self.process = 'catalina'
        self.listenerip = 'localhost'
        self.prev_req_data = {}
        self.prev_data = {}
        self.port = None
        self.documentsTypes = []
        self.jclient = JolokiaClient(os.path.basename(__file__)[:-3], self.process)

    def config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == PORT:
                self.port = children.values[0]
            if children.key == DOCUMENTSTYPES:
                self.documentsTypes = children.values[0]

    def get_jmx_parameters(self, jolokiaclient, doc, dict_jmx):
        """Fetch stats based on doc_type"""
        if doc == "tomcatStats":
            self.add_tomcat_parameters(jolokiaclient, dict_jmx)
        elif doc == "requestProcessorStats":
            self.add_request_proc_parameters(jolokiaclient, dict_jmx)
        elif doc == "jvmStats":
            self.add_jvm_parameters(jolokiaclient, dict_jmx)

    def add_default_diff_value(self, dict_jmx, doctype):
        """Add default value to rate key based on type"""
        if doctype == "tomcatStats":
            keylist = ["hitCount", "lookupCount"]
            for key in keylist:
                dict_jmx[key] = 0
        elif doctype == "requestProcessorStats":
            keylist = ["bytesReceived", "bytesSent"]
            for key in keylist:
                dict_jmx[key] = 0.0


    def get_diff(self, key, curr_data, prev_data):
        """Calculate and returns rate. Rate=(current_value-prev_value)/time."""
        # TODO The code is similar to the one in utils.py.
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

    def add_diff(self, pid, doc, dict_info):
        """diff tomcatStats and requestProcessorStats"""
        if doc == "tomcatStats":
            diff = self.get_diff('hitCount', dict_info, self.prev_data[pid])
            if diff != NAN:
                dict_info['hitCount'] = diff
            diff = self.get_diff('lookupCount', dict_info, self.prev_data[pid])
            if diff != NAN:
                dict_info['lookupCount'] = diff
        elif doc == "requestProcessorStats":
            diff = self.get_diff('bytesReceived', dict_info, self.prev_req_data[pid])
            if diff != NAN:
                dict_info['bytesReceived'] = round(diff, FLOATING_FACTOR)
            diff = self.get_diff('bytesSent', dict_info, self.prev_req_data[pid])
            if diff != NAN:
                dict_info['bytesSent'] = round(diff, FLOATING_FACTOR)

    def add_request_proc_parameters(self, jolokiaclient, dict_jmx):
        """Add jmx stats specific to tomcat metrics"""
        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=GlobalRequestProcessor',
                                  attribute='bytesSent')
        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=GlobalRequestProcessor',
                                  attribute='bytesReceived')
        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=GlobalRequestProcessor',
                                  attribute='requestCount')
        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=GlobalRequestProcessor',
                                  attribute='errorCount')
        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=GlobalRequestProcessor',
                                  attribute='maxTime')
        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=GlobalRequestProcessor',
                                  attribute='processingTime')

        bulkdata = jolokiaclient.getRequests()
        dict_jmx['bytesSent'] = bulkdata[0].get('value', 0) / (1024.0 * 1024.0)
        dict_jmx['bytesReceived'] = bulkdata[1].get('value', 0) / (1024.0 * 1024.0)
        dict_jmx['requestCount'] = bulkdata[2].get('value', 0)
        dict_jmx['errorCount'] = bulkdata[3].get('value', 0)
        dict_jmx['maxTime'] = bulkdata[4].get('value', 0)
        dict_jmx['processingTime'] = bulkdata[4].get('value', 0)

    def add_tomcat_parameters(self, jolokiaclient, dict_jmx):
        """Add jmx stats specific to tomcat metrics"""

        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=ThreadPool',
                                  attribute='currentThreadsBusy')
        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=ThreadPool',
                                  attribute='currentThreadCount')
        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=ThreadPool',
                                  attribute='maxThreads')
        jolokiaclient.add_request(type='read',
                                  mbean='Catalina:context=/ispring,host=localhost,name=Cache,type=WebResourceRoot',
                                  attribute='hitCount')
        jolokiaclient.add_request(type='read',
                                  mbean='Catalina:context=/ispring,host=localhost,name=Cache,type=WebResourceRoot',
                                  attribute='lookupCount')
        jolokiaclient.add_request(type='read', mbean='Catalina:type=Server', attribute='serverInfo')

        bulkdata = jolokiaclient.getRequests()
        dict_jmx['currentThreadsBusy'] = bulkdata[0].get('value', 0)
        dict_jmx['currentThreadCount'] = bulkdata[1].get('value', 0)
        dict_jmx['maxThreads'] = bulkdata[2].get('value', 0)
        dict_jmx['hitCount'] = bulkdata[3].get('value', 0)
        dict_jmx['lookupCount'] = bulkdata[4].get('value', 0)
        version = bulkdata[5].get('value', 0)
        dict_jmx['version'] = version.split('/')[1]

    def add_jvm_parameters(self, jolokiaclient, dict_jmx):
        """Add jmx stats specific to tomcat metrics"""
        jolokiaclient.add_request(type='read', mbean='java.lang:type=ClassLoading', attribute='LoadedClassCount')
        jolokiaclient.add_request(type='read', mbean='java.lang:type=ClassLoading', attribute='UnLoadedClassCount')
        jolokiaclient.add_request(type='read', mbean='java.lang:type=Memory', attribute='HeapMemoryUsage')
        jolokiaclient.add_request(type='read', mbean='java.lang:type=Memory', attribute='NonHeapMemoryUsage')
        jolokiaclient.add_request(type='read', mbean='java.lang:type=Runtime', attribute='Uptime')

        bulkdata = jolokiaclient.getRequests()
        dict_jmx['LoadedClassCount'] = bulkdata[0].get('value', 0)
        dict_jmx['UnLoadedClassCount'] = bulkdata[1].get('value', 0)
        dict_jmx['HeapMemoryUsage'] = bulkdata[2].get('value', 0)
        dict_jmx['NonHeapMemoryUsage'] = bulkdata[3].get('value', 0)
        dict_jmx['Uptime'] = bulkdata[4].get('value', 0)
        self.add_gc_parameters(jolokiaclient, dict_jmx)

    def add_gc_parameters(self, jolokiaclient, dict_jmx):
        """Add garbage collector related jmx stats"""

        def memory_gc_usage(self, mempool_gc, key, gc_name, dict_jmx):
            for name, values in mempool_gc.items():
                # if name in ['G1 Eden Space', 'G1 Old Gen']:
                mem = ''.join(name.split())

                if re.search("Tenured", mem):
                    mp_name = "TenuredGen"
                elif re.search("Old", mem):
                    mp_name = "OldGen"
                elif re.search("Compressed Class", mem):
                    mp_name = "CompClass"
                elif re.search("Metaspace", mem):
                    mp_name = "Metaspace"
                elif re.search("Survivor", mem):
                    mp_name = "Survivor"
                elif re.search("Code Cache", mem):
                    mp_name = "CodeCache"
                elif re.search("Eden", mem):
                    mp_name = "Eden"
                else:
                    mp_name = mem

                # self.handle_neg_bytes(values['init'], gc_name+key+mp_name+'Init', dict_jmx)
                # self.handle_neg_bytes(values['max'], gc_name+key+mp_name+'Max', dict_jmx)
                dict_jmx[key + mp_name + 'Used'] = round(values['used'] / 1024.0 / 1024.0, 2)
                dict_jmx[key + mp_name + 'Committed'] = round(values['committed'] / 1024.0 / 1024.0, 2)

        gc_names = self.get_gc_names(jolokiaclient)
        for gc_name in gc_names:
            str_mbean = 'java.lang:type=GarbageCollector,name=' + gc_name
            if_valid = jolokiaclient.request(type='read', mbean=str_mbean, attribute='Valid')

            if if_valid['status'] == 200 and if_valid['value'] == True:
                str_attribute = 'LastGcInfo'
                gc_values = jolokiaclient.request(type='read', mbean=str_mbean, attribute=str_attribute)
                gc_name_no_spaces = ''.join(gc_name.split())
                if gc_values['status'] == 200:
                    # dict_jmx[gc_name_no_spaces+'StartTime'] = gc_values['value']['startTime']
                    # dict_jmx[gc_name_no_spaces+'EndTime'] = gc_values['value']['endTime']
                    dict_jmx['GcDuration'] = gc_values['value']['duration']
                    dict_jmx[gc_name_no_spaces + 'GcThreadCount'] = gc_values['value']['GcThreadCount']
                    mem_aftergc = gc_values['value']['memoryUsageAfterGc']
                    memory_gc_usage(self, mem_aftergc, 'AfGc', gc_name_no_spaces, dict_jmx)
                    mem_beforegc = gc_values['value']['memoryUsageBeforeGc']
                    memory_gc_usage(self, mem_beforegc, 'BfGc', gc_name_no_spaces, dict_jmx)

    def get_gc_names(self, jolokiaclient):
        gc_json = jolokiaclient.request(type='read', mbean='java.lang:type=GarbageCollector,*', attribute='Name')
        gc_names = []
        if gc_json['status'] == 200:
            for _, value in gc_json['value'].items():
                if value['Name'] in DEFAULT_GC:
                    gc_names.append(value['Name'])
                else:
                    collectd.error("Plugin tomcat: not supported for GC %s" % value['Name'])
        return gc_names

    def add_common_params(self, doc, dict_jmx):
        """Adds TIMESTAMP, PLUGIN, PLUGITYPE to dictionary."""
        timestamp = int(round(time.time()))
        dict_jmx[TIMESTAMP] = timestamp
        dict_jmx[PLUGIN] = TOMCAT
        dict_jmx[PLUGINTYPE] = doc
        dict_jmx[ACTUALPLUGINTYPE] = TOMCAT
        # dict_jmx[PLUGIN_INS] = doc
        collectd.info("Plugin tomcat: Added common parameters successfully for %s doctype" % doc)

    def add_dispatch_tomcat(self, pid, doc, dict_jmx):
        """Rate calculation for topic metrics"""
        # for topic, topic_info in dict_jmx.items():
        #    self.add_common_params(doc, topic_info)
        if doc == "tomcatStats":
            if pid in self.prev_data:
                temp_tomcatStats = deepcopy(dict_jmx)
                self.add_diff(pid, doc, dict_jmx)
                self.prev_data[pid] = temp_tomcatStats
            else:
                self.prev_data[pid] = deepcopy(dict_jmx)
                self.add_default_diff_value(dict_jmx, doc)
        elif doc == "requestProcessorStats":
            if pid in self.prev_req_data:
                temp_reqStats = deepcopy(dict_jmx)
                self.add_diff(pid, doc, dict_jmx)
                self.prev_req_data[pid] = temp_reqStats
            else:
                self.prev_req_data[pid] = deepcopy(dict_jmx)
                self.add_default_diff_value(dict_jmx, doc)
        self.dispatch_data(doc, deepcopy(dict_jmx))

    def get_pid_jmx_stats(self, pid, port, output):
        """Call get_jmx_parameters function for each doc_type and add dict to queue"""
        jolokiaclient = JolokiaClient.get_jolokia_inst(port)
        for doc in TOMCAT_DOCS:
            try:
                dict_jmx = {}
                self.get_jmx_parameters(jolokiaclient, doc, dict_jmx)
                if not dict_jmx:
                    raise ValueError("No data found")

                collectd.info("Plugin tomcat: Added %s doctype information successfully for pid %s" % (doc, pid))
                # if doc in ["tomcatStats", "jvmStats", "requestProcessorStats"]:
                #    output.put((pid, doc, dict_jmx))
                #    continue

                self.add_common_params(doc, dict_jmx)
                output.put((pid, doc, dict_jmx))
            except Exception as err:
                collectd.error("Plugin tomcat: Error in collecting stats of %s doctype: %s" % (doc, str(err)))

    def run_pid_process(self, list_pid):
        """Spawn process for each pid"""
        procs = []
        output = multiprocessing.Queue()
        for pid in list_pid:
            port = self.jclient.get_jolokia_port(pid)
            if port and self.jclient.connection_available(port):
                proc = multiprocessing.Process(target=self.get_pid_jmx_stats, args=(pid, port, output))
                procs.append(proc)
                proc.start()

        for proc in procs:
            proc.join()
        # for p in procs:
        #          collectd.debug("%s, %s" % (p, p.is_alive()))
        return procs, output

    def collect_jmx_data(self):
        """Collects stats and spawns process for each pids."""
        list_pid = self.jclient.get_pid()
        if not list_pid:
            collectd.error("Plugin tomcat: No %s processes are running" % self.process)
            return

        procs, output = self.run_pid_process(list_pid)
        for _ in procs:
            for _ in TOMCAT_DOCS:
                try:
                    pid, doc_name, doc_result = output.get_nowait()
                except Queue.Empty:
                    collectd.error("Failed to send one or more doctype document to collectd")
                    continue
                # Dispatching documentsTypes which are requetsed alone
                if doc_name in self.documentsTypes:
                    # self.add_common_params(doc_name, doc_result)
                    # self.dispatch_data(doc_name, doc_result)

                    if doc_name in ["tomcatStats", "requestProcessorStats"]:
                        self.add_dispatch_tomcat(pid, doc_name, doc_result)
                    else:
                        self.dispatch_data(doc_name, doc_result)
        output.close()

    def dispatch_data(self, doc_name, result):
        """Dispatch data to collectd."""
        collectd.info("Plugin tomcat: Succesfully sent %s doctype to collectd." % doc_name)
        collectd.debug("Plugin tomcat: Values dispatched =%s" % json.dumps(result))

        utils.dispatch(result)

    def read_temp(self):
        """Collectd first calls register_read. At that time default interval is taken,
        hence temporary function is made to call, the read callback is unregistered
        and read() is called again with interval obtained from conf by register_config callback."""
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.collect_jmx_data, interval=int(self.interval))


def init():
    """When new process is formed, action to SIGCHLD is reset to default behavior."""
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)


OBJ = JmxStat()
collectd.register_init(init)
collectd.register_config(OBJ.config)
collectd.register_read(OBJ.read_temp)