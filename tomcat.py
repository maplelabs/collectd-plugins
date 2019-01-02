"""Python plugin for collectd to fetch tomcatstats for tomcat process"""

#!/usr/bin/python
import os
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

class JmxStat(object):
    """Plugin object will be created only once and collects JMX statistics info every interval."""

    def __init__(self):
        """Initializes interval and previous dictionary variable."""
        self.interval = DEFAULT_INTERVAL
        self.process = 'catalina'
        self.listenerip = 'localhost'
        self.prev_topic_data = {}
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

    def add_default_rate_value(self, dict_jmx, doctype):
        """Add default value to rate key based on type"""
        if doctype == "tomcatstats":
            keylist = ["messagesIn", "bytesIn", "bytesOut", "isrExpands", "isrShrinks", "leaderElection", \
                       "uncleanLeaderElections", "producerRequests", "fetchConsumerRequests", "fetchFollowerRequests"]
        else:
            keylist = ["messagesInRate", "bytesOutRate", "bytesInRate", "totalFetchRequestsRate", "totalProduceRequestsRate", \
                       "produceMessageConversionsRate", "failedProduceRequestsRate", "fetchMessageConversionsRate", "failedFetchRequestsRate",\
                       "bytesRejectedRate"]
        for key in keylist:
            dict_jmx[key] = 0

    def get_rate(self, key, curr_data, prev_data):
        """Calculate and returns rate. Rate=(current_value-prev_value)/time."""
        #TODO The code is similar to the one in utils.py.
        rate = NAN
        if not prev_data:
            return rate

        if key not in prev_data:
            collectd.error("%s key not in previous data. Shouldn't happen." % key)
            return rate

        if TIMESTAMP not in curr_data or TIMESTAMP not in prev_data:
            collectd.error("%s key not in previous data. Shouldn't happen." % key)
            return rate

        curr_time = curr_data[TIMESTAMP]
        prev_time = prev_data[TIMESTAMP]

        if curr_time <= prev_time:
            collectd.error("Current data time: %s is less than previous data time: %s. "
                           "Shouldn't happen." % (curr_time, prev_time))
            return rate

        rate = (curr_data[key] - prev_data[key]) / float(self.interval)
        # rate can get negative if the topic(s) are deleted and created again with the same name
        # intializing rate to 0 if rates are negative value.
        if rate < 0:
            rate = 0.0
        return rate

    def add_rate(self, pid, dict_jmx):
        """Rate only for kafka jmx metrics"""
        rate = self.get_rate("messagesInPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["messagesIn"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("bytesInPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["bytesIn"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("bytesOutPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["bytesOut"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("isrExpandsPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["isrExpands"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("isrShrinksPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["isrShrinks"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("leaderElectionPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["leaderElection"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("uncleanLeaderElectionPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["uncleanLeaderElections"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("producerRequestsPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["producerRequests"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("fetchConsumerRequestsPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["fetchConsumerRequests"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("fetchFollowerRequestsPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["fetchFollowerRequests"] = round(rate, FLOATING_FACTOR)

    def add_topic_rate(self, pid, topic_name, topic_info):
        """Rate only for kafka topic metrics"""
        rate = self.get_rate('messagesIn', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['messagesInRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('bytesOut', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['bytesOutRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('bytesIn', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['bytesInRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('totalFetchRequests', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['totalFetchRequestsRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('totalProduceRequests', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['totalProduceRequestsRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('produceMessageConversions', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['produceMessageConversionsRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('failedProduceRequests', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['failedProduceRequestsRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('fetchMessageConversions', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['fetchMessageConversionsRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('failedFetchRequests', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['failedFetchRequestsRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('bytesRejected', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['bytesRejectedRate'] = round(rate, FLOATING_FACTOR)

    def add_request_proc_parameters(self, jolokiaclient, dict_jmx):
        """Add jmx stats specific to tomcat metrics"""
        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=GlobalRequestProcessor', attribute='bytesSent')
        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=GlobalRequestProcessor', attribute='bytesReceived')
        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=GlobalRequestProcessor', attribute='requestCount')
        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=GlobalRequestProcessor', attribute='errorCount')
        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=GlobalRequestProcessor', attribute='maxTime')
        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=GlobalRequestProcessor', attribute='processingTime')

        bulkdata = jolokiaclient.getRequests()
        dict_jmx['bytesSent'] = bulkdata[0].get('value', 0)
        dict_jmx['bytesReceived'] = bulkdata[1].get('value', 0)
        dict_jmx['requestCount'] = bulkdata[2].get('value', 0)
        dict_jmx['errorCount'] = bulkdata[3].get('value', 0)
        dict_jmx['maxTime'] = bulkdata[4].get('value', 0)
        dict_jmx['processingTime'] = bulkdata[4].get('value', 0)

    def add_tomcat_parameters(self, jolokiaclient, dict_jmx):
        """Add jmx stats specific to tomcat metrics"""

        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=ThreadPool', attribute='currentThreadsBusy')
        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=ThreadPool', attribute='currentThreadCount')
        jolokiaclient.add_request(type='read', mbean='Catalina:name=http-nio-80,type=ThreadPool', attribute='maxThreads')
        jolokiaclient.add_request(type='read', mbean='Catalina:context=/ispring,host=localhost,name=Cache,type=WebResourceRoot',attribute='hitCount')
        jolokiaclient.add_request(type='read', mbean='Catalina:context=/ispring,host=localhost,name=Cache,type=WebResourceRoot',attribute='lookupCount')
        jolokiaclient.add_request(type='read', mbean='Catalina:type=Server',attribute='serverInfo')

        bulkdata = jolokiaclient.getRequests()
        dict_jmx['currentThreadsBusy'] = bulkdata[0].get('value', 0)
        dict_jmx['currentThreadCount'] = bulkdata[1].get('value', 0)
        dict_jmx['maxThreads'] = bulkdata[2].get('value', 0)
        dict_jmx['hitCount'] = bulkdata[3].get('value', 0)
        dict_jmx['lookupCount'] = bulkdata[4].get('value', 0)
        dict_jmx['version'] = bulkdata[5].get('value', 0)

    def add_jvm_parameters(self, jolokiaclient, dict_jmx):
        """Add jmx stats specific to tomcat metrics"""
        jolokiaclient.add_request(type='read', mbean='java.lang:type=ClassLoading', attribute='LoadedClassCount')
        jolokiaclient.add_request(type='read', mbean='java.lang:type=ClassLoading', attribute='UnLoadedClassCount')
        jolokiaclient.add_request(type='read', mbean='java.lang:type=Memory', attribute='HeapMemoryUsage')
        jolokiaclient.add_request(type='read', mbean='java.lang:type=Memory', attribute='NonHeapMemoryUsage')
        #jolokiaclient.add_request(type='read', mbean='java.lang:name=*,type=GarbageCollector', attribute='LastGcInfo')
        bulkdata = jolokiaclient.getRequests()
        dict_jmx['LoadedClassCount'] = bulkdata[0].get('value', 0)
        dict_jmx['UnLoadedClassCount'] = bulkdata[1].get('value', 0)
        dict_jmx['HeapMemoryUsage'] = bulkdata[2].get('value', 0)
        dict_jmx['NonHeapMemoryUsage'] = bulkdata[3].get('value', 0)


    def add_common_params(self, doc, dict_jmx):
        """Adds TIMESTAMP, PLUGIN, PLUGITYPE to dictionary."""
        timestamp = int(round(time.time()))
        dict_jmx[TIMESTAMP] = timestamp
        dict_jmx[PLUGIN] = TOMCAT
        dict_jmx[PLUGINTYPE] = doc
        dict_jmx[ACTUALPLUGINTYPE] = TOMCAT
        #dict_jmx[PLUGIN_INS] = doc
        collectd.info("Plugin tomcat: Added common parameters successfully for %s doctype" % doc)

    def add_rate_dispatch_topic(self, pid, doc, dict_jmx):
        """Rate calculation for topic metrics"""
        for topic, topic_info in dict_jmx.items():
            self.add_common_params(doc, topic_info)
            if pid in self.prev_topic_data:
                if topic in self.prev_topic_data[pid]:
                    self.add_topic_rate(pid, topic, topic_info)
                else:
                    self.add_default_rate_value(topic_info, "topic")
            else:
                self.prev_topic_data[pid] = {}
                self.add_default_rate_value(topic_info, "topic")

            self.prev_topic_data[pid][topic] = topic_info
            self.dispatch_data(doc, deepcopy(topic_info))

    def add_rate_dispatch_kafka(self, pid, doc, dict_jmx):
        if pid in self.prev_data:
            self.add_rate(pid, dict_jmx)
        else:
            self.add_default_rate_value(dict_jmx, "kafka")

        self.prev_data[pid] = dict_jmx
        self.dispatch_data(doc, deepcopy(dict_jmx))

    def dispatch_stats(self, doc, dict_jmx):
        if doc == "partitionStats":
            stats_list = dict_jmx["partitionStats"]
        else:
            stats_list = dict_jmx["consumerStats"]
        for stats in stats_list:
            self.add_common_params(doc, stats)
            self.dispatch_data(doc, stats)

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
                if doc in ["tomcatStats", "jvmStats", "requestProcessorStats"]:
                    output.put((pid, doc, dict_jmx))
                    continue

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
#       for p in procs:
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
                    self.add_common_params(doc_name, doc_result)
                    self.dispatch_data(doc_name, doc_result)

                    #if doc_name == "topicStats":
                    #    self.add_rate_dispatch_topic(pid, doc_name, doc_result)
                    #elif doc_name == "kafkaStats":
                    #    self.add_rate_dispatch_kafka(pid, doc_name, doc_result)
                    #else:
                    #    self.dispatch_stats(doc_name, doc_result)
        output.close()

    def dispatch_data(self, doc_name, result):
        """Dispatch data to collectd."""
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
