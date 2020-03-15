"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
"""Python plugin for collectd to fetch zookeeper_jmx for process given."""

#!/usr/bin/python
import os
import signal
import json
import time
import collectd
import Queue
import multiprocessing
from copy import deepcopy
import subprocess
# user imports
import utils
from constants import *
from libjolokia import JolokiaClient

#GENERIC_DOCS = ["memoryPoolStats", "memoryStats", "threadStats", "gcStats", "classLoadingStats",
#        "compilationStats", "nioStats", "operatingSysStats"]
ZOOK_DOCS = ["jmxStats", "zookeeperStats"]

DEFAULT_GC = ['G1 Old Generation', 'G1 Young Generation']
DEFAULT_MP = ['G1 Eden Space', 'G1 Old Gen', 'G1 Survivor Space', 'Metaspace', 'Code Cache', 'Compressed Class Space']

class JmxStat(object):
    """Plugin object will be created only once and collects JMX statistics info every interval."""

    def __init__(self):
        """Initializes interval and previous dictionary variable."""
        self.process = 'zookeeper'
        self.interval = DEFAULT_INTERVAL
        self.listenerip = 'localhost'
        self.port = None
        self.prev_data = {}
        self.documentsTypes = []
        self.jclient = JolokiaClient(os.path.basename(__file__)[:-3], self.process)

    def config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == LISTENERIP:
                self.listenerip = children.values[0]
            if children.key == PORT:
                self.port = children.values[0]
            if children.key == DOCUMENTSTYPES:
                self.documentsTypes = children.values[0]

    def get_jmx_parameters(self, jolokiaclient, doc, dict_jmx):
        """Fetch stats based on doc_type"""
        if doc == "memoryPoolStats":
            self.add_memory_pool_parameters(jolokiaclient, dict_jmx)
        elif doc == "memoryStats":
            self.add_memory_parameters(jolokiaclient, dict_jmx)
        elif doc == "threadStats":
            self.add_threading_parameters(jolokiaclient, dict_jmx)
        elif doc == "gcStats":
            self.add_gc_parameters(jolokiaclient, dict_jmx)
        elif doc == "classLoadingStats":
            self.add_classloading_parameters(jolokiaclient, dict_jmx)
        elif doc == "compilationStats":
            self.add_compilation_parameters(jolokiaclient, dict_jmx)
        elif doc == "nioStats":
            self.add_nio_parameters(jolokiaclient, dict_jmx)
        elif doc == "operatingSysStats":
            self.add_operating_system_parameters(jolokiaclient, dict_jmx)
        elif doc == "zookeeperStats":
            self.add_zookeeper_parameters(jolokiaclient, dict_jmx)
        elif doc == "jmxStats":
            self.add_jmxstats_parameters(jolokiaclient, dict_jmx)

    def add_default_rate_value(self, dict_jmx):
        """Add default value to rate key based on type"""
        keylist = ["packetsReceivedRate", "packetsSentRate"]
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
        if rate < 0:
            rate = 0
        return rate

    def add_rate(self, pid, dict_jmx):
        """Rate only for zookeeper jmx metrics"""
        rate = self.get_rate("packetsReceived", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["packetsReceivedRate"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("packetsSent", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["packetsSentRate"] = round(rate, FLOATING_FACTOR)

    def add_rate_dispatch(self, pid, doc, dict_jmx):
        """Add default for rate keys in first poll."""
        if pid in self.prev_data:
            self.add_rate(pid, dict_jmx)
        else:
            self.add_default_rate_value(dict_jmx)
        self.prev_data[pid] = dict_jmx
        self.dispatch_data(doc, deepcopy(dict_jmx))

    def get_memory_pool_names(self, jolokiaclient):
        """Get memory pool names of jvm process"""
        mempool_json = jolokiaclient.request(type='read', mbean='java.lang:type=MemoryPool,*', attribute='Name')
        mempool_names = []
        if mempool_json['status'] == 200:
            for _, value in mempool_json['value'].items():
                if value['Name'] in DEFAULT_MP:
                    mempool_names.append(value['Name'])
                else:
                    collectd.error("Plugin zookeeperjmx: not supported for memory pool %s" % value['Name'])
        return mempool_names

    def get_gc_names(self, jolokiaclient):
        gc_json = jolokiaclient.request(type='read', mbean='java.lang:type=GarbageCollector,*', attribute='Name')
        gc_names = []
        if gc_json['status'] == 200:
            for _, value in gc_json['value'].items():
                if value['Name'] in DEFAULT_GC:
                    gc_names.append(value['Name'])
                else:
                    collectd.error("Plugin zookeeperjmx: not supported for GC %s" % value['Name'])
        return gc_names

    def add_jmxstats_parameters(self, jolokiaclient, dict_jmx):
        """Add specific jmxstats parameter"""
        #classloading Stats
        classloading = jolokiaclient.request(type='read', mbean='java.lang:type=ClassLoading')
        if classloading['status'] == 200:
            dict_jmx['unloadedClass'] = classloading['value']['UnloadedClassCount']
            dict_jmx['loadedClass'] = classloading['value']['LoadedClassCount']

        #threading Stats
        thread_json = jolokiaclient.request(type='read', mbean='java.lang:type=Threading')
        if thread_json['status'] == 200:
            dict_jmx['threads'] = thread_json['value']['ThreadCount']

        #memory Stats
        memory_json = jolokiaclient.request(type='read', mbean='java.lang:type=Memory')
        if memory_json['status'] == 200:
            heap = memory_json['value']['HeapMemoryUsage']
            self.handle_neg_bytes(heap['init'], 'heapMemoryUsageInit', dict_jmx)
            dict_jmx['heapMemoryUsageUsed'] = round(heap['used'] / 1024.0/ 1024.0, 2)
            dict_jmx['heapMemoryUsageCommitted'] = round(heap['committed'] / 1024.0 /1024.0, 2)
            non_heap = memory_json['value']['NonHeapMemoryUsage']
            self.handle_neg_bytes(non_heap['init'], 'nonHeapMemoryUsageInit', dict_jmx)
            dict_jmx['nonHeapMemoryUsageUsed'] = round(non_heap['used'] / 1024.0/ 1024.0, 2)
            dict_jmx['nonHeapMemoryUsageCommitted'] = round(non_heap['committed'] / 1024.0/ 1024.0, 2)

        #initailize default values
        param_list = ['G1OldGenerationCollectionTime', 'G1OldGenerationCollectionCount', 'G1YoungGenerationCollectionTime',\
                      'G1YoungGenerationCollectionCount', 'G1OldGenUsageUsed', 'G1SurvivorSpaceUsageUsed', 'MetaspaceUsageUsed',\
                      'CodeCacheUsageUsed', 'CompressedClassSpaceUsageUsed', 'G1EdenSpaceUsageUsed']
        for param in param_list:
            dict_jmx[param] = 0

        #gc Stats
        gc_names = self.get_gc_names(jolokiaclient)
        for gc_name in gc_names:
            str_mbean = 'java.lang:type=GarbageCollector,name='+gc_name
            valid = jolokiaclient.request(type='read', mbean=str_mbean, attribute='Valid')
            if valid['status'] == 200 and valid['value'] == True:
                str_attribute = 'CollectionTime,CollectionCount'
                gc_values = jolokiaclient.request(type='read', mbean=str_mbean, attribute=str_attribute)
                gc_name_no_spaces = ''.join(gc_name.split())
                if gc_values['status'] == 200:
                    dict_jmx[gc_name_no_spaces+'CollectionTime'] = round(gc_values['value']['CollectionTime'] * 0.001, 2)
                    dict_jmx[gc_name_no_spaces+'CollectionCount'] = gc_values['value']['CollectionCount']

        #memoryPool Stats
        mp_names = self.get_memory_pool_names(jolokiaclient)
        for pool_name in mp_names:
            str_mbean = 'java.lang:type=MemoryPool,name='+pool_name
            valid = jolokiaclient.request(type='read', mbean=str_mbean, attribute='Valid')
            if valid['status'] == 200 and valid['value'] == True:
                mp_values = jolokiaclient.request(type='read', mbean=str_mbean, attribute='Usage')
                pool_name_no_spaces = ''.join(pool_name.split())
                if mp_values['status'] == 200:
                    dict_jmx[pool_name_no_spaces+'UsageUsed'] = round(mp_values['value']['used'] / 1024.0/ 1024.0, 2)

    def get_zookeeper_info(self):
        "Getting info about zookeeper type whether it is a standalone or cluster"
        try:
            p1 = subprocess.Popen("echo srvr | nc localhost 2181", shell=True, stdout=subprocess.PIPE)
            output = p1.communicate()[0]
            if output:
                output_list = output.splitlines()
            for line in output_list:
                if "Mode" in line:
                    zootype_list = line.split(":")
                    zootype = zootype_list[1].strip()
            return zootype
        except Exception as e:
            collectd.error("Error in getting zookeeper info due to %s" % str(e))
            return None

    def get_zookeeper_id(self):
        "If zookeeper type is a cluster, get its own id and set its appropriate"
        try:
            p2 = subprocess.Popen("cat /opt/kafka/data/zookeeper/myid", shell=True, stdout=subprocess.PIPE)
            output = p2.communicate()[0]
            if output:
                return output
        except Exception as e:
            collectd.error("Error in getting zookeeper info due to %s" % str(e))
            return None

    def add_zookeeper_parameters(self, jolokiaClient, dict_jmx):
        "Getting info about zookeeper type whether it is a standalone or cluster"
        zookpertype = self.get_zookeeper_info()
        if zookpertype == "standalone":
            zookper = jolokiaClient.request(type='read',
                                            mbean='org.apache.ZooKeeperService:name0=StandaloneServer_port' + self.port)
            mbean_memory = 'org.apache.ZooKeeperService:name0=StandaloneServer_port%s,name1=InMemoryDataTree' % self.port
        else:
            zookpertype = zookpertype[0].upper() + zookpertype[1:]
            # If zookeeper type is a cluster, get its own id and set its appropriate
            zookperId = self.get_zookeeper_id()
            bean = 'org.apache.ZooKeeperService:name0=ReplicatedServer_id' + zookperId + ',name1=replica.' + zookperId + ',name2=' + zookpertype
            zookper = jolokiaClient.request(type='read', mbean=bean)
            mbean_memory = 'org.apache.ZooKeeperService:name0=ReplicatedServer_id' + zookperId + ',name1=replica.' + zookperId + \
                           ',name2=' + zookpertype + ',name3=InMemoryDataTree'

        if zookper['status'] == 200:
            dict_jmx['avgRequestLatency'] = round(zookper['value']['AvgRequestLatency'] * 0.001, 2)
            dict_jmx['maxSessionTimeout'] = round(zookper['value']['MaxSessionTimeout'] * 0.001, 2)
            dict_jmx['minSessionTimeout'] = round(zookper['value']['MinSessionTimeout'] * 0.001, 2)
            dict_jmx['maxClientCnxnsPerHost'] = zookper['value']['MaxClientCnxnsPerHost']
            dict_jmx['numAliveConnections'] = zookper['value']['NumAliveConnections']
            dict_jmx['outstandingRequests'] = zookper['value']['OutstandingRequests']
            dict_jmx['packetsReceived'] = zookper['value']['PacketsReceived']
            dict_jmx['packetsSent'] = zookper['value']['PacketsSent']
            dict_jmx['zookeeperVersion'] = ((zookper['value']['Version']).split(","))[0]
        zookper_count = jolokiaClient.request(type='read', mbean=mbean_memory)
        if zookper_count['status'] == 200:
            dict_jmx['nodeCount'] = zookper_count['value']['NodeCount']
            dict_jmx['watchCount'] = zookper_count['value']['WatchCount']
        zookper_ephemerals = jolokiaClient.request(type='exec', mbean=mbean_memory, operation='countEphemerals')
        if zookper_ephemerals['status'] == 200:
            dict_jmx['countEphemerals'] = zookper_ephemerals['value']
        zookper_datasize = jolokiaClient.request(type='exec', mbean=mbean_memory, operation='approximateDataSize')
        if zookper_datasize['status'] == 200:
            dict_jmx['approximateDataSize'] = round(zookper_datasize['value'] / 1024.0 / 1024.0, 2)

    def add_operating_system_parameters(self, jolokiaclient, dict_jmx):
        """Add operating system related jmx stats"""
        ops = jolokiaclient.request(type='read', mbean='java.lang:type=OperatingSystem')
        if ops['status'] == 200:
            dict_jmx['osArchitecture'] = ops['value']['Arch']
            dict_jmx['availableProcessors'] = ops['value']['AvailableProcessors']
            self.handle_neg_bytes(ops['value']['CommittedVirtualMemorySize'], 'committedVirtualMemorySize', dict_jmx)
            dict_jmx['freePhysicalMemorySize'] = round(ops['value']['FreePhysicalMemorySize'] / 1024.0 / 1024.0, 2)
            dict_jmx['freeSwapSpaceSize'] = round(ops['value']['FreeSwapSpaceSize']/ 1024.0 / 1024.0, 2)
            dict_jmx['maxFileDescriptors'] = ops['value']['MaxFileDescriptorCount']
            dict_jmx['osName'] = ops['value']['Name']
            dict_jmx['openFileDescriptors'] = ops['value']['OpenFileDescriptorCount']
            dict_jmx['processCpuLoad'] = ops['value']['ProcessCpuLoad']
            pcputime = ops['value']['ProcessCpuTime']
            if pcputime >= 0:
                pcputime = round(pcputime / 1000000000.0, 2)
            dict_jmx['processCpuTime'] = pcputime
            dict_jmx['totalPhysicalMemorySize'] = round(ops['value']['TotalPhysicalMemorySize']/ 1024.0 / 1024.0, 2)
            dict_jmx['totalSwapSpaceSize'] = round(ops['value']['TotalSwapSpaceSize']/ 1024.0 / 1024.0, 2)
            dict_jmx['osVersion'] = ops['value']['Version']
            dict_jmx['systemCpuLoad'] = ops['value']['SystemCpuLoad']
            dict_jmx['systemLoadAverage'] = ops['value']['SystemLoadAverage']

    def add_nio_parameters(self, jolokiaclient, dict_jmx):
        """Add network related jmx stats"""
        nio = jolokiaclient.request(type='read', mbean='java.nio:type=BufferPool,*', attribute='Name')
        bufferpool_names = []
        if nio['status'] == 200:
            for _, value in nio['value'].items():
                bufferpool_names.append(value['Name'])

        for poolname in bufferpool_names:
            str_mbean = 'java.nio:type=BufferPool,name='+poolname
            poolinfo = jolokiaclient.request(type='read', mbean=str_mbean)
            if poolinfo['status'] == 200:
                dict_jmx[poolname+'BufferPoolCount'] = poolinfo['value']['Count']
                self.handle_neg_bytes(poolinfo['value']['MemoryUsed'], poolname+'BufferPoolMemoryUsed', dict_jmx)
                self.handle_neg_bytes(poolinfo['value']['TotalCapacity'], poolname+'BufferPoolTotalCapacity', dict_jmx)

    def add_compilation_parameters(self, jolokiaclient, dict_jmx):
        """Add compilation related jmx stats"""
        compilation = jolokiaclient.request(type='read', mbean='java.lang:type=Compilation')
        if compilation['status'] == 200:
            dict_jmx['compilerName'] = compilation['value']['Name']
            dict_jmx['totalCompilationTime'] = round(compilation['value']['TotalCompilationTime'] * 0.001, 2)

    def add_classloading_parameters(self, jolokiaclient, dict_jmx):
        """Add classloading related jmx stats"""
        classloading = jolokiaclient.request(type='read', mbean='java.lang:type=ClassLoading')
        if classloading['status'] == 200:
            dict_jmx['unloadedClass'] = classloading['value']['UnloadedClassCount']
            dict_jmx['loadedClass'] = classloading['value']['LoadedClassCount']
            dict_jmx['totalLoadedClass'] = classloading['value']['TotalLoadedClassCount']

    def add_gc_parameters(self, jolokiaclient, dict_jmx):
        """Add garbage collector related jmx stats"""
        def memory_gc_usage(self, mempool_gc, key, gc_name, dict_jmx):
            for name, values in mempool_gc.items():
                if name in ['G1 Eden Space', 'G1 Old Gen']:
                    mp_name = ''.join(name.split())
                    self.handle_neg_bytes(values['init'], gc_name+key+mp_name+'Init', dict_jmx)
                    self.handle_neg_bytes(values['max'], gc_name+key+mp_name+'Max', dict_jmx)
                    dict_jmx[gc_name+key+mp_name+'Used'] = round(values['used'] /1024.0 /1024.0, 2)
                    dict_jmx[gc_name+key+mp_name+'Committed'] = round(values['committed'] /1024.0 /1024.0, 2)

        gc_names = self.get_gc_names(jolokiaclient)
        for gc_name in gc_names:
            str_mbean = 'java.lang:type=GarbageCollector,name='+gc_name
            if_valid = jolokiaclient.request(type='read', mbean=str_mbean, attribute='Valid')
            if if_valid['status'] == 200 and if_valid['value'] == True:
                str_attribute = 'CollectionTime,CollectionCount,LastGcInfo'
                gc_values = jolokiaclient.request(type='read', mbean=str_mbean, attribute=str_attribute)
                gc_name_no_spaces = ''.join(gc_name.split())
                if gc_values['status'] == 200:
                    dict_jmx[gc_name_no_spaces+'CollectionTime'] = round(gc_values['value']['CollectionTime'] * 0.001, 2)
                    dict_jmx[gc_name_no_spaces+'CollectionCount'] = gc_values['value']['CollectionCount']
                    if gc_values['value']['LastGcInfo']:
                        dict_jmx[gc_name_no_spaces+'GcThreadCount'] = gc_values['value']['LastGcInfo']['GcThreadCount']
                        dict_jmx[gc_name_no_spaces+'StartTime'] = round(gc_values['value']['LastGcInfo']['startTime'] * 0.001, 2)
                        dict_jmx[gc_name_no_spaces+'EndTime'] = round(gc_values['value']['LastGcInfo']['endTime'] * 0.001, 2)
                        dict_jmx[gc_name_no_spaces+'Duration'] = round(gc_values['value']['LastGcInfo']['duration'] * 0.001, 2)
                        mem_aftergc = gc_values['value']['LastGcInfo']['memoryUsageAfterGc']
                        memory_gc_usage(self, mem_aftergc, 'MemUsageAfGc', gc_name_no_spaces, dict_jmx)
                        mem_beforegc = gc_values['value']['LastGcInfo']['memoryUsageBeforeGc']
                        memory_gc_usage(self, mem_beforegc, 'MemUsageBfGc', gc_name_no_spaces, dict_jmx)

    def add_threading_parameters(self, jolokiaclient, dict_jmx):
        """Add thread related jmx stats"""
        mbean_threading = 'java.lang:type=Threading'
        thread_json = jolokiaclient.request(type='read', mbean=mbean_threading)
        if thread_json['status'] == 200:
            dict_jmx['threads'] = thread_json['value']['ThreadCount']
            dict_jmx['peakThreads'] = thread_json['value']['PeakThreadCount']
            dict_jmx['daemonThreads'] = thread_json['value']['DaemonThreadCount']
            dict_jmx['totalStartedThreads'] = thread_json['value']['TotalStartedThreadCount']
            if thread_json['value']['CurrentThreadCpuTimeSupported']:
                dict_jmx['currentThreadCpuTime'] = round(thread_json['value']['CurrentThreadCpuTime'] / 1000000000.0, 2)
                dict_jmx['currentThreadUserTime'] = round(thread_json['value']['CurrentThreadUserTime'] / 1000000000.0, 2)

    def add_memory_parameters(self, jolokiaclient, dict_jmx):
        """Add memory related jmx stats"""
        memory_json = jolokiaclient.request(type='read', mbean='java.lang:type=Memory')
        if memory_json['status'] == 200:
            heap = memory_json['value']['HeapMemoryUsage']
            self.handle_neg_bytes(heap['init'], 'heapMemoryUsageInit', dict_jmx)
            self.handle_neg_bytes(heap['max'], 'heapMemoryUsageMax', dict_jmx)
            dict_jmx['heapMemoryUsageUsed'] = round(heap['used'] / 1024.0/ 1024.0, 2)
            dict_jmx['heapMemoryUsageCommitted'] = round(heap['committed'] / 1024.0 /1024.0, 2)

            non_heap = memory_json['value']['NonHeapMemoryUsage']
            self.handle_neg_bytes(non_heap['init'], 'nonHeapMemoryUsageInit', dict_jmx)
            self.handle_neg_bytes(non_heap['max'], 'nonHeapMemoryUsageMax', dict_jmx)
            dict_jmx['nonHeapMemoryUsageUsed'] = round(non_heap['used'] / 1024.0/ 1024.0, 2)
            dict_jmx['nonHeapMemoryUsageCommitted'] = round(non_heap['committed'] / 1024.0/ 1024.0, 2)
            dict_jmx['objectPendingFinalization'] = memory_json['value']['ObjectPendingFinalizationCount']

    def add_memory_pool_parameters(self, jolokiaclient, dict_jmx):
        """Add memory pool related jmx stats"""
        mp_names = self.get_memory_pool_names(jolokiaclient)
        for pool_name in mp_names:
            str_mbean = 'java.lang:type=MemoryPool,name='+pool_name
            if_valid = jolokiaclient.request(type='read', mbean=str_mbean, attribute='Valid')
            if if_valid['status'] == 200 and if_valid['value'] == True:
                str_attribute = 'CollectionUsage,PeakUsage,Type,Usage,CollectionUsageThresholdSupported,UsageThresholdSupported'
                mp_values = jolokiaclient.request(type='read', mbean=str_mbean, attribute=str_attribute)
                pool_name_no_spaces = ''.join(pool_name.split())
                if mp_values['status'] == 200:
                    coll_usage = mp_values['value']['CollectionUsage']
                    if coll_usage:
                        self.handle_neg_bytes(coll_usage['max'], pool_name_no_spaces+'CollectionUsageMax', dict_jmx)
                        self.handle_neg_bytes(coll_usage['init'], pool_name_no_spaces+'CollectionUsageInit', dict_jmx)
                        dict_jmx[pool_name_no_spaces+'CollectionUsageUsed'] = round(coll_usage['used'] /1024.0/1024.0, 2)
                        dict_jmx[pool_name_no_spaces+'CollectionUsageCommitted'] = round(coll_usage['committed'] /1024.0/1024.0, 2)
                    usage = mp_values['value']['Usage']
                    self.handle_neg_bytes(usage['max'], pool_name_no_spaces+'UsageMax', dict_jmx)
                    self.handle_neg_bytes(usage['init'], pool_name_no_spaces+'UsageInit', dict_jmx)
                    dict_jmx[pool_name_no_spaces+'UsageUsed'] = round(usage['used'] / 1024.0/ 1024.0, 2)
                    dict_jmx[pool_name_no_spaces+'UsageCommitted'] = round(usage['committed'] / 1024.0/ 1024.0, 2)
                    peak_usage = mp_values['value']['PeakUsage']
                    self.handle_neg_bytes(peak_usage['max'], pool_name_no_spaces+'PeakUsageMax', dict_jmx)
                    self.handle_neg_bytes(peak_usage['init'], pool_name_no_spaces+'PeakUsageInit', dict_jmx)
                    dict_jmx[pool_name_no_spaces+'PeakUsageUsed'] = round(peak_usage['used'] / 1024.0/ 1024.0, 2)
                    dict_jmx[pool_name_no_spaces+'PeakUsageCommitted'] = round(peak_usage['committed'] / 1024.0/ 1024.0, 2)
                    if mp_values['value']['CollectionUsageThresholdSupported']:
                        coll_attr = 'CollectionUsageThreshold,CollectionUsageThresholdCount,CollectionUsageThresholdExceeded'
                        coll_threshold = jolokiaclient.request(type='read', mbean=str_mbean, attribute=coll_attr)
                        if coll_threshold['status'] == 200:
                            dict_jmx[pool_name_no_spaces+'CollectionUsageThreshold'] = round(coll_threshold['value']['CollectionUsageThreshold'] / 1024.0 / 1024.0, 2)
                            dict_jmx[pool_name_no_spaces+'CollectionUsageThresholdCount'] = coll_threshold['value']['CollectionUsageThreshold']
                            dict_jmx[pool_name_no_spaces+'CollectionUsageThresholdExceeded'] = coll_threshold['value']['CollectionUsageThresholdExceeded']
                    if mp_values['value']['UsageThresholdSupported']:
                        usage_attr = 'UsageThreshold,UsageThresholdCount,UsageThresholdExceeded'
                        usage_threshold = jolokiaclient.request(type='read', mbean=str_mbean, attribute=usage_attr)
                        if usage_threshold['status'] == 200:
                            dict_jmx[pool_name_no_spaces+'UsageThreshold'] = round(usage_threshold['value']['UsageThreshold'] / 1024.0 / 1024.0, 2)
                            dict_jmx[pool_name_no_spaces+'UsageThresholdCount'] = usage_threshold['value']['UsageThreshold']
                            dict_jmx[pool_name_no_spaces+'UsageThresholdExceeded'] = usage_threshold['value']['UsageThresholdExceeded']

    def handle_neg_bytes(self, value, resultkey, dict_jmx):
        """Condition for byte keys whose return value may be -1 if not supported."""
        if value == -1:
            dict_jmx[resultkey] = value
        else:
            dict_jmx[resultkey] = round(value / 1024.0/ 1024.0, 2)

    def add_common_params(self, doc, dict_jmx):
        """Adds TIMESTAMP, PLUGIN, PLUGITYPE to dictionary."""
        timestamp = int(round(time.time() * 1000))
        dict_jmx[TIMESTAMP] = timestamp
        dict_jmx[PLUGIN] = ZOOK_JMX
        dict_jmx[PLUGINTYPE] = doc
        dict_jmx[ACTUALPLUGINTYPE] = ZOOK_JMX
        #dict_jmx[PLUGIN_INS] = doc
        collectd.info("Plugin zookeeperjmx: Added common parameters successfully for %s doctype" % doc)

    def get_pid_jmx_stats(self, pid, port, output):
        """Call get_jmx_parameters function for each doc_type and add dict to queue"""
        jolokiaclient = self.jclient.get_jolokia_inst(port)
        for doc in ZOOK_DOCS:
            try:
                dict_jmx = {}
                self.get_jmx_parameters(jolokiaclient, doc, dict_jmx)
                if not dict_jmx:
                    raise ValueError("No data found")

                collectd.info("Plugin zookeeperjmx: Added %s doctype information successfully for pid %s" % (doc, pid))
                self.add_common_params(doc, dict_jmx)
                output.put((pid, doc, dict_jmx))
            except Exception as err:
                collectd.error("Plugin zookeeperjmx: Error in collecting stats of %s doctype: %s" % (doc, str(err)))

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
            collectd.error("Plugin zookeeperjmx: No %s processes are running" % self.process)
            return

        procs, output = self.run_pid_process(list_pid)
        for _ in procs:
            for _ in ZOOK_DOCS:
                try:
                    pid, doc_name, doc_result = output.get_nowait()
                except Queue.Empty:
                    collectd.error("Failed to send one or more doctype document to collectd")
                    continue
                # Dispatching documentsTypes which are requetsed alone
                if doc_name in self.documentsTypes:
                    if doc_name == "zookeeperStats":
                        self.add_rate_dispatch(pid, doc_name, doc_result)
                    else:
                        self.dispatch_data(doc_name, doc_result)
        output.close()

    def dispatch_data(self, doc_name, result):
        """Dispatch data to collectd."""
        if doc_name == "zookeeperStats":
            for item in ["packetsSent", "packetsReceived"]:
                del result[item]

        collectd.info("Plugin zookeeperjmx: Succesfully sent %s doctype to collectd." % doc_name)
        collectd.debug("Plugin zookeeperjmx: Values dispatched =%s" % json.dumps(result))
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
