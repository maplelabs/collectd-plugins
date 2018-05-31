"""Python plugin for collectd to fetch kafka_jmx for process given."""

#!/usr/bin/python
import os
import subprocess
import re
import signal
import json
import time
import collectd
import requests
import socket
import Queue
import multiprocessing
from pyjolokia import Jolokia
from contextlib import closing
# user imports
import utils
from constants import *

GENERIC_DOCS = ["memoryPoolStats", "memoryStats", "threadStats", "gcStats", "classLoadingStats",
        "compilationStats", "nioStats", "operatingSysStats"]

JOLOKIA_PATH = "/opt/collectd/plugins/"
DEFAULT_GC = ['G1 Old Generation', 'G1 Young Generation']

class JmxStat(object):
    """Plugin object will be created only once and collects JMX statistics info every interval."""

    def __init__(self):
        """Initializes interval and previous dictionary variable."""
        self.interval = DEFAULT_INTERVAL
        self.process = None

    def config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == PROCESS:
                self.process = children.values[0]

    def get_cmd_output(self, cmd, shell_value=True, stdout_value=subprocess.PIPE,
                       stderr_value=subprocess.PIPE):
        """Returns subprocess output and return code of cmd passed in argument."""
        call = subprocess.Popen(cmd, shell=shell_value,
                                stdout=stdout_value, stderr=stderr_value)
        call.wait()
        status, err = call.communicate()
        returncode = call.returncode
        return status, err, returncode

    def check_prerequiste(self):
        """Need to run plugin as root."""
        if not os.geteuid() == 0:
            collectd.error("Please run collectd as root. Jmx_stats plugin requires root privileges")
            return False
        return True

    @staticmethod
    def get_free_port():
        """To get free port to run jolokia client."""
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sockt:
            sockt.bind(('localhost', 0))
            return sockt.getsockname()[1]

    def get_pid(self):
        """Get PIDs of all java process."""
        pid_cmd = "jcmd | awk '{print $1 \" \" $2}' | grep -w \"%s\"" % self.process
        pids, err = utils.get_cmd_output(pid_cmd)
        if err:
            collectd.error("Plugin kafka_jmx: Error in collecting pid: %s" % err)
            return False
        pids = pids.splitlines()
        pid_list = []
        for pid in pids:
            if pid is not "":
                pidval = pid.split()
                pid_list.append(pidval[0])
        return pid_list

    def get_uid(self, pid):
        """Jolokia needs to be run with same user of the process attached"""
        uid_cmd = "awk '/^Uid:/{print $2}' /proc/%s/status" % pid
        uid, err = utils.get_cmd_output(uid_cmd)
        if err:
            collectd.error("Plugin kafka_jmx:Failed to retrieve uid for pid %s, %s" % (pid, err))
            return False
        return uid.strip()

    def run_jolokia_cmd(self, cmd, pid, port=None):
        """Common logic to run jolokia cmds."""
        process_uid = self.get_uid(pid)
        jolokia_cmd = "sudo -u '#{0}' java -jar {1}jolokia.jar --host=127.0.0.1 {2} {3}".format(process_uid, JOLOKIA_PATH, cmd, pid)
        if port:
            jolokia_cmd += " --port=%s" % port
        return self.get_cmd_output(jolokia_cmd)

    def get_pid_port(self, pid):
        """check if jmx jolokia agent already running, if running get port"""
        status, err, ret = self.run_jolokia_cmd("status", pid)
        if err or ret:
            port = self.get_free_port()
            status, err, ret = self.run_jolokia_cmd("start", pid, port)
            if err or ret:
                collectd.error("Plugin kafka_jmx: Unable to start jolokia client for pid %s, %s" % (pid, err))
                return False
            collectd.info("Plugin kafka_jmx: started jolokia client for pid %s" % pid)
            return port

        # jolokia id already started and return port
        joloip = status.splitlines()[1]
        port = re.findall('\d+', joloip.split(':')[2])[0]
        collectd.debug("Plugin kafka_jmx: jolokia client is already running for pid %s" % pid)
        return port

    def connection_available(self, port):
        """Check if jolokia client is up."""
        try:
            jolokia_url = "http://127.0.0.1:%s/jolokia/version" % port
            resp = requests.get(jolokia_url)
            if resp.status_code == 200:
                return True
        except requests.exceptions.ConnectionError:
            return False

    def get_jmx_parameters(self, port, doc, dict_jmx):
        """Fetch stats based on doc_type"""
        if not self.connection_available(port):
            return None
        jolokia_url = "http://127.0.0.1:%s/jolokia/" % port
        jolokiaclient = Jolokia(jolokia_url)
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
        if not bufferpool_names:
            return

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

        gc_json = jolokiaclient.request(type='read', mbean='java.lang:type=GarbageCollector,*', attribute='Name')
        if gc_json['status'] == 200:
            gc_names = []
            for _, value in gc_json['value'].items():
                if value['Name'] in DEFAULT_GC:
                    gc_names.append(value['Name'])
                else:
                    collectd.error("Plugin kafka_jmx: not supported for GC %s" % value['Name'])
        if not gc_names:
            return

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

    def get_memory_pool_names(self, jolokiaclient):
        """Get memory pool names of jvm process"""
        mempool_json = jolokiaclient.request(type='read', mbean='java.lang:type=MemoryPool,*', attribute='Name')
        mempool_names = []
        if mempool_json['status'] == 200:
            for _, value in mempool_json['value'].items():
                mempool_names.append(value['Name'])
        return mempool_names

    def add_memory_pool_parameters(self, jolokiaclient, dict_jmx):
        """Add memory pool related jmx stats"""
        mp_names = self.get_memory_pool_names(jolokiaclient)
        if not mp_names:
            return

        for poll_name in mp_names:
            str_mbean = 'java.lang:type=MemoryPool,name='+poll_name
            if_valid = jolokiaclient.request(type='read', mbean=str_mbean, attribute='Valid')
            if if_valid['status'] == 200 and if_valid['value'] == True:
                str_attribute = 'CollectionUsage,PeakUsage,Type,Usage,CollectionUsageThresholdSupported,UsageThresholdSupported'
                mp_values = jolokiaclient.request(type='read', mbean=str_mbean, attribute=str_attribute)
                poll_name_no_spaces = ''.join(poll_name.split())
                if mp_values['status'] == 200:
                    coll_usage = mp_values['value']['CollectionUsage']
                    if coll_usage:
                        self.handle_neg_bytes(coll_usage['max'], poll_name_no_spaces+'CollectionUsageMax', dict_jmx)
                        self.handle_neg_bytes(coll_usage['init'], poll_name_no_spaces+'CollectionUsageInit', dict_jmx)
                        dict_jmx[poll_name_no_spaces+'CollectionUsageUsed'] = round(coll_usage['used'] /1024.0/1024.0, 2)
                        dict_jmx[poll_name_no_spaces+'CollectionUsageCommitted'] = round(coll_usage['committed'] /1024.0/1024.0, 2)
                    usage = mp_values['value']['Usage']
                    self.handle_neg_bytes(usage['max'], poll_name_no_spaces+'UsageMax', dict_jmx)
                    self.handle_neg_bytes(usage['init'], poll_name_no_spaces+'UsageInit', dict_jmx)
                    dict_jmx[poll_name_no_spaces+'UsageUsed'] = round(usage['used'] / 1024.0/ 1024.0, 2)
                    dict_jmx[poll_name_no_spaces+'UsageCommitted'] = round(usage['committed'] / 1024.0/ 1024.0, 2)
                    peak_usage = mp_values['value']['PeakUsage']
                    self.handle_neg_bytes(peak_usage['max'], poll_name_no_spaces+'PeakUsageMax', dict_jmx)
                    self.handle_neg_bytes(peak_usage['init'], poll_name_no_spaces+'PeakUsageInit', dict_jmx)
                    dict_jmx[poll_name_no_spaces+'PeakUsageUsed'] = round(peak_usage['used'] / 1024.0/ 1024.0, 2)
                    dict_jmx[poll_name_no_spaces+'PeakUsageCommitted'] = round(peak_usage['committed'] / 1024.0/ 1024.0, 2)
                    if mp_values['value']['CollectionUsageThresholdSupported']:
                        coll_attr = 'CollectionUsageThreshold,CollectionUsageThresholdCount,CollectionUsageThresholdExceeded'
                        coll_threshold = jolokiaclient.request(type='read', mbean=str_mbean, attribute=coll_attr)
                        if coll_threshold['status'] == 200:
                            dict_jmx[poll_name_no_spaces+'CollectionUsageThreshold'] = round(coll_threshold['value']['CollectionUsageThreshold'] / 1024.0 / 1024.0, 2)
                            dict_jmx[poll_name_no_spaces+'CollectionUsageThresholdCount'] = coll_threshold['value']['CollectionUsageThreshold']
                            dict_jmx[poll_name_no_spaces+'CollectionUsageThresholdExceeded'] = coll_threshold['value']['CollectionUsageThresholdExceeded']
                    if mp_values['value']['UsageThresholdSupported']:
                        usage_attr = 'UsageThreshold,UsageThresholdCount,UsageThresholdExceeded'
                        usage_threshold = jolokiaclient.request(type='read', mbean=str_mbean, attribute=usage_attr)
                        if usage_threshold['status'] == 200:
                            dict_jmx[poll_name_no_spaces+'UsageThreshold'] = round(usage_threshold['value']['UsageThreshold'] / 1024.0 / 1024.0, 2)
                            dict_jmx[poll_name_no_spaces+'UsageThresholdCount'] = usage_threshold['value']['UsageThreshold']
                            dict_jmx[poll_name_no_spaces+'UsageThresholdExceeded'] = usage_threshold['value']['UsageThresholdExceeded']

    def handle_neg_bytes(self, value, resultkey, dict_jmx):
        """Condition for byte keys whose return value may be -1 if not supported."""
        if value == -1:
            dict_jmx[resultkey] = value
        else:
            dict_jmx[resultkey] = round(value / 1024.0/ 1024.0, 2)

    def add_common_params(self, doc, dict_jmx):
        """Adds TIMESTAMP, PLUGIN, PLUGITYPE to dictionary."""
        timestamp = int(round(time.time()))
        dict_jmx[TIMESTAMP] = timestamp
        dict_jmx[PLUGIN] = KAFKA_JMX
        dict_jmx[PLUGINTYPE] = doc
        dict_jmx[ACTUALPLUGINTYPE] = KAFKA_JMX
        dict_jmx[PROCESSNAME] = self.process
        dict_jmx[PLUGIN_INS] = doc
        collectd.info("Plugin kafka_jmx: Added common parameters successfully")

    def get_pid_jmx_stats(self, pid, port, output):
        """Call get_jmx_parameters function for each doc_type and add dict to queue"""
        for doc in GENERIC_DOCS:
            try:
                dict_jmx = {}
                self.get_jmx_parameters(port, doc, dict_jmx)
                if not dict_jmx:
                    raise Exception("No data found")

                collectd.info("Plugin kafka_jmx: Added doctype %s of pid %s information successfully" % (doc, pid))
                dict_jmx['_processPid'] = pid
                self.add_common_params(doc, dict_jmx)
                output.put((doc, dict_jmx))
            except Exception as err:
                collectd.error("Plugin kafka_jmx: Error in collecting stats of doctype %s: %s" % (doc, str(err)))

    def run_pid_process(self, list_pid):
        """Spawn process for each pid"""
        procs = []
        output = multiprocessing.Queue()
        for pid in list_pid:
            port = self.get_pid_port(pid)
            if port:
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
        if not self.check_prerequiste():
            return

        list_pid = self.get_pid()
        if not list_pid:
            collectd.error("Plugin kafka_jmx: No %s processes are running" % self.process)
            return

        procs, output = self.run_pid_process(list_pid)
        for _ in procs:
            for _ in GENERIC_DOCS:
                try:
                    doc_name, doc_result = output.get_nowait()
                except Queue.Empty:
                    collectd.error("Failed to send one or more doctype document to collectd")
                    continue
                self.dispatch_data(doc_name, doc_result)
        output.close()

    def dispatch_data(self, doc_name, result):
        """Dispatch data to collectd."""
        collectd.info("Plugin kafka_jmx: Succesfully sent doctype %s to collectd." % doc_name)
        collectd.debug("Plugin kafka_jmx: Values dispatched =%s" % json.dumps(result))
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
