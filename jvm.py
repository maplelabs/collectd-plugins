"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
"""
Collectd Python plugin to get JVM stats [pid, classes, threads, cpu usage, ram usage,
heap size, heap usage, Process state]
"""

import subprocess
import collectd
from constants import *
from utils import *
import time
import os
import shutil
class JVM(object):
    """Plugin object will be created only once and collects jvm statistics info every interval."""

    def __init__(self):
        """Initializes interval and previous dictionary variable."""
        self.interval = 0
        self.process = None

    def read_config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == PROCESS:
                self.process = children.values[0]

    def get_ramusage(self, pid):
        """Returns RAM usage in MB"""
        call = subprocess.Popen("ps aux | grep '%s'" % pid, shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (ramusage, err) = call.communicate()
        if err:
            collectd.debug("Error: %s" % err)
            return -1
        usage = ramusage.split("\n")
        ramusage = -1
        for ram in usage:
            if ram is not "":
                ramusage = ram.split()
                if ramusage[1] == pid:
                    ramusage = (float(ramusage[5])) / 1024
                    break
        return ramusage

    def get_cpuusage(self, pid):
        """Returns cpu utilization, CLK_TCK, utime, and stime"""
        call = subprocess.Popen("ps aux | grep %s" % pid, shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (cpuusage, err) = call.communicate()
        if err:
            collectd.debug("Error: %s" % err)
            return -1, -1, -1, -1
        usage = cpuusage.split("\n")
        for cpu in usage:
            if cpu is not "":
                cpu = cpu.split()
                if cpu[1] == pid:
                    break

        call = subprocess.Popen("getconf -a | grep CLK_TCK", shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (clk_tick, err) = call.communicate()
        if err:
            collectd.debug("Error: %s" % err)
            return -1, -1, -1, -1
        clk_tick = clk_tick.split()
        try:
            fileobj = open('/proc/%d/stat' % (int(pid)))
        except:
            collectd.debug("Error: Unable to open /proc/%d/stat" % (int(pid)))
            return -1, -1, -1, -1
        lines = fileobj.readlines()
        for line in lines:
            line = line.split()
            utime = line[13]
            stime = line[14]
            cpuval = cpu[2]
            return cpuval, utime, stime, clk_tick[1]
    def remove_inactive_pids(self,active_pids):
        for root, dirs, files in os.walk(JVM_DATA_PATH):
            for d in dirs:
                if d not in active_pids:
                    shutil.rmtree(JVM_DATA_PATH+ "/"+ d)
    def get_pid(self, process_name):
        """Returns pid for JVM process"""
	collectd.info("jvm: getting pids")
        call = subprocess.Popen("sudo jcmd | grep -E '%s'|grep -v JCmd " % (
            process_name), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (pids, err) = call.communicate()
        if err:
            collectd.info("jvm Error: %s" % err)
            return None
        pids = pids.split("\n")
        pid_list = []
        pName_list = []
        for pid in pids:
            collectd.info("pids value : %s" % pid)
            if pid is not "":
                collectd.info("pids value indside: %s" % pid)
                pidval = pid.split()
                collectd.info("pidxs value indside: %s" % pidval[0])
                collectd.info("pid names value indside: %s" % pidval[1])
                pid_list.append(pidval[0])
                pName_list.append(pidval[1])
        return pid_list,pName_list

    def get_jvmstatistics(self, pid, state, process_name):
        """Returns a list containg JVM stats no.of threads, class, heap usage, ram usage"""
	collectd.info("jvm: Getting jvm statistics")
        jvm_res = {}
        fileobj = open('/proc/%d/status' % (int(pid)))
        if fileobj is None:
            collectd.debug(
                "Error: Unable to open /proc/%d/status" % (int(pid)))
            return
        lines = fileobj.readlines()
        for line in lines:
            if line.startswith("Threads:"):
                num_threads = line.split()
                num_threads = num_threads[1]
                break

        numof_classloaded = "%s %s" % ('jstat -class', pid)
        call = subprocess.Popen(numof_classloaded, shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (classes, err) = call.communicate()
        if err:
            collectd.info("Error: %s" % err)
            return
        classes = classes.split()

        heapsize = 'java -XX:+PrintFlagsFinal -version | grep -iE MaxHeapSize'
        call = subprocess.Popen("%s" % heapsize, shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (heapsize, err) = call.communicate()
        #heapsize = heapsize.split("\n")
        #heapsize = heapsize[0].split(":=")
        #heapsize = heapsize[1].split(" ")
        #heapsize = (float(heapsize[1])) / (1024 * 1024)
        heapsize = heapsize.split()
        heapsize = (float(heapsize[3])) / (1024 * 1024)
        
        call = subprocess.Popen("jstat -gc %s" % int(pid), shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)


        (heapusage, err) = call.communicate()
        if err:
            collectd.debug("Error: Jstat does not give correct output ")
            return

        heapusage = heapusage.split("\n")
        heapusage = str(heapusage[1]).split()

        heapusageValue = 0
        try:
            collectd.debug("Calculating heapUsage by evaluating S0U.")
            heapusageValue += float(heapusage[2])
        except:
            collectd.debug("Error calculating heapUsage by evaluating S0U.")

        try:
            collectd.debug("Calculating heapUsage by evaluating S1U.")
            heapusageValue += float(heapusage[3])
        except:
            collectd.debug("Error calculating heapUsage by evaluating S1U.")

        try:
            collectd.debug("Calculating heapUsage by evaluating EU.")
            heapusageValue += float(heapusage[5])
        except:
            collectd.debug("Error calculating heapUsage by evaluating EU.")

        try:
            collectd.debug("Calculating heapUsage by evaluating OU.")
            heapusageValue += float(heapusage[7])
        except:
            collectd.debug("Error calculating heapUsage by evaluating OU.")

        #try:
        #    collectd.debug("Calculating heapUsage by evaluating MU.")
        #    heapusageValue += float(heapusage[9])
        #except:
        #    collectd.debug("Error calculating heapUsage by evaluating MU.")

        try:
            collectd.debug("Calculating heapUsage by evaluating CCSU.")
            heapusageValue += float(heapusage[11])
        except:
            collectd.debug("Error calculating heapUsage by evaluating CCSU.")

        # heapusageValue = ((float(heapusage[2]) + float(heapusage[3]) + float(heapusage[
        #     5]) + float(heapusage[7]) + float(heapusage[9]) + float(heapusage[11])) / 1024)

        #Collect information about Garbage collection
        try:
            collectd.debug("Calculating GCT.")
            gct = float(heapusage[16])
        except:
            collectd.debug("Error calculating GCT.")
            gct = 0

        try:
            collectd.debug("Calculating YGC.")
            ygc = float(heapusage[12])
        except:
            collectd.debug("Error calculating YGC.")
            ygc = 0

        try:
            collectd.debug("Calculating FGC.")
            fgc = int(heapusage[14])
        except:
            collectd.debug("Error calculating FGC.")
            fgc = 0

        # gct = float(heapusage[16])
        # ygc = int(heapusage[12])
        # fgc = int(heapusage[14])
        tgc = ygc + fgc

        ram_usage = self.get_ramusage(pid)
        if ram_usage == -1:
            return

        cpu_usage, utime, stime, clk_tick = self.get_cpuusage(pid)
        if cpu_usage == -1:
            return

        jvm_res["numThreads"] = int(num_threads)
        jvm_res["numLoadedClasses"] = int(classes[5])
        jvm_res["numUnloadedClasses"] = int(classes[7])
        jvm_res["heapSize"] = float(heapsize)
        jvm_res["heapUsage"] = float(heapusageValue)/1024
        jvm_res["ramUsage"] = float(ram_usage)
        jvm_res["cpuUsage"] = float(cpu_usage)
        jvm_res["pid"] = int(pid)
        jvm_res["stime"] = float(stime)
        jvm_res["utime"] = float(utime)
        jvm_res["clockTick"] = int(clk_tick)
        jvm_res["gct"] = gct
        jvm_res["survivor_0"] = float(heapusage[2])
        jvm_res["survivor_1"] = float(heapusage[3])
        jvm_res["eden"] = float(heapusage[5])
        jvm_res["old"] = float(heapusage[7])
        jvm_res["permanent"] = float(heapusage[9])
        jvm_res["gc"] = int(heapusage[14])
      
        self.add_common_params(jvm_res, state, pid, process_name)
        self.dispatch_data(jvm_res)

    def add_common_params(self,  jvm_dict, state, pid, process_name):
        hostname = gethostname()
        timestamp = int(round(time.time() * 1000))
        jvm_dict[HOSTNAME] = hostname
        jvm_dict[TIMESTAMP] = timestamp
        jvm_dict[PLUGIN] = "jvm"
        jvm_dict[PLUGINTYPE] = JVM_STATS
        jvm_dict[ACTUALPLUGINTYPE] = JVM_STATS
        jvm_dict[PLUGIN_INS] = str(pid)
        #jvm_dict[TYPE] = "jvmStatic"
        jvm_dict[INTERVAL] = int(self.interval)
        jvm_dict[PROCESS_STATE] = state
        jvm_dict["_processName"] = process_name

    @staticmethod
    def dispatch_data(jvm_dict):
	collectd.info("jvm: Values dispatched: %s" % str(jvm_dict))
        dispatch(jvm_dict)

    def get_jvmstate(self):
        """Get the state of jvm process"""
	collectd.info("jvm: get the state of jvm")
        for process_name in self.process.split(','):
            pids,pNames = self.get_pid(process_name)
            collectd.info( "pids +++ : %s" % pids)
            collectd.info( "pNames +++ : %s" % pNames)
            self.remove_inactive_pids(pids)   
            if not pids:
                collectd.info("No JAVA process are running")
                return
            #for pid in pids:
            for (pid,pname) in zip(pids,pNames):
                collectd.info(pid)
                process_name = pname
                collectd.info(pname)
                #process_name = os.system("ps -p ",int(15903), "-o comm=")
                try:
                    fileobj = open('/proc/%d/status' % (int(pid)))
                except:
                    collectd.info("PID got changed for the process")
                    continue
                lines = fileobj.readlines()
                for line in lines:
                    if line.startswith("State:"):
                        state = (line.split())[2]
                        state = state.strip("()")
                        self.get_jvmstatistics(pid, state, process_name)
                        break

    def read_temp(self):
        """Collectd first calls register_read. At that time default interval is taken,
        hence temporary function is made to call, the read callback is unregistered
        and read() is called again with interval obtained from conf by register_config callback."""
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.get_jvmstate, interval=int(self.interval))

OBJ = JVM()
collectd.register_config(OBJ.read_config)
collectd.register_read(OBJ.read_temp)

