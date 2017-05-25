'''
Collectd Python plugin to get JVM stats [pid, classes, threads, cpu usage, ram usage,
heap size, heap usage, Process state]
'''

import subprocess
import collectd


class JVM(object):
    """Plugin object will be created only once and collects jvm statistics info every interval."""

    def __init__(self):
        """Initializes interval and previous dictionary variable."""
        self.interval = 0
        self.process = None

    def read_config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == 'interval':
                self.interval = children.values[0]
            if children.key == 'process':
                self.process = children.values[0]

    def get_ramusage(self, pid):
        """Returns RAM usage in MB"""
        call = subprocess.Popen("ps aux | grep %s" % pid, shell=True,
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
                    ramusage = (float(ramusage[5])) / (1024)
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

        fileobj = open('/proc/%d/stat' % (int(pid)))
        if fileobj is None:
            collectd.debug("Error: Unable to open /proc/%d/stat" % (int(pid)))
            return -1, -1, -1, -1
        lines = fileobj.readlines()
        for line in lines:
            line = line.split()
            utime = line[13]
            stime = line[14]
            cpuval = cpu[2]
            return cpuval, utime, stime, clk_tick[1]

    def get_pid(self):
        """Returns pid for JVM process"""
        call = subprocess.Popen("jcmd | grep %s" % (
            self.process), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (pids, err) = call.communicate()
        if err:
            collectd.debug("Error: %s" % err)
            return None
        pids = pids.split("\n")
        pid_list = []
        for pid in pids:
            if pid is not "":
                pidval = pid.split()
                pid_list.append(pidval[0])
        return pid_list

    def get_jvmstatistics(self, pid, state):
        """Returns a list containg JVM stats no.of threads, class, heap usage, ram usage"""
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
            collectd.debug("Error: %s" % err)
            return
        classes = classes.split()

        heapsize = 'java -XX:+PrintFlagsFinal -version | grep -iE MaxHeapSize'
        call = subprocess.Popen("%s" % (heapsize), shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (heapsize, err) = call.communicate()
        heapsize = heapsize.split("\n")
        heapsize = heapsize[0].split(":=")
        heapsize = heapsize[1].split(" ")
        heapsize = (float(heapsize[1])) / (1024 * 1024)

        call = subprocess.Popen("jstat -gc %s" % int(pid), shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (heapusage, err) = call.communicate()
        if err:
            collectd.debug("Error: %s" % err)
            return
        heapusage = heapusage.split("\n")
        heapusage = str(heapusage[1]).split()
        heapusage = ((float(heapusage[2]) + float(heapusage[3]) + float(heapusage[
            5]) + float(heapusage[7]) + float(heapusage[9]) + float(heapusage[11])) / 1024)

        ram_usage = self.get_ramusage(pid)
        if ram_usage == -1:
            return

        cpu_usage, utime, stime, clk_tick = self.get_cpuusage(pid)
        if cpu_usage == -1:
            return

        values = [float(num_threads), float(classes[5]), float(heapsize), float(heapusage), float(
            ram_usage), float(cpu_usage), float(pid), float(utime), float(stime), float(clk_tick)]
        self.dispatch_collectd(values, state, pid)

    def dispatch_collectd(self, values, state, pid):
        """Dispatches values to collectd."""
        metric = collectd.Values()
        metric.plugin = 'jvm_stats'
        metric.plugin_instance = str(pid)
        metric.type = 'jvmstatic'
        metric.values = values
        metric.interval = int(self.interval)
        metric.meta["Process_state"] = state
        metric.meta["name"] = self.process
        metric.dispatch()

    def get_jvmstate(self):
        """Get the state of jvm process"""
        pids = self.get_pid()
        if not pids:
            collectd.debug("No JAVA process are running")
            return
        for pid in pids:
            fileobj = open('/proc/%d/status' % (int(pid)))
            if fileobj is None:
                collectd.debug(
                    "Error: Unable to open /proc/%d/status" % (int(pid)))
                return
            lines = fileobj.readlines()
            for line in lines:
                if line.startswith("State:"):
                    state = (line.split())[2]
                    state = state.strip("()")
                    self.get_jvmstatistics(pid, state)
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
