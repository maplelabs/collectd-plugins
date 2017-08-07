f#!/usr/bin/python
"""Python plugin for collectd to get static CPU information."""

from __future__ import division
import signal
import re
import json
import time
import collectd

# user imports
import utils
from constants import *


class CpuStatic(object):
    """Plugin object will be created only once and collects cpuType, HT, Sockets, Clock, Cores
    and logical CPUs info every interval."""

    def __init__(self):
        """Initializes interval."""
        self.interval = DEFAULT_INTERVAL
        self.nodeType = ""

    def read_config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == NODETYPE:
                self.nodeType = children.values[0]


    def add_cpu_data(self):
        """Return dictionary with values of  CPUType, HT, CLOCK, SOCKET, TOTAL_CORE
        and TOTAL_LOGICAL_CPU obtained from /proc/cpuinfo file."""
        dict_cpu_static = {}
        total_logical_cpus = 0
        total_physical_cpus = 0
        total_cores = 0
        keyword_processor_found = False
        keyword_physical_id_found = False
        keyword_core_id_found = False
        logical_cpus = {}
        physical_cpus = {}
        cores = {}
        hyperthreading = 0
        model_list = []
        total_freq = 0
        count_freq = 0

        try:
            with open('/proc/cpuinfo') as cpuinfo_file:
                lines = cpuinfo_file.readlines()
        except IOError:
            collectd.error("Plugin cpu_static: Unable to open /proc/cpuinfo")
            return None

        index = 0
        while index < len(lines):
            line = lines[index]
            if re.match('processor', line):
                cpu = int(line.split()[2])
                keyword_processor_found = True

                if cpu not in logical_cpus:
                    logical_cpus[cpu] = []
                    total_logical_cpus += 1

            if re.match('physical id', line):
                phys_id = int(line.split()[3])
                keyword_physical_id_found = True

                if phys_id not in physical_cpus:
                    physical_cpus[phys_id] = []
                    total_physical_cpus += 1

            if re.match('core id', line):
                core = int(line.split()[3])
                keyword_core_id_found = True

                if core not in cores:
                    cores[core] = []
                    total_cores += 1

            if re.match('model name', line):
                modelname = line.split(":")
                modelname = str(modelname[1]).strip(' ')
                modelname = modelname.strip('\n')
                if modelname not in model_list:
                    model_list.append(modelname)

            if re.match("cpu MHz", line):
                total_freq += float(line.split()[3])
                count_freq += 1

            index += 1

        if keyword_processor_found and keyword_physical_id_found and keyword_core_id_found:
            if (total_cores * total_physical_cpus) * 2 == total_logical_cpus:
                hyperthreading = 1
            model_name = ",".join(model_list)

        dict_cpu_static[CPU_TYPE] = model_name
        if count_freq != 0:
            dict_cpu_static[CLOCK] = round(
                total_freq / count_freq, FLOATING_FACTOR)
        dict_cpu_static[TOTAL_CORE] = total_cores
        dict_cpu_static[SOCKET] = total_physical_cpus
        dict_cpu_static[TOTAL_LOGICAL_CPU] = total_logical_cpus
        dict_cpu_static[HT] = "On" if int(hyperthreading) else "off"
        return dict_cpu_static

    def add_common_params(self, dict_cpu_static):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        timestamp = time.time()
        dict_cpu_static[TIMESTAMP] = timestamp
        dict_cpu_static[TYPE] = CPU_STATIC
        dict_cpu_static[PLUGIN] = LINUX_STATIC
        dict_cpu_static[PLUGIN_INS] = P_INS_ALL
        collectd.info(
            "Plugin cpu_static: Added common parameters successfully")

    def collect_data(self):
        """Validates if dictionary is not null.If yes then returns None."""
        dict_cpu_static = self.add_cpu_data()
        if not dict_cpu_static:
            collectd.error(
                "Plugin cpu_static: Unable to fetch data information of CPU.")
            return None

        collectd.info(
            "Plugin cpu_static: Added CPU static parameters successfully")
        self.add_common_params(dict_cpu_static)

        return dict_cpu_static

    def dispatch_data(self, dict_cpu_static):
        """Dispatches dictionary to collectd."""
        collectd.info("Plugin cpu_static: Successfully sent to collectd.")
        collectd.debug("Plugin cpu_static: Values dispatched = " +
                       json.dumps(dict_cpu_static))
        utils.dispatch(dict_cpu_static)

    def read(self):
        """Collects all data for interval registered in read callback."""
        if(self.nodeType.lower() == "virtual"):
            return
        dict_cpu_static = self.collect_data()
        if not dict_cpu_static:
            return

        # dispatch data to collectd
        self.dispatch_data(dict_cpu_static)

    def read_temp(self):
        """
        Collectd first calls register_read. At that time default interval is taken,
        hence temporary function is made to call, the read callback is unregistered
        and read() is called again with interval obtained from conf by register_config callback.
        """
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


def init():
    """When new process is formed, action to SIGCHLD is reset to default behavior."""
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)


OBJ = CpuStatic()
collectd.register_config(OBJ.read_config)
collectd.register_read(OBJ.read_temp)
