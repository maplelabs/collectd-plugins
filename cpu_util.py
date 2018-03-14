"""Python plugin for collectd to fetch cpu utilization information."""

#!/usr/bin/python

import signal
import json
import time
import psutil
import collectd
import re

# user imports
import utils
from constants import *


class CpuUtil(object):
    """Plugin object will be created only once and collects cpu statistics info every interval."""

    def __init__(self):
        """Initializes interval and previous dictionary variable."""
        self.interval = DEFAULT_INTERVAL
        self.prev_data = {}

    def config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]

    def get_LLC_stats(self):
        LLCStats, LLCerr = utils.get_cmd_output("perf stat -a -e LLC-loads,LLC-load-misses sleep 2")

        sdlines = re.split("\n", LLCerr)
        num_lines = len(sdlines)
        index = 0
        LLCLoads = 0
        LLCLoadMisses = 0
        LLC = False
        while index < num_lines:
            line = sdlines[index]
            if line:
                if "perf not found for kernel" in line:
                    collectd.info(
                        "Plugin cpu_static: Perf is not installed for the kernel.")
                    return 0
                elif "not supported" in line:
                    collectd.info(
                        "Plugin cpu_static: LLC Statistics is hidden from Virtual machines.")
                    return 0
                elif "LLC-loads" in line:
                    LLCLoads = float(line.strip(" ").split(" ")[0].replace(",", ""))
                    LLC = True
                elif "LLC-load-misses" in line:
                    LLCLoadMisses = float(line.strip(" ").split(" ")[0].replace(",", ""))
                    LLC = True
                index += 1
            else:
                index += 1
        if LLC:
            return round(((LLCLoadMisses / LLCLoads) * 100), 2)

        return 0


    def add_cpu_data(self):
        """Returns dictionary with values of total,per core CPU utilization
           and no. of cores in low, medium and high range."""
        dict_cpu_util = {CPU_UTIL: psutil.cpu_percent(
            interval=None, percpu=False)}
        per_cpu_util = psutil.cpu_percent(interval=None, percpu=True)
        no_of_cores = len(per_cpu_util)

        dict_cpu_util[NUM_HIGH_ACTIVE] = dict_cpu_util[
            NUM_MEDIUM_ACTIVE] = dict_cpu_util[NUM_LOW_ACTIVE] = 0

        for i in range(1, no_of_cores + 1):
            dict_cpu_util[CORE + str(i)] = per_cpu_util[i - 1]
            if LOW_RANGE_BEGIN <= per_cpu_util[i - 1] <= LOW_RANGE_END:
                dict_cpu_util[NUM_LOW_ACTIVE] += 1
            elif LOW_RANGE_END < per_cpu_util[i - 1] <= MEDIUM_RANGE_END:
                dict_cpu_util[NUM_MEDIUM_ACTIVE] += 1
            elif MEDIUM_RANGE_END < per_cpu_util[i - 1] <= HIGH_RANGE_END:
                dict_cpu_util[NUM_HIGH_ACTIVE] += 1

        dict_cpu_util["LLCMissRate"] = self.get_LLC_stats()
        return dict_cpu_util

    def add_common_params(self, dict_cpu_util):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        timestamp = int(round(time.time()))
        dict_cpu_util[TIMESTAMP] = timestamp
        dict_cpu_util[PLUGINTYPE] = CPU
        dict_cpu_util[ACTUALPLUGINTYPE] = CPU
        dict_cpu_util[PLUGIN] = LINUX
        #dict_cpu_util[PLUGIN_INS] = P_INS_ALL
        collectd.info("Plugin cpu_util: Added common parameters successfully")

    def collect_data(self):
        """Validates if dictionary is not null.If null returns None."""
        dict_cpu_util = self.add_cpu_data()
        if not dict_cpu_util:
            collectd.error(
                "Plugin cpu_util: Unable to fetch information of cpu.")
            return None

        collectd.info(
            "Plugin cpu_util: Added cpu utilization information successfully")
        # add common parameters
        self.add_common_params(dict_cpu_util)

        return dict_cpu_util

    def ignore_first_value(self):
        """Ignores the first dictionary created."""
        if not self.prev_data:
            return True

    def dispatch_data(self, dict_cpu_util):
        """Dispatches dictionary to collectd."""
        collectd.info("Plugin cpu_util: Succesfully sent to collectd.")
        collectd.debug("Plugin cpu_util: Values dispatched = " +
                       json.dumps(dict_cpu_util))
        utils.dispatch(dict_cpu_util)

    def read(self):
        """Collects all data and validates if its not first dictionary created."""
        dict_cpu_util = self.collect_data()
        if not dict_cpu_util:
            return

        if self.ignore_first_value():
            self.prev_data = dict_cpu_util
            return

        self.prev_data = dict_cpu_util

        # dispatch data to collectd
        self.dispatch_data(dict_cpu_util)

    def read_temp(self):
        """Collectd first calls register_read. At that time default interval is taken,
        hence temporary function is made to call, the read callback is unregistered
        and read() is called again with interval obtained from conf by register_config callback."""
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


def init():
    """When new process is formed, action to SIGCHLD is reset to default behavior."""
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)


OBJ = CpuUtil()
collectd.register_config(OBJ.config)
collectd.register_read(OBJ.read_temp)
