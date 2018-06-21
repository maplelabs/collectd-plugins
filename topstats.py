"""Python plugin for collectd to get highest CPU/Memory usage process using top command"""


#!/usr/bin/python
import signal
import json
import time
import collectd
import subprocess

# user imports
import utils
from constants import *


class TopStats(object):
    """Plugin object will be created only once and collects utils
       and available RAM info every interval."""

    def __init__(self, interval=1, utilize_type="mem", maximum_grep=5, process_name='*'):
        """Initializes interval."""
        self.interval = DEFAULT_INTERVAL
        self.utilize_type = utilize_type
        self.maximum_grep = maximum_grep
        self.process = process_name

    def config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == 'utilize_type':
                self.utilize_type = children.values[0]
            if children.key == 'maximum_grep':
                self.maximum_grep = children.values[0]
            if children.key == "process":
                self.process = children.values[0]

    def top_command(self):
        """
        Returns dictionary with values of available and top SPU and memory usage summary of teh process.
        """
        #cmnd = "top -b -o +" + self.usage_parameter +" -n 1 | head -17 | sed -n '8,20p' | awk '{print $1, $2, $9, $10, $12}'"
        head_value = 7 + int(self.maximum_grep)
        if self.process and self.process != 'None' and self.process != '*':
            #cmnd  = "top -b -n 1 | grep '" + self.process + "' | awk '{print $1, $2, $9, $10, $12}'"
            cmnd = "top -b -o +%" + self.utilize_type.upper() + " -n 1 | grep '" + self.process + "' | head -"+ str(head_value) + " | awk '{print $1, $2, $9, $10, $12}'"
        elif self.utilize_type.upper() == "CPU" or self.utilize_type.upper() == "RAM":
	    if self.utilize_type.upper() == "RAM":
                resource_type = "MEM"
	    else:
		resource_type = self.utilize_type.upper()
            cmnd = "top -b -o +%" + resource_type + " -n 1 | head -" + str(head_value) + " | sed -n '8,20p' | awk '{print $1, $2, $9, $10, $12}'"
        process = subprocess.Popen(cmnd, shell=True, stdout=subprocess.PIPE)
        result = []
        i = 1
        while True:
            top_stats_res = {}
            line = process.stdout.readline()
            if line != b'':
                top_stats_res['order'] = i
                top_stats_res['pid'] = long(line.split(' ')[0])
                top_stats_res['user'] = line.split(' ')[1]
                top_stats_res['cpu'] = float(line.split(' ')[2])
                top_stats_res['memory'] = float(line.split(' ')[3])
                top_stats_res['command'] = line.split(' ')[4].strip()
                top_stats_res['process_group'] = self.process
                top_stats_res['resource_type'] = self.utilize_type.upper()
                #os.write(1, line)
                i+= 1
                result.append(top_stats_res)
            else:
                break
        return result

    def add_common_params(self, top_stats_res):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        timestamp = int(round(time.time()))
        for result in top_stats_res:
            result[TIMESTAMP] = timestamp
            result[PLUGIN] = "topstats"
            result[PLUGINTYPE] = "usage_stats"
            result[ACTUALPLUGINTYPE] = "topstats"
        #top_stats_res[PLUGIN_INS] = P_INS_ALL
        collectd.info("Plugin topstats: Added common parameters successfully")

    def collect_data(self):
        """Validates if dictionary is not null.If null then returns None."""
        top_stats_res = self.top_command()
        if not top_stats_res:
            collectd.error("Plugin topstats: Unable to fetch Top Usage Summary")
            return None

        collectd.info("Plugin topstats: Added ram information successfully")
        self.add_common_params(top_stats_res)

        return top_stats_res

    def dispatch_data(self, top_stats_res):
        """Dispatches dictionary to collectd."""
        collectd.info("Plugin topstats: Successfully sent to collectd.")
        collectd.debug("Plugin topstats: Values dispatched = " +
                       json.dumps(top_stats_res))
        for result in top_stats_res:
            utils.dispatch(result)

    def read(self):
        """Collects all data."""
        top_stats_res = self.collect_data()
        if not top_stats_res:
            return

        # dispatch data to collectd
        self.dispatch_data(top_stats_res)

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


OBJ = TopStats()
collectd.register_config(OBJ.config)
collectd.register_read(OBJ.read_temp)
