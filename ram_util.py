"""Python plugin for collectd to get ram utilization and available ram."""


#!/usr/bin/python
import signal
import json
import time
import psutil
import collectd

# user imports
import utils
from constants import *


class RamUtil(object):
    """Plugin object will be created only once and collects utils
       and available RAM info every interval."""

    def __init__(self):
        """Initializes interval."""
        self.interval = DEFAULT_INTERVAL

    def config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]

    def add_ram_data(self):
        """
        Returns dictionary with values of available and ram utilization information.
        """
        mem = psutil.virtual_memory()
        dict_ram_util = {'available': round(
            float(mem.available) / (FACTOR * FACTOR * FACTOR), FLOATING_FACTOR), 'RAMUtil': mem.percent,
            'totalRAM': round(float(mem.total) / (FACTOR * FACTOR * FACTOR), FLOATING_FACTOR), 
            'free': round(float(mem.free) / (FACTOR * FACTOR * FACTOR), FLOATING_FACTOR),
            'cached':round(float(mem.cached) / (FACTOR * FACTOR * FACTOR), FLOATING_FACTOR),
            'buffered':round(float(mem.buffers) / (FACTOR * FACTOR * FACTOR), FLOATING_FACTOR),
            'used': round(float(mem.used) / (FACTOR * FACTOR * FACTOR), FLOATING_FACTOR)
        }
        return dict_ram_util

    def add_common_params(self, dict_ram_util):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        timestamp = int(round(time.time()))
        dict_ram_util[TIMESTAMP] = timestamp
        dict_ram_util[PLUGIN] = LINUX
        dict_ram_util[PLUGINTYPE] = RAM
        dict_ram_util[ACTUALPLUGINTYPE] = RAM
        #dict_ram_util[PLUGIN_INS] = P_INS_ALL
        collectd.info("Plugin ram_util: Added common parameters successfully")

    def collect_data(self):
        """Validates if dictionary is not null.If null then returns None."""
        dict_ram_util = self.add_ram_data()
        if not dict_ram_util:
            collectd.error("Plugin ram_util: Unable to fetch RAM information.")
            return None

        collectd.info("Plugin ram_util: Added ram information successfully")
        self.add_common_params(dict_ram_util)

        return dict_ram_util

    def dispatch_data(self, dict_ram_util):
        """Dispatches dictionary to collectd."""
        collectd.info("Plugin ram_util: Successfully sent to collectd.")
        collectd.debug("Plugin ram_util: Values dispatched = " +
                       json.dumps(dict_ram_util))
        utils.dispatch(dict_ram_util)

    def read(self):
        """Collects all data."""
        dict_ram_util = self.collect_data()
        if not dict_ram_util:
            return

        # dispatch data to collectd
        self.dispatch_data(dict_ram_util)

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


OBJ = RamUtil()
collectd.register_config(OBJ.config)
collectd.register_read(OBJ.read_temp)
