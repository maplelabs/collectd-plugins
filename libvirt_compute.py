#!/usr/bin/python
#
# Copyright (c) 2017 Maplelabs
# All Rights Reserved.
#
#
# Collectd libvirt compute python plugin
#
#

import datetime
import re
import time
from copy import deepcopy
from xml.etree import ElementTree
from subprocess import (PIPE, Popen)
import collectd

from utils import *
import libvirt
from libvirt_utils import *
from libvirt_constants import *

PLUGIN_NAME = "libvirt_compute"


class LibvirtCompute:
    """
    LibvirtCompute collects libvirt related compute data from libvirt
    """

    def __init__(self, interval=5):
        """
        :param:interval: integer collection interval
        """
        self.interval = interval
        url = LIBVIRT_URL
        self.error = None
        self.prev_comp_data = {}
        self.conn = None
        self.url = LIBVIRT_URL

    def read_config(self, cfg):
        """
        read_config method fetch configuration parameter from
        collectd.conf
        :param cfg: Configuration object from collectd.conf
        :return: None
        """
        for child in cfg.children:
            if child.key == 'interval':
                self.interval = int(child.values[0])
                collectd.info("Collector interval : %d sec" % self.interval)

    def establish_connection(self):
        try:
            self.conn, self.error = establish_connection(self.url)
        except libvirt.libvirtError as e:
            collectd.error('Failed to open connection %s, reason: %s' % (self.url, e.get_error_message()))
            self.error= FAILURE

    def read(self):
        """
        read() method collects the data from libvirt and dispatch it
        """
        self.establish_connection()
        if self.error == FAILURE:
            collectd.error("Empty stats dispatch, failed to open connection.")
            return
        for domain in self.conn.listAllDomains(0):
            if not domain.isActive():
                collectd.warning("Failed to collectd interface "
                                 "stats for VM %s, VM is not running!" % domain.name())
                continue
            compute_dict = self.get_compute_stats(domain)
            self.prev_comp_data[compute_dict[VMNAME]] = deepcopy(compute_dict)
            dispatch(compute_dict)
            collectd.info(
                "Compute stats dispatched for VM%s" %
                (domain.name()))
        if not self.error:
            self.conn.close()

    def get_compute_stats(self, domain):
        """
        Collect stats related to CPU and RAM
        :param domain: Object represents a domain or VM
        :return: Dictionary containing CPU and RAM stats of a VM
        """
        data = {}
        data[PLUGINTYPE] = COMP_PLUGIN
        data[PLUGIN] = LIBVIRT
        data[UTC] = str(datetime.datetime.utcnow())
        data[INTERVAL] = self.interval
        data[PLUGIN_INS] = str(domain.name())
        data[VMNAME] = domain.name()
        state, reason = domain.state()
        data[VM_STATE] = get_vm_state(state)
        data[NO_OF_VCPU] = domain.maxVcpus()
        data[TIMESTAMP] = time.time()
        try:
            memstat = domain.memoryStats()
            # Data is in KB so converting into MB
            data[ALLOCATED_MEMORY] = float(memstat.get('actual')) / 1024
            # TODO USED_MEMORY is not correct, Need to do more research on this
            data[USED_MEMORY] = round(float(memstat.get('rss')) / 1024, 2)
            # TODO RAM_UTIL is not correct, Need to do more research on this
            data[RAM_UTIL] = round(
                ((data[USED_MEMORY] / data[ALLOCATED_MEMORY]) * 100.0), 2)
            stats = domain.getCPUStats(True)
            data[CPU_TIME] = stats[0]['cpu_time']
            collectd.info(
                "Memory stats collected for VM: %s" %
                (domain.name()))
        except libvirt.libvirtError as e:
            collectd.warning("Unable to collect memory stats for VM: %s,"
                             " Reason: %s" % (domain.name(),
                                              e.get_error_message()))
        cpu_util = get_cpu_percentage(
            self.prev_comp_data.get(
                data[VMNAME], {}), data)
        if cpu_util != NAN:
            data[CPU_UTIL] = round(cpu_util, 2)
        return data

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


collectd.info("Registering '%s' ... " % PLUGIN_NAME)
virt = LibvirtCompute()
collectd.register_config(virt.read_config)
collectd.register_read(virt.read_temp)
collectd.info("Registered '%s' plugin successfully :)" % PLUGIN_NAME)
