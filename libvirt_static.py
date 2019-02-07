"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
#!/usr/bin/python
#
# Collectd libvirt static python plugin
#
#

import datetime
import time
from copy import deepcopy
from xml.etree import ElementTree
import collectd
import libvirt
from utils import *
from libvirt_constants import *
from libvirt_utils import *


PLUGIN_NAME = "libvirt_static"


class LibvirtStatic:
    """
    CollectLibvirt collects libvirt related data from libvirt
    """

    def __init__(self, interval=5):
        """
        :param:interval: integer collection interval
        """
        self.interval = interval
        self.url = LIBVIRT_URL
        self.prev_data = {}
        self.error = None
        self.conn = None

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

    def estabish_connection(self):
        try:
            self.conn = libvirt.open(self.url)
            if not self.conn:
                collectd.error('Failed to open connection %s' % self.url)
                self.error = FAILURE
            collectd.info(
                "Plugin successfully created connection with hypervisor")

            libvirt_version = self.conn.getLibVersion()
            if libvirt_version >= LIBVIRT_VERSION:
                lib_ver = ".".join([i for i in LIBVIRT_VERSION[:3]])
                collectd.warning(
                    "Libvirtd version should be greater than or equal "
                    "to " + lib_ver + ", things may not work as expected")
        except libvirt.libvirtError as e:
            collectd.error(
                'Failed to open connection %s, reason: %s' %
                (self.url, e.get_error_message()))
            self.error = FAILURE

    def read(self):
        """
        read() method collects the data from libvirt and dispatch it
        """

        self.estabish_connection()

        if self.error == FAILURE:
            collectd.error("Empty stats dispatch, failed to open connection.")
            return

        for domain in self.conn.listAllDomains(0):
            collectd.info("Collecting libvirt static data for VM: %s" % (
                domain.name()))
            data = self.collect_vm_data(domain)
            self.prev_data[data[VMNAME]] = deepcopy(data)

            dispatch(data)
            collectd.info("Libvirt static data for VM: %s is dispatched" % (
                domain.name()))

        if not self.error:
            self.conn.close()

    def fill_common(self, data_dict):
        data_dict[PLUGINTYPE] = STATIC_PLUGIN
        data_dict[PLUGIN] = LIBVIRT
        data_dict[UTC] = str(datetime.datetime.utcnow())
        data_dict[INTERVAL] = self.interval
        data_dict[TIMESTAMP] = time.time()
        return data_dict

    def collect_vm_data(self, domain):
        """
        Collect stats for the VM or domain
        :param domain: Object representing the domain or VM
        :return: Dictionary with VM stats
        """
        data = {}

        if self.error == FAILURE:
            collectd.error(
                "Empty VM stats dispatch, failed to open connection.")
            return data

        data = self.fill_common(data)

        data[VMNAME] = domain.name()
        tree = ElementTree.fromstring(domain.XMLDesc())
        data[VM_SOURCE] = tree.find('devices/disk/source').get('file')

        data[PLUGIN_INS] = domain.name()
        data[UTC] = str(datetime.datetime.utcnow())
        data[TAGS] = "vm_static"

        try:
            state, maxmem, mem, cpus, cput = domain.info()
            data[NO_OF_VCPU] = int(cpus)
            data[CPU_TIME] = cput
            data[VM_STATE] = get_vm_state(state)
            data[ALLOCATED_MEMORY] = float(maxmem) / 1024
            collectd.info(
                "Memory stats collected for VM: %s" %
                (domain.name()))
        except libvirt.libvirtError as e:
            collectd.warning(
                "Unable to collect stats for"
                " VM: %s, Reason: %s" %
                (domain.name(), e.get_error_message()))

        collectd.info("Collected VM static data.")
        return data

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


collectd.info("Registering '%s' ... " % PLUGIN_NAME)
virt = LibvirtStatic()
collectd.register_config(virt.read_config)
collectd.register_read(virt.read_temp)
collectd.info("Registered '%s' plugin successfully :)" % PLUGIN_NAME)
