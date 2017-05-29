#!/usr/bin/python
#
# Copyright (c) 2017 Maplelabs
# All Rights Reserved.
#
#
# Collectd libvirt disk python plugin
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

PLUGIN_NAME = "libvirt_disk"

class LibvirtDisk:
    """
    LibvirtDisk collects libvirt related disk data from libvirt
    """

    def __init__(self, interval=5):
        """
        :param:interval: integer collection interval
        """
        self.interval = interval
        url = LIBVIRT_URL
        self.error = None
        self.prev_disk_data = {}
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
            tree = ElementTree.fromstring(domain.XMLDesc())
            disks = [i.get("file")
                     for i in tree.findall("devices/disk/source")]
            for disk in disks:
                if disk:
                    collectd.info("Collecting stats for '%s' disk" % (disk))
                    disk_dict = self.collect_disk_stats(domain, disk)
                    self.prev_disk_data[disk_dict[DISK_PATH]] = deepcopy(disk_dict)
                    dispatch(disk_dict)
                    collectd.info(
                        "Data for disk: %s of VM: %s is dispatched" %
                        (disk, domain.name()))
        if not self.error:
            self.conn.close()

    def collect_disk_stats(self, domain, disk):
        """
        Collect VM disk statistics
        :param domain: Object represents a domain or VM
        :param disk: String representing disk name
        :return: Dictionary with Disk stats for the domain
        """
        data = dict()
        data[PLUGIN] = DISK_PLUGIN
        data[UTC] = str(datetime.datetime.utcnow())
        data[INTERVAL] = self.interval
        data[DISK_NAME] = str(disk.split('/')[-2] + "-" + disk.split('/')[-1])
        data[PLUGIN_INS] = str(domain.name() + "-" + data[DISK_NAME])
        state, reason = domain.state()
        data[VM_STATE] = get_vm_state(state)
        data[VMNAME] = domain.name()
        data[DISK_PATH] = disk
        data[TIMESTAMP] = time.time()
        try:
            rd_req, rd_bytes, wr_req, wr_bytes, err = domain.blockStats(disk)
            data[DISK_READ_REQ] = int(rd_req)
            data[DISK_READ_BYTES] = int(rd_bytes)
            data[DISK_WRITE_REQ] = int(wr_req)
            data[DISK_WRITE_BYTES] = int(wr_bytes)
            data[DISK_ERR] = int(err)
            read_iops = get_rate(
                DISK_READ_REQ, data, self.prev_disk_data.get(
                    data[DISK_PATH], {}))
            write_iops = get_rate(
                DISK_WRITE_REQ, data, self.prev_disk_data.get(
                    data[DISK_PATH], {}))
            write_throughput = get_rate(
                DISK_WRITE_BYTES, data, self.prev_disk_data.get(
                    data[DISK_PATH], {}))
            read_throughput = get_rate(
                DISK_READ_BYTES, data, self.prev_disk_data.get(
                    data[DISK_PATH], {}))
            if read_iops != NAN:
                data[READ_IOPS] = read_iops
            if write_iops != NAN:
                data[WRITE_IOPS] = write_iops
            if write_throughput != NAN:
                data[WRITE_THROUGHPIUT] = round(
                    (float(write_throughput) / (1024.0 * 1024.0)), 2)
            if read_throughput != NAN:
                data[READ_THROUGHPUT] = round(
                    (float(read_throughput) / (1024.0 * 1024.0)), 2)
            collectd.info(
                "Collectd stats for disk '%s' of vm: %s" %
                (disk, domain.name()))
        except libvirt.libvirtError:
            collectd.warning(
                "VM is not in Running mode !!, VM: %s" %
                (domain.name()))

        return data

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


collectd.info("Registering '%s' ... " % PLUGIN_NAME)
virt = LibvirtDisk()
collectd.register_config(virt.read_config)
collectd.register_read(virt.read_temp)
collectd.info("Registered '%s' plugin successfully :)" % PLUGIN_NAME)
