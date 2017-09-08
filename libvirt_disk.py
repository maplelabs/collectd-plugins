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
import time
from copy import deepcopy
from xml.etree import ElementTree

from utils import *
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
        self.prev_disk_agg = {}
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
            self.error = FAILURE

    def add_aggregate(self, domain, disk_agg):
        disk_agg[PLUGINTYPE] = DISK_PLUGIN
        disk_agg[PLUGIN] = LIBVIRT
        disk_agg[UTC] = str(datetime.datetime.utcnow())
        disk_agg[INTERVAL] = self.interval
        disk_agg[PLUGIN_INS] = str(domain.name() + "-disk-aggregate")
        state, reason = domain.state()
        disk_agg[VM_STATE] = get_vm_state(state)
        disk_agg[VMNAME] = domain.name()
        disk_agg[TIMESTAMP] = time.time()

        read_iops = get_rate(
            DISK_READ_REQ, disk_agg, self.prev_disk_agg.get(domain.name(), {}))
        write_iops = get_rate(
            DISK_WRITE_REQ, disk_agg, self.prev_disk_agg.get(domain.name(), {}))
        write_throughput = get_rate(
            DISK_WRITE_BYTES, disk_agg, self.prev_disk_agg.get(domain.name(), {}))
        read_throughput = get_rate(
            DISK_READ_BYTES, disk_agg, self.prev_disk_agg.get(domain.name(), {}))

        if read_iops != NAN:
            disk_agg[AGG_READ_IOPS] = read_iops
        if write_iops != NAN:
            disk_agg[AGG_WRITE_IOPS] = write_iops
        if write_throughput != NAN:
            disk_agg[AGG_WRITE_THROUGHPUT] = round(
                (float(write_throughput) / (FACTOR * FACTOR * 1.0)), 2)
        if read_throughput != NAN:
            disk_agg[AGG_READ_THROUGHPUT] = round(
                (float(read_throughput) / (FACTOR * FACTOR * 1.0)), 2)

        return

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
                collectd.warning("Failed to collectd dynamic disk "
                                 "stats for VM %s, VM is not running!" % domain.name())
                continue
            tree = ElementTree.fromstring(domain.XMLDesc())
            disks = [i.get("file")
                     for i in tree.findall("devices/disk/source")]

            total_disk_read_bytes = 0
            total_disk_write_bytes = 0
            total_disk_read_req = 0
            total_disk_write_req = 0

            for disk in disks:
                if disk:
                    collectd.info("Collecting stats for '%s' disk" % (disk))
                    disk_dict = self.collect_disk_stats(domain, disk)
                    self.prev_disk_data[disk_dict[DISK_PATH]] = deepcopy(disk_dict)
                    dispatch(disk_dict)
                    collectd.info(
                        "Data for disk: %s of VM: %s is dispatched" %
                        (disk, domain.name()))

                    total_disk_read_bytes = total_disk_read_bytes + disk_dict[DISK_READ_BYTES]
                    total_disk_write_bytes = total_disk_write_bytes + disk_dict[DISK_WRITE_BYTES]
                    total_disk_read_req = total_disk_read_req + disk_dict[DISK_READ_REQ]
                    total_disk_write_req = total_disk_write_req + disk_dict[DISK_WRITE_REQ]

            disk_agg = {}
            disk_agg[TYPE] = AGGREGATE
            disk_agg[DISK_READ_BYTES] = total_disk_read_bytes
            disk_agg[DISK_WRITE_BYTES] = total_disk_write_bytes
            disk_agg[DISK_READ_REQ] = total_disk_read_req
            disk_agg[DISK_WRITE_REQ] = total_disk_write_req

            self.add_aggregate(domain, disk_agg)
            self.prev_disk_agg[domain.name()] = disk_agg

            dispatch(disk_agg)

        if not self.error:
            self.conn.close()

    def add_common(self, data):
        data[TIMESTAMP] = time.time()

    def collect_disk_stats(self, domain, disk):
        """
        Collect VM disk statistics
        :param domain: Object represents a domain or VM
        :param disk: String representing disk name
        :return: Dictionary with Disk stats for the domain
        """
        data = dict()
        data[PLUGIN] = LIBVIRT
        data[PLUGINTYPE] = DISK_PLUGIN
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
                    (float(write_throughput) / (FACTOR * FACTOR * 1.0)), 2)
            if read_throughput != NAN:
                data[READ_THROUGHPUT] = round(
                    (float(read_throughput) / (FACTOR * FACTOR * 1.0)), 2)
            collectd.info(
                "Collectd stats for disk '%s' of vm: %s" %
                (disk, domain.name()))
        except libvirt.libvirtError as e:
            collectd.warning(
                "Failed for VM %s, reason: %s" %
                (domain.name()), e.message)

        return data

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


collectd.info("Registering '%s' ... " % PLUGIN_NAME)
virt = LibvirtDisk()
collectd.register_config(virt.read_config)
collectd.register_read(virt.read_temp)
collectd.info("Registered '%s' plugin successfully :)" % PLUGIN_NAME)
