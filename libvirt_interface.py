"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
#!/usr/bin/python
#
# Collectd libvirt interface / network python plugin
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

PLUGIN_NAME = "libvirt_interface"


class LibvirtInterface:
    """
    LibvirtInterface collects libvirt related interface data from libvirt
    """

    def __init__(self, interval=5):
        """
        :param:interval: integer collection interval
        """
        self.interval = interval
        url = LIBVIRT_URL
        self.error = None
        self.prev_iface_data = {}
        self.prev_iface_agg = {}
        self.conn = None
        self.url = LIBVIRT_URL
        self.first_time = True

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
            tree = ElementTree.fromstring(domain.XMLDesc())
            interfaces = [i.get("dev")
                          for i in tree.findall('devices/interface/target')]

            total_rx_pkts = 0
            total_tx_pkts = 0
            total_rx_drops = 0
            total_tx_drops = 0
            total_rx_bytes = 0
            total_tx_bytes = 0

            for iface in interfaces:
                if iface:
                    collectd.info(
                        "Collecting stats for '%s' interface of VM: %s" %
                        (iface, domain.name()))
                    nic_data = self.collectd_nic_stats(domain, iface)
                    self.prev_iface_data[nic_data[IFACE_NAME]] = deepcopy(
                        nic_data)
                    dispatch(nic_data)
                    collectd.info("Data for interface: %s of VM: %s is "
                                  "dispatched" % (iface, domain.name()))

                    total_rx_pkts = total_rx_pkts + nic_data[RX_PKTS]
                    total_tx_pkts = total_tx_pkts + nic_data[TX_PKTS]
                    total_rx_drops = total_rx_drops + nic_data[RX_DROPS]
                    total_tx_drops = total_tx_drops + nic_data[TX_DROPS]
                    total_rx_bytes = total_rx_bytes + nic_data[RX_BYTES]
                    total_tx_bytes = total_tx_bytes + nic_data[TX_BYTES]

            interface = {}
            interface[TYPE] = AGGREGATE
            interface[RX_PKTS] = total_rx_pkts
            interface[TX_PKTS] = total_tx_pkts
            interface[RX_DROPS] = total_rx_drops
            interface[TX_DROPS] = total_tx_drops
            interface[RX_BYTES] = total_rx_bytes
            interface[TX_BYTES] = total_tx_bytes
            self.add_aggregate(domain, interface)
            self.prev_iface_agg[domain.name()] = interface
            dispatch(interface)

        if not self.error:
            self.conn.close()

    def add_aggregate(self, domain, iface):
        iface[PLUGINTYPE] = IFACE_PLUGIN
        iface[PLUGIN] = LIBVIRT
        iface[UTC] = str(datetime.datetime.utcnow())
        iface[INTERVAL] = self.interval
        iface[PLUGIN_INS] = str(domain.name() + "-iface-aggregate")
        iface[VMNAME] = domain.name()
        state, reason = domain.state()
        iface[VM_STATE] = get_vm_state(state)
        iface[UTC] = str(datetime.datetime.utcnow())
        iface[TIMESTAMP] = time.time()

        rx_rate = get_rate(
            RX_BYTES, iface, self.prev_iface_agg.get(domain.name(), {}))
        tx_rate = get_rate(
            TX_BYTES, iface, self.prev_iface_agg.get(domain.name(), {}))
        if rx_rate != NAN:
            iface[AGG_RX_RATE] = round(float(rx_rate) / (1024 * 1024), 2)
        if tx_rate != NAN:
            iface[AGG_TX_RATE] = round(float(tx_rate) / (1024 * 1024), 2)
        collectd.info(
            "Collectd stats for nic: '%s' is collected of VM: %s" %
            (iface, domain.name()))

    def collectd_nic_stats(self, domain, iface):
        """
        Collect interface stats
        :param domain: Object represents a domain or VM
        :param iface: String naming an interface
        :return: Dictionary with nic stats
        """
        data = {}
        data[PLUGIN] = LIBVIRT
        data[PLUGINTYPE] = IFACE_PLUGIN
        data[UTC] = str(datetime.datetime.utcnow())
        data[INTERVAL] = self.interval
        data[PLUGIN_INS] = str(domain.name() + "-" + iface)
        data[VMNAME] = domain.name()
        state, reason = domain.state()
        data[VM_STATE] = get_vm_state(state)
        data[IFACE_NAME] = iface
        try:
            stats = domain.interfaceStats(iface)
            data[RX_BYTES] = int(stats[0])
            data[RX_PKTS] = int(stats[1])
            data[RX_ERR] = int(stats[2])
            data[RX_DROPS] = int(stats[3])
            data[TX_BYTES] = int(stats[4])
            data[TX_PKTS] = int(stats[5])
            data[TX_ERR] = int(stats[6])
            data[TX_DROPS] = int(stats[7])
            data[UTC] = str(datetime.datetime.utcnow())
            data[TIMESTAMP] = time.time()
            rx_rate = get_rate(
                RX_BYTES, data, self.prev_iface_data.get(
                    data[IFACE_NAME], {}))
            tx_rate = get_rate(
                TX_BYTES, data, self.prev_iface_data.get(
                    data[IFACE_NAME], {}))
            if rx_rate != NAN:
                data[RX_RATE] = round(float(rx_rate) / (1024 * 1024), 2)
            if tx_rate != NAN:
                data[TX_RATE] = round(float(tx_rate) / (1024 * 1024), 2)
            collectd.info(
                "Collectd stats for nic: '%s' is collected of VM: %s" %
                (iface, domain.name()))

        except libvirt.libvirtError as e:
            collectd.info(
                "Nic stats ignored for VM: %s, reason: %s" %
                (domain.name(), e.get_error_message()))

        tag, trunks = self.get_vlan_details(iface)
        data[VLAN_TAG] = tag
        data[VLAN_TRUNKS] = trunks

        return data

    def execute_cli(self, cli):
        try:
            p = Popen(cli, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            output, err = p.communicate()
        except Exception as e:
            return None, e.message
        return output, err

    def get_vlan_details(self, iface):
        """
        Collect vlan details from ovs-vsctl show output
        for the specified interface
        :param iface: String , contain interface name
        :return: Two String value, tag and trunk details
        """
        cli = ["ovs-vsctl", "show"]
        tag = None
        trunks = None

        output, err = self.execute_cli(cli)
        if output is None:
            collectd.warning("Unable to get VLAN details %s." % (err))
            return tag, trunks

        entries = re.split(r"Port", output)

        for entry in entries:
            if iface in entry:
                try:
                    tag = re.findall(r'tag:.*', entry)[0][5:]
                except IndexError:
                    pass
                try:
                    trunks = re.findall(r'trunks:.*', entry)[0][8:].strip("[]")
                except IndexError:
                    pass
                return tag, trunks

            # Ignoring first three characters of the interface name
            # to make it work with OVS, if the it is an OpenStack setup
            elif iface[3:] in entry:
                try:
                    tag = re.findall(r'tag:.*', entry)[0][5:]
                except IndexError:
                    pass
                try:
                    trunks = re.findall(r'trunks:.*', entry)[0][8:].strip("[]")
                except IndexError:
                    pass
                return tag, trunks

        return tag, trunks

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


collectd.info("Registering '%s' ... " % PLUGIN_NAME)
virt = LibvirtInterface()
collectd.register_config(virt.read_config)
collectd.register_read(virt.read_temp)
collectd.info("Registered '%s' plugin successfully :)" % PLUGIN_NAME)
