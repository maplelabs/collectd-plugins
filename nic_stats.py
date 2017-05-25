"""Python plugin for collectd to fetch nic statistics information."""


#!/usr/bin/python

import signal
import time
import socket
import json
import psutil
import collectd

# user imports
import utils
from constants import *


class NicStats(object):
    """Plugin object will be created only once and collects ip, mtu, type,
    tx/rxPkts, tx/rxBytes, tx/rxRate, tx/rxDrops, aggregate info every interval."""

    def __init__(self):
        """Initializes interval and previous dictionary variable."""
        self.interval = DEFAULT_INTERVAL
        self.prev_data = {}

    def read_config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]

    def get_virtual_interfaces(self):
        """Function to get all virtual nics."""
        virt_if_list = []
        (virt_dev, err) = utils.get_cmd_output(
            'ls -l /sys/class/net/ | grep devices/virtual')
        if err:
            return virt_if_list

        virt_dev = virt_dev.splitlines()
        for device in virt_dev:
            # At the end of each line we have "/" followed by interface name
            index = device.rfind("/")
            if index != -1:
                ifname = device[index + 1:]
                virt_if_list.append(ifname)

        return virt_if_list

    def get_static_data(self):
        """Returns dictionary with values of TYPE, IPADDR, MAC, SPEED, MTU and UP info."""
        dict_nics = {}  # will contain data for all interfaces
        for if_name, if_info in psutil.net_if_addrs().items():
            interface = {}
        #    interface[NICNAME] = if_name
            interface[TYPE] = VIRT
            for add_family in if_info:
                if add_family.family == socket.AF_INET:
                    interface[IPADDR] = add_family.address
                elif add_family.family != socket.AF_INET6:
                    interface[MAC] = add_family.address
            dict_nics[if_name] = interface

        if not dict_nics:
            return None

        virt_list = self.get_virtual_interfaces()
        for if_name, if_info in psutil.net_if_stats().items():
            if if_name in dict_nics:
                if if_name not in virt_list:
                    dict_nics[if_name][TYPE] = PHY
                dict_nics[if_name][SPEED] = round(
                    (float(if_info.speed) / (FACTOR * 8)), FLOATING_FACTOR)
                dict_nics[if_name][MTU] = if_info.mtu
                dict_nics[if_name][UP] = if_info.isup

        return dict_nics

    def get_dynamic_data(self):
        """Returns dictionary with values of RX/TX_PKTS,RX/TX_PKTS,RX/TX_DROPS and RX/TX_BYTES."""
        dict_nics = {}
        for if_name, if_info in psutil.net_io_counters(pernic=True).items():
            interface = {}
        #    interface[NICNAME]  = if_name
            interface[RX_PKTS] = if_info.packets_recv
            interface[TX_PKTS] = if_info.packets_sent
            interface[RX_DROPS] = if_info.dropin
            interface[TX_DROPS] = if_info.dropout
            interface[RX_BYTES] = if_info.bytes_recv
            interface[TX_BYTES] = if_info.bytes_sent
            dict_nics[if_name] = interface

        return dict_nics

    def join_dicts(self, if_static_data, if_dynamic_data):
        """Merges param1 with param2.

        param1: dictionary containing TYPE, IPADDR, MAC, SPEED, MTU and UP keys.
        param2: dictionary containing RX/TX_PKTS, RX/TX_PKTS, RX/TX_DROPS and RX/TX_BYTES keys.

        return: param1 which contains merged values of param2.
        """
        dict_nics = if_static_data
        for if_name, if_info in dict_nics.items():
            if if_name in if_dynamic_data:
                if_info.update(if_dynamic_data[if_name])
        return dict_nics

    def add_aggregate(self, dict_nics):
        """Function to get aggregate of all physical nics."""
        total_rx_pkts = 0
        total_tx_pkts = 0
        total_rx_drops = 0
        total_tx_drops = 0
        total_rx_bytes = 0
        total_tx_bytes = 0

        for if_name, if_info in dict_nics.items():
            if if_info[TYPE] == PHY:
                total_rx_pkts = total_rx_pkts + if_info[RX_PKTS]
                total_tx_pkts = total_tx_pkts + if_info[TX_PKTS]
                total_rx_drops = total_rx_drops + if_info[RX_DROPS]
                total_tx_drops = total_tx_drops + if_info[TX_DROPS]
                total_rx_bytes = total_rx_bytes + if_info[RX_BYTES]
                total_tx_bytes = total_tx_bytes + if_info[TX_BYTES]

        interface = {}
        #interface[NICNAME]  = AGGREGATE
        interface[TYPE] = AGGREGATE
        interface[AGG + RX_PKTS] = total_rx_pkts
        interface[AGG + TX_PKTS] = total_tx_pkts
        interface[AGG + RX_DROPS] = total_rx_drops
        interface[AGG + TX_DROPS] = total_tx_drops
        interface[AGG + RX_BYTES] = total_rx_bytes
        interface[AGG + TX_BYTES] = total_tx_bytes
        dict_nics[AGGREGATE] = interface

    def add_common_params(self, dict_nics):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        timestamp = time.time()

        for if_name, if_info in dict_nics.items():
            if_info[TIMESTAMP] = timestamp
            if_info[PLUGIN] = IF_STATS
            if_info[PLUGIN_INS] = if_name

    def add_rate(self, dict_nics):
        """Function to get RX_RATE and TX_RATE. Rate is not calculated for virtaual nics."""
        for if_name, if_info in dict_nics.items():
            if self.prev_data and if_name in self.prev_data:
                if if_info[TYPE] == PHY:
                    rate = utils.get_rate(
                        RX_BYTES, if_info, self.prev_data[if_name])
                    if rate != NAN:
                        if_info[RX_RATE] = round(
                            (rate / (FACTOR * FACTOR)), FLOATING_FACTOR)
                    rate = utils.get_rate(
                        TX_BYTES, if_info, self.prev_data[if_name])
                    if rate != NAN:
                        if_info[TX_RATE] = round(
                            (rate / (FACTOR * FACTOR)), FLOATING_FACTOR)
                elif if_info[TYPE] == AGGREGATE:
                    rate = utils.get_rate(
                        AGG + RX_BYTES, if_info, self.prev_data[if_name])
                    if rate != NAN:
                        if_info[AGG + RX_RATE] = round(
                            (rate / (FACTOR * FACTOR)), FLOATING_FACTOR)
                    rate = utils.get_rate(
                        AGG + TX_BYTES, if_info, self.prev_data[if_name])
                    if rate != NAN:
                        if_info[AGG + TX_RATE] = round(
                            (rate / (FACTOR * FACTOR)), FLOATING_FACTOR)

    def collect_data(self):
        """Collects all data."""
        # get static data of interfaces
        if_static_data = self.get_static_data()
        if not if_static_data:
            collectd.error(
                "Plugin nic_stats: Unable to fetch static data for interfaces.")
            return None

        # get dynamic data
        if_dynamic_data = self.get_dynamic_data()
        if not if_dynamic_data:
            collectd.error(
                "Plugin nic_stats: Unable to fetch dynamic data for interfaces.")
            return None

        # join data
        dict_nics = self.join_dicts(if_static_data, if_dynamic_data)

        # Add aggregate key
        self.add_aggregate(dict_nics)
        collectd.info(
            "Plugin nic_stats: Added aggregate information successfully.")
        # Add common parameters
        self.add_common_params(dict_nics)
        collectd.info(
            "Plugin nic_stats: Added common parameters successfully.")
        # calculate rate
        self.add_rate(dict_nics)
        collectd.info(
            "Plugin nic_stats: Calculated and added rate parameters successfully.")
        # set previous  data to current data
        self.prev_data = dict_nics
        return dict_nics

    def dispatch_data(self, dict_nics):
        """Dispatches dictionary to collectd."""
        for if_name, if_info in dict_nics.items():
            collectd.info("Plugin nic_stats: Successfully sent to collectd.")
            collectd.debug("Plugin nic_stats: Values: " + json.dumps(if_info))
            utils.dispatch(if_info)

    def read(self):
        """Validates if dictionary is not null."""
        dict_nics = self.collect_data()
        if not dict_nics:
            return

        # dispatch data to collectd
        self.dispatch_data(dict_nics)

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


collectd.register_init(init)
OBJ = NicStats()
collectd.register_config(OBJ.read_config)
collectd.register_read(OBJ.read_temp)
