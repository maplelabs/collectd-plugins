"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
"""Python plugin for collectd to get ram utilization and available ram."""


#!/usr/bin/python
import signal
import json
import time
import psutil
import collectd
import datetime
import socket
import re
import copy
from copy import deepcopy


# user imports
import utils
import libdiskstat
from constants import *


class LinuxStats(object):
    """Plugin object will be created only once and collects utils
       and available Linux plugin info every interval."""

    def __init__(self):
        """Initializes interval."""
        self.interval = DEFAULT_INTERVAL
        self.prev_disk_data = dict()
        self.prev_nic_data = dict()
        self.first_poll = True

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
            'total': mem.total}
        return dict_ram_util
    
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
            #dict_cpu_util[CORE + str(i)] = per_cpu_util[i - 1]
            if LOW_RANGE_BEGIN <= per_cpu_util[i - 1] <= LOW_RANGE_END:
                dict_cpu_util[NUM_LOW_ACTIVE] += 1
            elif LOW_RANGE_END < per_cpu_util[i - 1] <= MEDIUM_RANGE_END:
                dict_cpu_util[NUM_MEDIUM_ACTIVE] += 1
            elif MEDIUM_RANGE_END < per_cpu_util[i - 1] <= HIGH_RANGE_END:
                dict_cpu_util[NUM_HIGH_ACTIVE] += 1

        return dict_cpu_util



    def get_retransmit_and_reset(self):
        """
        Function to get tcp_reset and tcp_retrans values.
        """
        retrans = resets_recv = resets_sent = 0
        try:
            with open("/proc/net/snmp") as snmp_file:
                snmp_output = snmp_file.readlines()
        except IOError:
            collectd.error(
                "Plugin tcp_stats: Could not open file : /proc/net/snmp")
            return FAILURE, None

        flag = 0
        for i in range(0, len(snmp_output)):
            if 'Tcp:' not in snmp_output[i]:
                continue
            if flag:
                tcp_stat = snmp_output[i].split(" ")
                resets_recv = tcp_stat[8]
                retrans = tcp_stat[12]
                resets_sent = tcp_stat[14]
                return SUCCESS, [retrans, resets_sent, resets_recv]
            flag += 1

    def get_tcp_buffersize(self):
        """Returns dictionary with values of tcpWin(low, medium and high),
        tcpRetrans and tcpResets."""
        dict_tcp = {}

        try:
            with open("/proc/sys/net/ipv4/tcp_wmem") as tcp_wmem_file:
                wmem_lines = tcp_wmem_file.readline()
        except IOError:
            collectd.error(
                "Plugin tcp_stats: Could not open file : /proc/sys/net/ipv4/tcp_wmem")
            return None

        try:
            with open("/proc/sys/net/ipv4/tcp_rmem") as tcp_rmem_file:
                rmem_lines = tcp_rmem_file.readline()
        except IOError:
            collectd.error(
                "Plugin tcp_stats: Could not open file : /proc/sys/net/ipv4/tcp_rmem")
            return None


        read_low, read_medium, read_high = rmem_lines.split("\t")
        write_low, write_medium, write_high = wmem_lines.split("\t")

        dict_tcp[READ_TCPWIN_LOW] = round(
            float(read_low) / FACTOR, FLOATING_FACTOR)
        dict_tcp[READ_TCPWIN_MEDIUM] = round(
            float(read_medium) / FACTOR, FLOATING_FACTOR)
        dict_tcp[READ_TCPWIN_HIGH] = round(
            float(read_high) / FACTOR, FLOATING_FACTOR)
        collectd.info(
            "Plugin tcp_stats: TCP read buffer size got successfully")

        dict_tcp[WRITE_TCPWIN_LOW] = round(
            float(write_low) / FACTOR, FLOATING_FACTOR)
        dict_tcp[WRITE_TCPWIN_MEDIUM] = round(
            float(write_medium) / FACTOR, FLOATING_FACTOR)
        dict_tcp[WRITE_TCPWIN_HIGH] = round(
            float(write_high) / FACTOR, FLOATING_FACTOR)
        collectd.info(
            "Plugin tcp_stats: TCP write buffer size got successfully")

        (status, val_list) = self.get_retransmit_and_reset()
        if status == SUCCESS:
            collectd.info(
                "Plugin tcp_stats: TCP reset and retransmit values got successfully")

            tcp_resets = float(val_list[1]) + float(val_list[2])
            dict_tcp[TCPRETRANS] = int(val_list[0])
            dict_tcp[TCPRESET] = tcp_resets

        return dict_tcp



    def get_disk_info(self):
        """Function to get name, type, size and mountpoint info of disk and partition."""
        (lsblk, err) = utils.get_cmd_output(
            'lsblk -bno  KNAME,TYPE,SIZE,MOUNTPOINT | grep \'disk\|part\'')
        if err:
            collectd.error('Plugin disk_stat: error in lsblk command')
            return None

        disk_info = re.split('\n', lsblk)
        return disk_info

    def get_disk_static_data(self):
        """Returns dictionary with values of NAME, TYPE, CAPACITY and MOUNTPOINT."""
        dict_disk = {}
        list_disk = self.get_disk_info()
        if not list_disk:
            return None

        index = 0
        num_lines = len(list_disk)

        while index < num_lines:
            line = list_disk[index]
            if line:
                parts = re.split(r'\s+', line.strip())
                disk = {DISK_TYPE: parts[1], CAPACITY: round(
                    float(parts[2]) / (FACTOR * FACTOR * FACTOR), FLOATING_FACTOR)}
                if len(parts) == 4:
                    disk[MOUNTPOINT] = parts[3]
                else:
                    disk[MOUNTPOINT] = []
                if SWAP not in disk[MOUNTPOINT]:
                    dict_disk[parts[0]] = disk
                index += 1
            else:
                break

        return dict_disk

    def get_dick_dynamic_data(self):
        """Returns dictionary with values of READBYTE, WRITEBYTE, READCOUNT, WRITECOUNT."""
        dict_disk = {}
        disk_part_io = libdiskstat.disk_io_counters()
        if disk_part_io == FAILURE:
            return None

        for name, disk_ioinfo in disk_part_io.items():
            disk = {READBYTE: float(disk_ioinfo.read_bytes) / (FACTOR * FACTOR), WRITEBYTE: float(
                disk_ioinfo.write_bytes) / (FACTOR * FACTOR), READCOUNT: disk_ioinfo.read_count,
                    WRITECOUNT: disk_ioinfo.write_count, READTIME: disk_ioinfo.read_time,
                    WRITETIME: disk_ioinfo.write_time}
            dict_disk[name] = disk

        return dict_disk

    def disk_join_dicts(self, disk_static_data, disk_dynamic_data):
        """Merges param1 with param2.

        param1: dictionary containing NAME, TYPE, CAPACITY and MOUNTPOINT keys.
        param2: dictionary containing READBYTE, WRITEBYTE, READCOUNT, WRITECOUNT keys.

        return: param1 which contains merged values of param2.
        """
        disk_data = disk_static_data
        for disk_name, disk_info in disk_data.items():
            if disk_name in disk_dynamic_data:
                disk_info.update(disk_dynamic_data[disk_name])
        return disk_data

    def add_agg_capacity(self):
        """Function to get total capacity."""
        lsblk, err = utils.get_cmd_output(
            "lsblk -nbo KNAME,TYPE,SIZE | grep disk | grep -v fd | awk '{print $3}'")
        if err:
            collectd.error("Plugin disk_stat : error in lsblk command")
            return None

        sdlines = re.split("\n", lsblk)
        num_lines = len(sdlines)
        index = 0
        total_sum = 0
        while index < num_lines:
            line = sdlines[index]
            if line:
                total_sum += int(line)
                index += 1
            else:
                index += 1
        return round(float(total_sum) / (FACTOR * FACTOR * FACTOR), FLOATING_FACTOR)

    def add_agg_usage(self):
        """Function to get total usage."""
        (df_out, err) = utils.get_cmd_output(
            "df -kl |awk '/^\/dev\//' | awk '{print $3}'")
        if err:
            collectd.error('Plugin disk_stat: error in df command')
            return None

        splines = re.split('\n', df_out)
        unum_lines = len(splines)
        uindex = 0
        usage_sum = 0
        while uindex < unum_lines:
            line = splines[uindex]
            if line:
                usage_sum += int(line)  # usage_sum is in Kb
                uindex += 1
            else:
                uindex += 1
        return round(float((usage_sum * FACTOR)) / (FACTOR * FACTOR * FACTOR), FLOATING_FACTOR)

    def add_disk_aggregate(self, dict_disks):
        """Function to get aggregate of all disk. MOUNTPOINT key is not added"""
        total_readcount = 0
        total_writecount = 0
        total_readbytes = 0
        total_writebytes = 0
        total_readtime = 0
        total_writetime = 0

        for name, io_info in dict_disks.items():
            if io_info[DISK_TYPE] == DISK:
                total_readbytes += float(io_info[READBYTE])
                total_writebytes += float(io_info[WRITEBYTE])
                total_readcount += float(io_info[READCOUNT])
                total_writecount += float(io_info[WRITECOUNT])
                total_readtime += float(io_info[READTIME])
                total_writetime += float(io_info[WRITETIME])

        disk = {DISK_TYPE: AGGREGATE, READBYTE: float(total_readbytes), WRITEBYTE: float(total_writebytes),
                READCOUNT: float(total_readcount), WRITECOUNT: float(total_writecount),
                READTIME: float(total_readtime), WRITETIME: float(total_writetime)}
        agg_cap = self.add_agg_capacity()
        if agg_cap:
            disk[AGG + CAPACITY] = agg_cap
        agg_usage = self.add_agg_usage()
        if agg_usage:
            disk[AGG + USAGE] = agg_usage

        return disk

    def add_disk_rate(self, dict_disks):
        #Function to get READTHROUGHPUT, READIOPS, WRITETHROUGHPUT and WRITEIOPS."""
        #for items in dict_disks():
            #if self.prev_disk_data and disk_name in self.prev_disk_data:
             if self.prev_disk_data:
                    #disk_name = AGGREGATE
                    collectd.info("add rate start ")
                    rate = utils.get_rate(
                        READBYTE, dict_disks, self.prev_disk_data)
                    if rate != NAN:
                        dict_disks[AGG +
                                  READTHROUGHPUT] = round(rate, FLOATING_FACTOR)
                    rate = utils.get_rate(WRITEBYTE, dict_disks,
                                          self.prev_disk_data)
                    if rate != NAN:
                        dict_disks[AGG +
                                  WRITETHROUGHPUT] = round(rate, FLOATING_FACTOR)
                    rate = utils.get_rate(READCOUNT, dict_disks,
                                          self.prev_disk_data)
                    if rate != NAN:
                        dict_disks[AGG +
                                  READIOPS] = round(rate, FLOATING_FACTOR)
                    rate = utils.get_rate(WRITECOUNT, dict_disks,
                                          self.prev_disk_data)
                    if rate != NAN:
                        dict_disks[AGG +
                                  WRITEIOPS] = round(rate, FLOATING_FACTOR)

    def add_disk_latency(self, dict_disks):
        #for disk_name, disk_info in dict_disks.items():
            if self.prev_disk_data:
                try:
                    dict_disks[READLATENCY] = (dict_disks[READTIME] - self.prev_disk_data[READTIME]) / (
                    dict_disks[READCOUNT] - self.prev_disk_data[READCOUNT])
                except ZeroDivisionError as err:
                    dict_disks[READLATENCY] = 0
                try:
                    dict_disks[WRITELATENCY] = (dict_disks[WRITETIME] - self.prev_disk_data[WRITETIME]) / (
                    dict_disks[WRITECOUNT] - self.prev_disk_data[WRITECOUNT])
                except ZeroDivisionError as err:
                    dict_disks[READLATENCY] = 0


    def add_cpu_static_data(self):
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




    def add_common_params(self, dict_linux_stats):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
       
        timestamp = int(round(time.time() * 1000))
        dict_linux_stats[TIMESTAMP] = timestamp
        dict_linux_stats[PLUGIN] = "linux_stats"
        #dict_ram_util[PLUGINTYPE] = RAM
        dict_linux_stats[PLUGINTYPE] = LINUX
        dict_linux_stats[ACTUALPLUGINTYPE] = "linux_stats"
        #dict_linux_stats[PLUGIN_INS] = P_INS_ALL 
        #dict_ram_util[PLUGIN_INS] = P_INS_ALL

    

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

    def get_nic_static_data(self):
        """Returns dictionary with values of NIC_TYPE, IPADDR, MAC, SPEED, MTU and UP info."""
        dict_nics = {}  # will contain data for all interfaces
        for if_name, if_info in psutil.net_if_addrs().items():
            interface = {NIC_TYPE: VIRT}
        #    interface[NICNAME] = if_name
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
                    dict_nics[if_name][NIC_TYPE] = PHY
                dict_nics[if_name][SPEED] = round(
                    (float(if_info.speed) / (FACTOR * 8)), FLOATING_FACTOR)
                dict_nics[if_name][MTU] = if_info.mtu
                dict_nics[if_name][UP] = if_info.isup

        return dict_nics

    def get_nic_dynamic_data(self):
        """Returns dictionary with values of RX/TX_PKTS,RX/TX_PKTS,RX/TX_DROPS and RX/TX_BYTES."""
        dict_nics = {}
        for if_name, if_info in psutil.net_io_counters(pernic=True).items():
            interface = {RX_PKTS: if_info.packets_recv, TX_PKTS: if_info.packets_sent, RX_DROPS: if_info.dropin,
                         TX_DROPS: if_info.dropout, RX_BYTES: if_info.bytes_recv, TX_BYTES: if_info.bytes_sent}
        #    interface[NICNAME]  = if_name
            dict_nics[if_name] = interface

        return dict_nics

    def join_nic_dicts(self, if_static_data, if_dynamic_data):
        """Merges param1 with param2.

        param1: dictionary containing NIC_TYPE, IPADDR, MAC, SPEED, MTU and UP keys.
        param2: dictionary containing RX/TX_PKTS, RX/TX_PKTS, RX/TX_DROPS and RX/TX_BYTES keys.

        return: param1 which contains merged values of param2.
        """
        dict_nics = if_static_data
        for if_name, if_info in dict_nics.items():
            if if_name in if_dynamic_data:
                if_info.update(if_dynamic_data[if_name])
        return dict_nics




    def add_nic_aggregate(self, dict_nics):
        """Function to get aggregate of all physical nics."""
        total_rx_pkts = 0
        total_tx_pkts = 0
        total_rx_drops = 0
        total_tx_drops = 0
        total_rx_bytes = 0
        total_tx_bytes = 0

        for if_name, if_info in dict_nics.items():
            if not self.first_poll:
                if if_info[NIC_TYPE] == PHY:
                    total_rx_pkts = total_rx_pkts + if_info[RX_PKTS]
                    total_tx_pkts = total_tx_pkts + if_info[TX_PKTS]
                    total_rx_drops = total_rx_drops + if_info[RX_DROPS]
                    total_tx_drops = total_tx_drops + if_info[TX_DROPS]
                    total_rx_bytes = total_rx_bytes + if_info[RX_BYTES]
                    total_tx_bytes = total_tx_bytes + if_info[TX_BYTES]

        interface = {NIC_TYPE: AGGREGATE, AGG + RX_PKTS: total_rx_pkts, AGG + TX_PKTS: total_tx_pkts,
                     AGG + RX_DROPS: total_rx_drops, AGG + TX_DROPS: total_tx_drops, AGG + RX_BYTES: total_rx_bytes,
                     AGG + TX_BYTES: total_tx_bytes}
        #interface[NICNAME]  = AGGREGATE
        return interface



    def add_nic_rate(self, dict_nics):
        #Function to get RX_RATE and TX_RATE. Rate is not calculated for virtaual nics."""
        #for if_name, if_info in dict_nics.items():
            if not self.first_poll:
                    rate = utils.get_rate(
                        AGG + RX_BYTES, dict_nics, self.prev_nic_data)
                    if rate != NAN:
                        dict_nics[AGG + RX_RATE] = round(
                            (rate / (FACTOR * FACTOR)), FLOATING_FACTOR)
                    rate = utils.get_rate(
                        AGG + TX_BYTES, dict_nics, self.prev_nic_data)
                    if rate != NAN:
                        dict_nics[AGG + TX_RATE] = round(
                            (rate / (FACTOR * FACTOR)), FLOATING_FACTOR)





    def add_nic_data(self):
        # get static data of interfaces
        if_static_data = self.get_nic_static_data()
        if not if_static_data:
            collectd.error(
                "Plugin nic_stats: Unable to fetch static data for interfaces.")
            return None
        # get dynamic data
        if_dynamic_data = self.get_nic_dynamic_data()
        if not if_dynamic_data:
            collectd.error(
                "Plugin nic_stats: Unable to fetch dynamic data for interfaces.")
            return None

        # join data
        dict_nics = self.join_nic_dicts(if_static_data, if_dynamic_data)


        # Add aggregate key
        dict_nic_stats = self.add_nic_aggregate(dict_nics)

        # calculate rate
        self.add_nic_rate(dict_nic_stats)
        collectd.info(
            "Plugin nic_stats: Calculated and added rate parameters successfully.")

        final_dict = copy.deepcopy(dict_nic_stats)
        if not self.first_poll:
           # for if_name, if_info in dict_nic_stats.items():
                dict_nic_stats[RX_PKTS] = dict_nic_stats[RX_PKTS] - self.prev_nic_data[RX_PKTS]
                dict_nic_stats[TX_PKTS] = dict_nic_stats[TX_PKTS] - self.prev_nic_data[TX_PKTS]
                dict_nic_stats[RX_BYTES] = dict_nic_stats[RX_BYTES] - self.prev_nic_data[RX_BYTES]
                dict_nic_stats[TX_BYTES] = dict_nic_stats[TX_BYTES] - self.prev_nic_data[TX_BYTES]
                dict_nic_stats[RX_DROPS] = dict_nic_stats[RX_DROPS] - self.prev_nic_data[RX_DROPS]
                dict_nic_stats[TX_DROPS] = dict_nic_stats[TX_DROPS] - self.prev_nic_data[TX_DROPS]

        # set previous  data to current data
        self.prev_nic_data = final_dict

        if self.first_poll:
            #for if_name, if_info in dict_nic_stats.items():
                dict_nic_stats[RX_PKTS] = 0
                dict_nic_stats[TX_PKTS] = 0
                dict_nic_stats[RX_BYTES] = 0
                dict_nic_stats[TX_BYTES] = 0
                dict_nic_stats[RX_DROPS] = 0
                dict_nic_stats[TX_DROPS] = 0

        self.first_poll = False
        return dict_nic_stats

    def add_disk_data(self):
        disk_static_data = self.get_disk_static_data()
        if not disk_static_data:
            collectd.error(
                "Plugin disk_stat: Unable to fetch static data for disk and partition")
            return None

        # get dynamic data
        disk_dynamic_data = self.get_dick_dynamic_data()
        if not disk_dynamic_data:
            collectd.info(
                "Plugin disk_stat: Unable to fetch dynamic data for disk and partition")
            return None

        # join data
        dict_final = self.disk_join_dicts(disk_static_data, disk_dynamic_data)

        # get aggregated disk data
        timestamp = int(round(time.time() * 1000))
        dict_disk_stats = self.add_disk_aggregate(dict_final)
        dict_disk_stats[TIMESTAMP] = timestamp

        # calculate disk rate
        collectd.info("plugin rate started")
        self.add_disk_rate(dict_disk_stats)
        collectd.info("plugin rate ended")

        # calculate disk latency
        self.add_disk_latency(dict_disk_stats)
        collectd.info(
            "Plugin disk_stat latency: Calculated and added rate parameters successfully.")
        collectd.info("Disk data %s: " % str(dict_disk_stats))
        return dict_disk_stats
 

    def collect_data(self):
        """Validates if dictionary is not null.If null then returns None."""
        dict_linux = dict()

        # get ram  data
        dict_ram_util = self.add_ram_data()
        if not dict_ram_util:
            collectd.error("Plugin ram_util: Unable to fetch RAM information.")
            return None
        dict_linux.update(dict_ram_util)

        # get cpu_util data
        dict_cpu_util = self.add_cpu_data()
        if not dict_cpu_util:
            collectd.error("Plugin cpu_util: Unable to fetch CPU UTIL information.")
            return None
        dict_linux.update(dict_cpu_util)

        # get cpu_static data
        dict_cpu_static = self.add_cpu_static_data()
        if not dict_cpu_static:
            collectd.error("Plugin cpu_static: Unable to fetch CPU STATIC information.")
            return None
        dict_linux.update(dict_cpu_static)


        # set tcp stats
        #"""Validates if dictionary is not null.If yes then returns None."""
        dict_tcp = self.get_tcp_buffersize()
        if not dict_tcp:
            collectd.error("Plugin tcp_stats: Unable to fetch data for tcp.")
            return None
        dict_linux.update(dict_tcp)


        
        # set dict_disk_stats
        dict_disk_stats = self.add_disk_data()


        # set previous  data to current data
        self.prev_disk_data = dict_disk_stats

        #add disk dict to final linux dict
        dict_linux.update(dict_disk_stats)
        
        dict_nic_stats = self.add_nic_data()

        # set previous data to current data
        self.prev_nic_data = dict_nic_stats

        # add nic dict to final linux dict
        dict_linux.update(dict_nic_stats)

        # set common parametes to final linux dict
        self.add_common_params(dict_linux)
	return dict_linux


    def dispatch_data(self, linux_stats):
        """Dispatches dictionary to collectd."""
        collectd.info("Plugin linux_stats: Successfully sent to collectd.")
        collectd.info("Plugin linux_stats: Values dispatched = " +
                       json.dumps(linux_stats))
        utils.dispatch(linux_stats)
 

    def read(self):
        """Collects all data."""
        start_time = datetime.datetime.now()
        dict_linux = self.collect_data()
        end_time = datetime.datetime.now()
        elapsed_time = end_time - start_time
        if not dict_linux:
            return

        # dispatch data to collectd
        dict_linux["elapsed_time"] = elapsed_time.microseconds
        self.dispatch_data(dict_linux)

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


OBJ = LinuxStats()
collectd.register_config(OBJ.config)
collectd.register_read(OBJ.read_temp)
