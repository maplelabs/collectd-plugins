"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
"""Python plugin for collectd to fetch disk statistics information."""

#!/usr/bin/python
import signal
import time
import re
import json
from copy import deepcopy
import collectd

# user imports
import utils
import libdiskstat
from constants import *


class DiskStats(object):
    """Plugin object will be created only once and collects capacity, used, type, mount,
    readThroughput, readIOPS, writeThroughput, writeIOPS and aggregate info every interval."""

    def __init__(self):
        """Initializes interval and previous dictionary variable."""
        self.interval = DEFAULT_INTERVAL
        self.prev_data = {}

    def read_config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]

    def get_disk_info(self):
        """Function to get name, type, size and mountpoint info of disk and partition."""
        (lsblk, err) = utils.get_cmd_output(
            'lsblk -bno  KNAME,TYPE,SIZE,MOUNTPOINT | grep \'disk\|part\'')
        if err:
            collectd.error('Plugin disk_stat: error in lsblk command')
            return None

        disk_info = re.split('\n', lsblk)
        return disk_info

    def get_static_data(self):
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
                    disk[MOUNTPOINT] = ""
                if disk[MOUNTPOINT] is "" or SWAP not in disk[MOUNTPOINT]:
                    dict_disk[parts[0]] = disk
                disk[USAGE] = 0
                index += 1
            else:
                break

        return dict_disk

    def get_iostat_data(self):
        """Returns dictionary with values avgqu_sz  """
        (iodata, err) = utils.get_cmd_output('iostat -d -x')
        if err:
            collectd.error('Plugin disk_stat: error in iostat command or sysstat is not installed')
            return None

        iodata_info = re.split('\n', iodata)
        iodata = {}
        for statline in iodata_info[3:]:
            if statline:
                iodata[statline.split()[0]] = statline.split()[8]
        return iodata

    def get_dynamic_data(self):
        """Returns dictionary with values of READBYTE, WRITEBYTE, READCOUNT, WRITECOUNT."""
        dict_disk = {}
        dict_iostats = self.get_iostat_data()
        if not dict_iostats:
           collectd.insert('Plugin disk_stat: error in iostat command or sysstat is not installed')
           return None
        disk_part_io = libdiskstat.disk_io_counters()
        if disk_part_io == FAILURE:
            return None

        for name, disk_ioinfo in disk_part_io.items():
            avgqusizedata = dict_iostats.get(name)
            if avgqusizedata:
               avgqusizedata = float(avgqusizedata)
            else:
               avgqusizedata = 0.0
            disk = {READBYTE: float(disk_ioinfo.read_bytes) / (FACTOR), WRITEBYTE: float(
                disk_ioinfo.write_bytes) / (FACTOR), READCOUNT: disk_ioinfo.read_count,
                    WRITECOUNT: disk_ioinfo.write_count, READTIME: disk_ioinfo.read_time,
                    WRITETIME: disk_ioinfo.write_time,USAGE: 0,AVGQUEUESIZE: avgqusizedata}
            dict_disk[name] = disk

        return dict_disk

    def join_dicts(self, disk_static_data, disk_dynamic_data):
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

    def add_aggregate(self, dict_disks):
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

        disk[AVGQUEUESIZE] = 0.0
        dict_disks[AGGREGATE] = disk

    def add_common_params(self, dict_disks):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        timestamp = int(round(time.time() * 1000))

        for disk_name, disk_info in dict_disks.items():
            disk_info[TIMESTAMP] = timestamp
            disk_info[PLUGIN] = LINUX
            disk_info[PLUGINTYPE] = "disk_stats"
            disk_info[ACTUALPLUGINTYPE] = DISKSTAT
            disk_info[DISK_NAME] = disk_name

    def add_rate(self, dict_disks):
        """Function to get READTHROUGHPUT, READIOPS, WRITETHROUGHPUT and WRITEIOPS."""
        for disk_name, disk_info in dict_disks.items():
            if self.prev_data and disk_name in self.prev_data:
                if disk_name != "aggregate":
                    rate = utils.get_rate(
                        READBYTE, disk_info, self.prev_data[disk_name])
                    if rate != NAN:
                        disk_info[READTHROUGHPUT] = round(
                            rate, FLOATING_FACTOR)
                    rate = utils.get_rate(WRITEBYTE, disk_info,
                                          self.prev_data[disk_name])
                    if rate != NAN:
                        disk_info[WRITETHROUGHPUT] = round(
                            rate, FLOATING_FACTOR)
                    rate = utils.get_rate(READCOUNT, disk_info,
                                          self.prev_data[disk_name])
                    if rate != NAN:
                        disk_info[READIOPS] = round(rate, FLOATING_FACTOR)
                    rate = utils.get_rate(WRITECOUNT, disk_info,
                                          self.prev_data[disk_name])
                    if rate != NAN:
                        disk_info[WRITEIOPS] = round(rate, FLOATING_FACTOR)
                else:
                    rate = utils.get_rate(
                        READBYTE, disk_info, self.prev_data[disk_name])
                    if rate != NAN:
                        disk_info[AGG +
                                  READTHROUGHPUT] = round(rate, FLOATING_FACTOR)
                    rate = utils.get_rate(WRITEBYTE, disk_info,
                                          self.prev_data[disk_name])
                    if rate != NAN:
                        disk_info[AGG +
                                  WRITETHROUGHPUT] = round(rate, FLOATING_FACTOR)
                    rate = utils.get_rate(READCOUNT, disk_info,
                                          self.prev_data[disk_name])
                    if rate != NAN:
                        disk_info[AGG +
                                  READIOPS] = round(rate, FLOATING_FACTOR)
                    rate = utils.get_rate(WRITECOUNT, disk_info,
                                          self.prev_data[disk_name])
                    if rate != NAN:
                        disk_info[AGG +
                                  WRITEIOPS] = round(rate, FLOATING_FACTOR)

    def add_differential_value(self, dict_disks):
        for disk_name, disk_info in dict_disks.items():
            if self.prev_data and disk_name in self.prev_data:
                disk_info[READDATA] = disk_info[READBYTE] - self.prev_data[disk_name][READBYTE]
                disk_info[WRITEDATA] = disk_info[WRITEBYTE] - self.prev_data[disk_name][WRITEBYTE]
                disk_info[READIOCOUNT] = disk_info[READCOUNT] - self.prev_data[disk_name][READCOUNT]
                disk_info[WRITEIOCOUNT] = disk_info[WRITECOUNT] - self.prev_data[disk_name][WRITECOUNT]

    def add_latency(self, dict_disks):
        for disk_name, disk_info in dict_disks.items():
            if self.prev_data and disk_name in self.prev_data:
                try:
                    disk_info[READLATENCY] = (disk_info[READTIME] - self.prev_data[disk_name][READTIME]) / (
                    disk_info[READCOUNT] - self.prev_data[disk_name][READCOUNT])
                except ZeroDivisionError as err:
                    disk_info[READLATENCY] = 0
                try:
                    disk_info[WRITELATENCY] = (disk_info[WRITETIME] - self.prev_data[disk_name][WRITETIME]) / (
                    disk_info[WRITECOUNT] - self.prev_data[disk_name][WRITECOUNT])
                except ZeroDivisionError as err:
                    disk_info[READLATENCY] = 0

    def collect_data(self):
        """Collects all data."""
        # get static data of disk and part
        disk_static_data = self.get_static_data()
        if not disk_static_data:
            collectd.error(
                "Plugin disk_stat: Unable to fetch static data for disk and partition")
            return None

        # get dynamic data
        disk_dynamic_data = self.get_dynamic_data()
        if not disk_dynamic_data:
            collectd.info(
                "Plugin disk_stat: Unable to fetch dynamic data for disk and partition")
            return None

        # join data
        dict_disks = self.join_dicts(disk_static_data, disk_dynamic_data)

        # Add aggregate key
        self.add_aggregate(dict_disks)
        collectd.info(
            "Plugin disk_stat: Added aggregate information successfully.")
        # Add common parameters
        self.add_common_params(dict_disks)
        collectd.info(
            "Plugin disk_stat: Added common parameters successfully.")
        # calculate rate
        self.add_rate(dict_disks)
        #calculate differential values READDATA, WRITEDATA, READIOCOUNT, WRITEIOCOUNT
        self.add_differential_value(dict_disks)
        # calculate latency
        self.add_latency(dict_disks)
        collectd.info(
            "Plugin disk_stat: Calculated and added rate parameters successfully.")
        # set previous  data to current data
        self.prev_data = dict_disks
        return dict_disks

    def dispatch_data(self, dict_disks_copy):
        """Dispatches dictionary to collectd."""
        for disk_name, disk_info in dict_disks_copy.items():
            # delete readbyte, writebyte, readcount and writecount field
            del dict_disks_copy[disk_name][READBYTE], dict_disks_copy[disk_name][
                WRITEBYTE], dict_disks_copy[disk_name][READCOUNT], dict_disks_copy[disk_name][
                    WRITECOUNT]
            collectd.info("Plugin disk_stat: Successfully sent to collectd.")
            collectd.debug("Plugin disk_stat: Values: " +
                           json.dumps(disk_info))
            utils.dispatch(disk_info)

    def read(self):
        """Validates if dictionary is not null."""
        # collect data
        dict_disks = self.collect_data()
        if not dict_disks:
            return

        # dispatch data to collectd, copying by value
        self.dispatch_data(deepcopy(dict_disks))

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


OBJ = DiskStats()
collectd.register_config(OBJ.read_config)
collectd.register_read(OBJ.read_temp)
