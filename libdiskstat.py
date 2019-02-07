"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
"""
python script to return disk I/O statistics as a dict of raw tuples
"""

import re
from collections import namedtuple
import collectd
import utils
from constants import *

IoTuple = namedtuple(
    'io', 'read_count write_count read_bytes write_bytes read_mb write_mb read_time write_time')


def get_part_to_disk():
    """Function to get disk and partitions from lsblk command."""
    dict_disk_part = {}
    (lsblk, err) = utils.get_cmd_output('lsblk -bno  KNAME,TYPE | grep \'disk\|part\'')
    if err:
        collectd.error(
            'Plugin disk_stat in libdiskstat: error in lsblk command')
        return FAILURE
    disk_info = re.split('\n', lsblk)
    index = 0
    num_lines = len(disk_info)
    dname = None
    while index < num_lines:
        line = disk_info[index]
        if line:
            parts = re.split(r'\s+', line.strip())
            if parts[1] == 'disk':
                dname = parts[0]
            dict_disk_part[parts[0]] = dname
            index += 1
        else:
            break

    if not dict_disk_part:
        return FAILURE

    return dict_disk_part


def get_sector_size(disk):
    """Function to get sector size of disk."""
    try:
        with open(b"/sys/block/%s/queue/hw_sector_size" % disk) as size_file:
            return int(size_file.read())
    except (IOError, ValueError):
        # default
        return 512


def disk_io_counters():
    """Return disk I/O statistics for every disk installed on the
    system as a dict of raw tuples.
    """
    retdict = {}
    disk_part = get_part_to_disk()
    if disk_part == FAILURE:
        return FAILURE

    try:
        with open("/proc/diskstats") as diskstats_file:
            lines = diskstats_file.readlines()
    except IOError:
        collectd.error(
            "libdiskstat library: Could not read file '/proc/diskstats'")
        return FAILURE

    for line in lines:
        fields = line.split()
        fields_len = len(fields)
        if fields_len == 15:
            # Linux 2.4
            name = fields[3]
            reads = int(fields[2])
            (reads_merged, rsector, rtime, writes, writes_merged,
             wsector, wtime, _, busy_time, _) = map(int, fields[4:14])
        elif fields_len == 14:
            # Linux 2.6+, line referring to a disk
            name = fields[2]
            (reads, reads_merged, rsector, rtime, writes, writes_merged,
             wsector, wtime, _, busy_time, _) = map(int, fields[3:14])
        elif fields_len == 7:
            # Linux 2.6+, line referring to a partition
            name = fields[2]
            reads, rsector, writes, wsector = map(int, fields[3:])
            rtime = wtime = reads_merged = writes_merged = busy_time = 0
        else:
            raise ValueError(
                "libdiskstat library: not sure how to interpret line %r in /proc/diskstats" % line)

        if name in disk_part:
            sector_size = get_sector_size(disk_part[name])
            rbytes = rsector * sector_size
            wbytes = wsector * sector_size
            rmb = rbytes * float(0.000001)
            wmb = wbytes * float(0.000001)
            retdict[name] = IoTuple(reads, writes, rbytes, wbytes, rmb, wmb, rtime, wtime)
    return retdict
