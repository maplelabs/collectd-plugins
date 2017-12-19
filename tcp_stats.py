"""
Python plugin for collectd to fetch hostname, tcpWin, tcpReset, tcpRetrans and timestamp
in one single document.
"""

#!/usr/bin/python

import signal
import json
import time
import collectd

# user imports
import utils
from constants import *



class TcpStats(object):
    """Plugin object will be created only once and collects tcpWin, tcpReset,
    tcpRetrans info every interval."""

    def __init__(self):
        """Initializes interval."""
        self.interval = DEFAULT_INTERVAL

    def read_config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]

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

    def add_common_params(self, dict_tcp):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        timestamp = int(round(time.time()))
        dict_tcp[TIMESTAMP] = timestamp
        dict_tcp[PLUGINTYPE] = TCP
        dict_tcp[ACTUALPLUGINTYPE] = TCP
        dict_tcp[PLUGIN] = LINUX
        #dict_tcp[PLUGIN_INS] = P_INS_ALL

    def collect_data(self):
        """Validates if dictionary is not null.If yes then returns None."""
        dict_tcp = self.get_tcp_buffersize()
        if not dict_tcp:
            collectd.error("Plugin tcp_stats: Unable to fetch data for tcp.")
            return None

        self.add_common_params(dict_tcp)

        return dict_tcp

    def dispatch_data(self, dict_tcp):
        """Dispatches dictionary to collectd."""
        collectd.info("Plugin tcp_stats: Successfully sent to collectd.")
        collectd.debug("Plugin tcp_stats: Values :" + json.dumps(dict_tcp))
        utils.dispatch(dict_tcp)

    def read(self):
        """Collects all data for interval registered in read callback."""
        dict_tcp = self.collect_data()
        if not dict_tcp:
            return

        # dispatch data to collectd
        self.dispatch_data(dict_tcp)

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


OBJ = TcpStats()
collectd.register_config(OBJ.read_config)
collectd.register_read(OBJ.read_temp)
