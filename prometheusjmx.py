"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
""" A collectd-python plugin for retrieving
    metrics from prometheus JMX status module. """

import signal
import collectd
import prometheus_poller
from constants import *


class PrometheusJmx(prometheus_poller.PrometheusStat):
    def __init__(self):
        self.interval = DEFAULT_INTERVAL
        self.port = None
        self.conf = {}

    def config(self, cfg):
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == PORT:
                self.port = children.values[0]
        self.conf.update({'interval': self.interval, 'port': self.port, 'name': 'prometheusjmx'})
        super(PrometheusJmx, self).__init__(self.conf)

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


def init():
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)


OBJ = PrometheusJmx()
collectd.register_init(init)
collectd.register_config(OBJ.config)
collectd.register_read(OBJ.read_temp)
