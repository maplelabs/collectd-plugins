""" A collectd-python plugin for retrieving
    metrics from Apache access log file. """

import collectd
import subprocess
import json
import time
from utils import *
from constants import *
from os import access, R_OK
from threading import Thread
from datetime import datetime
from Queue import Queue, Empty
from apache_log_parser import make_parser, LineDoesntMatchException

key_map = {
    "local_ip": "localIP",
    "remote_ip": "remoteIP",
    "response_bytes": "respBytes",
    "time_s": "respTime",
    "remote_host": "remoteHost",
    "protocol": "reqProtocol",
    "server_port": "port",
    "status": "status",
    "time_received_tz_isoformat": "reqTime",
    "request_first_line": "firstLineReq",
    "url_path": "urlPath",
}


class LogWatch(Thread):
    """ Thread watching a log file for appended lines """

    def __init__(self, logfile, line_queue):
        Thread.__init__(self)
        self.killed = False
        self.logfile = logfile
        self.line_queue = line_queue

    def run(self):
        """ Main thread entrypoint """
        self.tail()

    def tail(self):
        """ Watch for, and enqueue lines appended to a file """
        try:
            tail = subprocess.Popen(
                    ["tail", "-f", self.logfile], stdout=subprocess.PIPE)
            while not self.killed:
                line = tail.stdout.readline()
                self.line_queue.put(line)
                if not line:
                    collectd.info("No logs are present in the access log file of apache")
                    break
        except Exception as e:
            collectd.info(e)
            return

class ApacheLog:
    """ Main plugin class """

    def __init__(self):
        """ Initialize itself with sane defaults """
        self.parser = None
        self.values = {}
        self.logwatch = None
        self.interval = 1
        self.access_log = DEFAULT_LOG_FILE
        self.access_log_format = DEFAULT_LOG_FORMAT
        self.line_buffer = Queue()

    def configure(self, conf):
        """ Receive and process configuration block from collectd """
        for node in conf.children:
            key = node.key.lower()
            val = node.values[0]

            if key == 'accesslog':
                self.access_log = val
                if not access(self.access_log, R_OK):
                    collectd.error('AccessLog %s is not readable!' % self.access_log)
            elif key == 'name':
                self.plugin_name = val
            elif key == 'interval':
                self.interval = val
            else:
                self.warn('Unknown config key: %s.' % key)
            self.parser = make_parser(self.access_log_format)

    def init(self):
        """ Prepare itself for reading in new values """
        if not self.logwatch:
            self.logwatch = LogWatch(self.access_log, self.line_buffer)
            self.logwatch.start()

    def gather_metrics(self):
        global key_map
        """ Gather metrics data from lines queued by self.logwatch """
        read_start = datetime.now()

        while (self.logwatch.isAlive() and
                       (datetime.now() - read_start).seconds < self.interval):
            try:
                line = self.line_buffer.get_nowait()
            except Empty:
                break
            try:
                request = self.parser(line)
            except Exception as e:
                collectd.error("Apache2 Log Format doesn't match with the plugins custom format : %s" % e)
                collectd.info("Change the Default Log format in the apache2 configuration file to the provided "
                              "custom format: %s" % self.access_log_format)
                return
            self.values = dict([(key_map[key], value) for key, value in request.items() if (key in key_map.keys())])
            collectd.info("values:%s" % self.values)
            if self.values:
                for key, value in self.values.items():
                    try:
                        self.values[key] = float(value)
                    except ValueError:
                        continue
            else:
                collectd.info("Data is not present to get the key value pair")
                return

    def add_common_params(self):
        hostname = gethostname()
        timestamp = time.time()
        self.values[PLUGIN] = APACHE_TRANS
        self.values[PLUGIN_INS] = P_INS_ALL
        self.values[HOSTNAME] = hostname
        self.values[TIMESTAMP] = timestamp
        collectd.info("Plugin apache_trans: Added common parameters successfully")

    def dispatch_data(self):
        collectd.debug("Plugin apache_trans: Values dispatched = " + json.dumps(self.values))
        dispatch(self.values)

    def read(self):
        """ Collectd read callback to gather metrics
            data from the access log and submit them """
        try:
            self.init()
            self.gather_metrics()
            if len(self.values) > 0:
                self.add_common_params()
                self.dispatch_data()
            else:
                collectd.error("No values are present to dispatch")
                return
        except Exception as e:
            collectd.error("Couldn't gather metrics due to the exception %s" % e)
            return

    def shutdown(self):
        """ Collectd plugin shutdown callback """
        self.logwatch.killed = True
        self.logwatch.join(1)


a_log = ApacheLog()
collectd.register_config(a_log.configure)
collectd.register_read(a_log.read)
collectd.register_shutdown(a_log.shutdown)
