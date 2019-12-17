"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""

import json
import time
import collectd
import re
import signal
import os
import traceback
import lxml.etree as et

# user imports
import utils
from constants import *


class jmeterStats(object):
    """Plugin object will be created only once and collects utils
           and available CPU/RAM usage info every interval."""

    def __init__(self, interval=1, utilize_type="CPU", maximum_grep=5, process_name='*'):
        """Initializes interval."""
        self.interval = DEFAULT_INTERVAL
        self.buf = ''
        self.path = ''
        self.length = 0

    def config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == "listener_path":
                self.path = children.values[0]

    def get_sample(self, elem, children=()):
        try:
            sample = {}
            #sample['allThreads'] = int(elem.get('na', 0))
            sample['bytesRec'] = int(elem.get('by', 0))
            #sample['dataEncoding'] = elem.get('de', '')
            #sample['dataType'] = elem.get('dt', '')
            sample['elapsedTime'] = int(elem.get('t', 0))
            sample['errorCount'] = int(elem.get('ec', 0))
            sample['groupThreads'] = int(elem.get('ng', 0))
            #sample['hostname'] = elem.get('hn', '')
            sample['idleTime'] = int(elem.get('it', 0))
            #sample['label'] = elem.get('lb', '')
            sample['latencyTime'] = int(elem.get('lt', 0))
            sample['method'] = elem.findtext('method', '')
            sample['queryString'] = elem.findtext('queryString', '')
            sample['responseCode'] = elem.get('rc', '')
            #sample['responseData'] = elem.findtext('responseData', '')
            #sample['responseFilename'] = elem.findtext('responseFile', '')
            #sample['responseMessage'] = elem.get('rm', '')
            #sample['sampleCount'] = int(elem.get('sc', 0))
            sample['success'] = bool(elem.get('s') == 'true')
            sample['threadName'] = elem.get('tn', '')
            sample['time_stamp'] = int(elem.get('ts', 0))
            sample['url'] = elem.findtext('java.net.URL', '')

            if re.match("http:\/\/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+[: ][0-9]*\/opportunity\/[0-9]+", sample['url']):
                url_id = re.sub("/opportunity/[0-9]+", "/opportunity/[id]", sample['url'])
                sample['api'] = url_id

            elif re.match("http:\/\/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+[: ][0-9]*\/userDetails\/[0-9]+", sample['url']):
                url_id = re.sub("/userDetails/[0-9]+", "/userDetails/[id]", sample['url'])
                sample['api'] = url_id

            else:
                sample['api'] = sample['url']

            return sample

        except Exception as err:
            collectd.error("Plugin jmeter: Exception in get_sample due to %s" % err)

    def get_jmeter_data(self):
        try:
            sample_started = False
            sample_children = []

            parser = et.XMLPullParser(['start', 'end'])
            parser.feed(self.buf)

            for event, elem in parser.read_events():
                if event == 'start' and elem.tag == 'sample':
                    sample_started = True
                    sample_children = []
                elif event == 'end' and elem.tag == 'httpSample':
                    sample = self.get_sample(elem)
                    if sample_started:
                        sample_children.append(sample)
                    else:
                        return sample
                elif event == 'end' and elem.tag == 'sample':
                    sample = self.get_sample(elem, sample_children)
                    sample_started = False
                    return sample

        except Exception as err:
            collectd.error("Plugin jmeter: Exception in get_jmeter_data due to %s" % err)

    def add_common_params(self, jmeter_stats):
        """Adds TIMESTAMP, PLUGIN, PLUGITYPE to dictionary."""
        timestamp = int(round(time.time() * 1000))
        jmeter_stats[TIMESTAMP] = timestamp
        jmeter_stats[PLUGIN] = "jmeter"
        jmeter_stats[PLUGINTYPE] = "jmeter"
        jmeter_stats[ACTUALPLUGINTYPE] = "jmeter"
        # dict_jmx[PLUGIN_INS] = doc
        collectd.info("Plugin jmeter: Added common parameters successfully")

    def collect_dispatch_data(self):
        """Collect data for jmeter"""
        try:
            while(1):
                if not os.path.exists(self.path):
                    collectd.info("Plugin jmeter: Waiting for log file")
                    time.sleep(5)
                else:
                    break

            fp = open(self.path, "r")
            while True:
                if os.path.getsize(self.path) < self.length:
                    fp.seek(0)
                self.length = os.path.getsize(self.path)

                line = fp.readline()
                if line:
                    # collectd.info("Plugin jmeter: Line is -- %s" % line)

                    if "</testResults>" in line:
                        collectd.info("Plugin jmeter: Reached EOF")
                        fp.close()
                        os.remove(self.path)
                        return

                    if not re.search("xml version", line) and not re.search("testResults version", line):
                        self.buf = self.buf + line

                    else:
                        continue

                    if not re.search("</httpSample>", line):
                        continue

                    jmeter_stats = self.get_jmeter_data()
                    # collectd.info("Plugin jmeter: Stats are = %s" % str(jmeter_stats))
                    # self.add_common_params(jmeter_stats)
                    self.buf = ''

                    if not jmeter_stats:
                        collectd.error("Plugin jmeter: Unable to fetch data for jmeter")
                        return
                    else:
                        self.add_common_params(jmeter_stats)
                        self.dispatch_data(jmeter_stats)
                        #time.sleep(1)

                else:
                    time.sleep(1)
                    #collectd.info("Plugin jmeter: waiting for line")
                    continue

        except Exception as err:
            collectd.error("Plugin jmeter: Exception in collect_dispatch_data due to %s in %s" % (err, traceback.format_exc()))

    def read(self):
        """Collects all data."""
        try:
            self.collect_dispatch_data()

        except Exception as err:
            collectd.error("Plugin jmeter: Couldn't read and gather the metrics due to the exception %s in %s" % (err, traceback.format_exc()))

    def dispatch_data(self, result):
        """Dispatch data to collectd."""
        collectd.info("Plugin jmeter: Values dispatched =%s" % json.dumps(result))
        utils.dispatch(result)

    def read_temp(self):
        """Collectd first calls register_read. At that time default interval is taken,
        hence temporary function is made to call, the read callback is unregistered
        and read() is called again with interval obtained from conf by register_config callback."""
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))

def init():
    """When new process is formed, action to SIGCHLD is reset to default behavior."""
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)


OBJ = jmeterStats()
collectd.register_init(init)
collectd.register_config(OBJ.config)
collectd.register_read(OBJ.read_temp)

