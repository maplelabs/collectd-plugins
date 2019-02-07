"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
"""Class to handle jolokia related operations"""

import os
import re
import time
import subprocess
import socket
import requests
import threading
import collectd
from pyjolokia import Jolokia
from contextlib import closing

#constants
JOLOKIA_PATH = "/opt/collectd/plugins/"

def synchronized(func):
    """Synchronise to prevent race condition which can occur
       when kafkajmx and kafkatopic plugin try to start jolokia with same process."""
    func.__lock__ = threading.Lock()

    def synced_func(*args, **kws):
        with func.__lock__:
            return func(*args, **kws)
    return synced_func

def get_cmd_output(cmd, shell_value=True, stdout_value=subprocess.PIPE,
                   stderr_value=subprocess.PIPE):
    """Returns subprocess output and return code of cmd passed in argument."""
    call = subprocess.Popen(cmd, shell=shell_value,
                            stdout=stdout_value, stderr=stderr_value)
    call.wait()
    status, err = call.communicate()
    returncode = call.returncode
    return status, err, returncode


class JolokiaClient(object):

    def __init__(self, plugin_name, process_name):
        self.plugin_name = plugin_name
        self.process_name = process_name

    def check_prerequiste(self):
        """Need to run plugin as root."""
        if not os.geteuid() == 0:
            collectd.error("Please run collectd as root. %s plugin requires root privileges" % self.plugin_name)
            return False
        return True

    @staticmethod
    def get_jolokia_inst(port):
        """Returns instance of jolokia"""
        jolokia_url = "http://127.0.0.1:%s/jolokia/" % port
        jolokiaclient = Jolokia(jolokia_url)
        return jolokiaclient

    @staticmethod
    def get_free_port():
        """get free port to run jolokia client."""
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sockt:
            sockt.bind(('localhost', 0))
            return sockt.getsockname()[1]

    def get_pid(self):
        """Get PIDs of the java process."""
        pid_cmd = "jcmd | awk '{print $1 \" \" $2}' | grep -w \"%s\"" % self.process_name
        pids, err, _ = get_cmd_output(pid_cmd)
        if err:
            collectd.error("Plugin %s: Error in collecting pid for process %s: %s" % (self.plugin_name, self.process_name, err))
            return False
        pids = pids.splitlines()
        pid_list = []
        for pid in pids:
            if pid is not "":
                pidval = pid.split()
                pid_list.append(pidval[0])
        collectd.debug("Plugin %s: PID(s) of %s process: %s" % (self.plugin_name, self.process_name, pid_list))
        return pid_list

    def get_uid_of_pid(self, pid):
        """Jolokia needs to be run with same user of the process attached"""
        uid_cmd = "awk '/^Uid:/{print $2}' /proc/%s/status" % pid
        uid, err, _ = get_cmd_output(uid_cmd)
        if err:
            collectd.error("Plugin %s: Failed to retrieve uid for pid %s, %s" % (self.plugin_name, pid, err))
            return False
        return uid.strip()

    def run_jolokia_cmd(self, cmd, pid, port=None):
        """Common logic to run jolokia cmds."""
        process_uid = self.get_uid_of_pid(pid)
        jolokia_cmd = "sudo -u '#{0}' java -jar {1}jolokia.jar --host=127.0.0.1 {2} {3}".format(process_uid, JOLOKIA_PATH, cmd, pid)
        if port:
            jolokia_cmd += " --port=%s" % port
        return get_cmd_output(jolokia_cmd)

    @synchronized
    def get_jolokia_port(self, pid):
        """check if jmx jolokia agent already running, if running get port"""
        status, err, ret = self.run_jolokia_cmd("status", pid)
        if err or ret:
            port = self.get_free_port()
            status, err, ret = self.run_jolokia_cmd("start", pid, port)
            if err or ret:
                collectd.error("Plugin kafka_topic: Failed to start jolokia. Retrying again for pid %s" % pid)
                # sleep for some seconds and check again [Sometimes Jolokia on first time connection throws port occupied
                # error however jolokia is connected in background. Possible bug in jolokia]
                time.sleep(1)
                self.run_jolokia_cmd("start", pid, port)
                status, err, ret = self.run_jolokia_cmd("status", pid)
                if err or ret:
                    collectd.error("Plugin kafka_topic: Failed to start jolokia for pid %s" % pid)
                    return False
            else:
                collectd.info("Plugin %s: Jolokia client started for pid %s and listening in port %s" % (self.plugin_name, pid, port))
                return port

        # jolokia id already running and return port
        jolokiaip = status.splitlines()[1]
        port = re.findall('\d+', jolokiaip.split(':')[2])[0]
        collectd.info("Plugin %s: Jolokia client is already running for pid %s in IP %s" % (self.plugin_name, pid, jolokiaip))
        return port

    def connection_available(self, port):
        """Check if jolokia client is up."""
        try:
            jolokia_url = "http://127.0.0.1:%s/jolokia/version" % port
            resp = requests.get(jolokia_url)
            if resp.status_code == 200:
                collectd.debug("Plugin %s: Jolokia Connection available in port %s" % (self.plugin_name, port))
                return True
        except requests.exceptions.ConnectionError:
            return False
