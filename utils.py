"""Python scripts contains function for adding hostname to dictionary,
   dispatch to collectd and rate calculation."""


import subprocess
import write_json
import collectd
import socket
from constants import *


def gethostname():
    hostname = HOSTNAMEFAILURE
    try:
        hostname = socket.gethostname()
    except:
        pass
    return hostname

#def gethostname():
#    """Returns hostname."""
#    hostname = HOSTNAMEFAILURE
#    try:
#        with open("/etc/hostname") as hostname_file:
#            hostname = hostname_file.readline()
#            hostname = hostname.rstrip('\n')
#    except:
#        pass
#    return hostname


def dispatch(data_dict):
    """Dispatches data to collectd."""
    # add hostname to data_dict
    data_dict[HOSTNAME] = gethostname()

    # first dispatch to write json
    write_json.write(data_dict)

    # dispatch to other write functions
    metric = collectd.Values()
    metric.plugin = data_dict[ACTUALPLUGINTYPE]

    if PLUGIN_INS in data_dict:
        metric.plugin_instance = data_dict[PLUGIN_INS]
    if VAL_TYPE in data_dict:
        metric.type = data_dict[VAL_TYPE]
    else:
        metric.type = DUMMY
    if VAL_INS in data_dict:
        metric.type_instance = data_dict[VAL_INS]

    metric.meta = data_dict
    metric.values = [DUMMY_VAL]
    metric.dispatch()


def get_cmd_output(cmd, shell_value=True, stdout_value=subprocess.PIPE,
                   stderr_value=subprocess.PIPE):
    """Returns subprocess output of command passed in argument."""
    call = subprocess.Popen(cmd, shell=shell_value,
                            stdout=stdout_value, stderr=stderr_value)
    return call.communicate()


def get_rate(key, curr_data, prev_data):
    """Calculate and returns rate. Rate=(current_value-prev_value)/time."""
    rate = NAN
    if not prev_data:
        return rate

    if key not in prev_data:
        collectd.error("%s key not in previous data. Shouldn't happen." % key)
        return rate

    if TIMESTAMP not in curr_data or TIMESTAMP not in prev_data:
        collectd.error("%s key not in previous data. Shouldn't happen." % key)
        return rate

    curr_time = curr_data[TIMESTAMP]
    prev_time = prev_data[TIMESTAMP]

    if curr_time <= prev_time:
        collectd.error("Current data time: %s is less than previous data time: %s. "
                       "Shouldn't happen." % (curr_time, prev_time))
        return rate

    """
    if ((curr_time-prev_time) > (curr_data[INTERVAL]*TIME_DIFF_FACTOR)):
        collectd.error("Too much time diff between current data time: %s and previous data time: %s."
        %(curr_time, prev_time))
        return rate
    """

    rate = (curr_data[key] - prev_data[key]) / (curr_time - prev_time)
    return rate
