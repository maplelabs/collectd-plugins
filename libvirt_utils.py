import libvirt
import collectd
from libvirt_constants import *


def get_vm_state(state):
    """
    :param state:
    :return: String, VM status
    """
    if state == libvirt.VIR_DOMAIN_NOSTATE:
        return "noState"
    elif state == libvirt.VIR_DOMAIN_RUNNING:
        return "Running"
    elif state == libvirt.VIR_DOMAIN_BLOCKED:
        return "Blocked"
    elif state == libvirt.VIR_DOMAIN_PAUSED:
        return "Paused"
    elif state == libvirt.VIR_DOMAIN_SHUTDOWN:
        return "Shutdown"
    elif state == libvirt.VIR_DOMAIN_SHUTOFF:
        return "Shutoff"
    elif state == libvirt.VIR_DOMAIN_CRASHED:
        return "Crashed"
    elif state == libvirt.VIR_DOMAIN_PMSUSPENDED:
        return "Suspended"
    else:
        return "Unknown"


def get_cpu_percentage(prev_data, cur_data):

    cpu_util = NAN

    if not prev_data:
        return cpu_util

    # IF any
    if CPU_TIME not in prev_data or CPU_TIME not in cur_data:
        collectd.error("%s not present in previous data" % (CPU_TIME))
        return cpu_util

    # If VM restarted then, the cpu time changes
    if prev_data[CPU_TIME] > cur_data[CPU_TIME]:
        collectd.error(
            "Current CPU time is smaller than the previous CPU time.")
        return cpu_util

    if TIMESTAMP not in prev_data or TIMESTAMP not in cur_data:
        collectd.error("Timestamp is not present in the collected stats")
        return cpu_util

    if cur_data[CPU_TIME] is not NAN and prev_data[CPU_TIME] is not NAN:
        cpu_time_diff = cur_data[CPU_TIME] - prev_data[CPU_TIME]

        nr_cores = int(cur_data[NO_OF_VCPU])

        time_diff = cur_data[TIMESTAMP] - prev_data[TIMESTAMP]

        cpu_util = 100.0 * cpu_time_diff / (time_diff * nr_cores * 1000000000)

    return cpu_util


def establish_connection(url):
    conn = libvirt.open(url)
    if not conn:
        collectd.error('Failed to open connection %s' % url)
        return None, FAILURE
    collectd.info(
        "Plugin successfully created connection with hypervisor")

    libvirt_version = conn.getLibVersion()
    if libvirt_version >= LIBVIRT_VERSION:
        lib_ver = ".".join([i for i in LIBVIRT_VERSION[:3]])
        collectd.warning("Libvirtd version should be greater than or equal to " + lib_ver + ", things may not work as expected")
    return conn, None

