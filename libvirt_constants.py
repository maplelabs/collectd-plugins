from constants import * 


# Libvirt iface plugin
IFACE_PLUGIN = "libvirtInterface"
IFACE_NAME = "ifaceName"
TX_PKTS = "txPkts"
TX_BYTES = "txBytes"
TX_RATE = "txRate(Mbps)"
TX_DROPS = "txDrops"
TX_ERR = "txErr"
RX_PKTS = "rxPkts"
RX_BYTES = "rxBytes"
RX_RATE = "rxRate(Mbps)"
RX_DROPS = "rxDrops"
RX_ERR = "rxErr"
VLAN_TAG = "vlanTag"
VLAN_TRUNKS = "vlanTrunks"
VM_NIC_NAME = "vm_nic_name"
AGG_TX_RATE = "agg_"+TX_RATE
AGG_RX_RATE = "agg_"+RX_RATE


# Libvirt common constants
HOSTNME = "hostName"
VMNAME = "vmName"
VMID = "vmId"
PLUGIN_NAME = "pluginName"
LIBVIRT_VERSION = "2000000"
LIBVIRT_URL = "qemu:///system"
UTC = "utc"
TAGS = "tags"
VM_STATE = "vmState"


# Libvirt static plugin constants
VM_SOURCE = "vmSource"
OSNAME = "osName"
OSTYPE = "osType"
CPUTYPE = "cpuType"
NUM_CORES = "numCores"
STATIC_PLUGIN = "libvirtStatic"
NO_OF_VCPU = "noOfCpus"
ALLOCATED_MEMORY = "allocatedMemory(MB)"


# Libvirt Compute plugin constants
COMP_PLUGIN = "libvirtCompute"
CPU_UTIL = "cpuUtil(%)"
CPU_TIME = "totalCpuTime(ns)"
USED_MEMORY = "usedMemory(MB)"
RAM_UTIL = "ramUtil(%)"


# Libvirt disk plugin constants
DISK_CAPACITY = "diskSize(GB)"
DISK_USED = "diskUsed(GB)"
DISK_TYPE = "diskType"
READ_THROUGHPUT = "readThroughput(Mbps)"
READ_IOPS = "readIops"
WRITE_THROUGHPIUT = "writeThroughput(Mbps)"
WRITE_IOPS = "writeIops"
DISK_NAME = "diskName"
DISK_PATH = "diskPath"
DISK_READ_BYTES = "diskReadBytes"
DISK_WRITE_BYTES = "diskWriteBytes"
DISK_READ_REQ = "diskReadReq"
DISK_WRITE_REQ = "diskWriteReq"
DISK_ERR = "diskError"
DISK_PLUGIN = "libvirtDisk"

AGG_READ_IOPS = "aggReadIops"
AGG_WRITE_IOPS = "aggWriteIops"
AGG_READ_THROUGHPUT = "aggReadThroughput(Mbps)"
AGG_WRITE_THROUGHPUT = "aggWriteThroughput(Mbps)"
