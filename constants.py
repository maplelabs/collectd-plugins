# Error Codes
SUCCESS = 1
FAILURE = -1
HOSTNAMEFAILURE = "Unknown"
AGG = "agg_"
# Common Plugin Constants
HOSTNAME = "hostName"
PLUGIN = "plugin"
PLUGIN_INS = "plugin_instance"
P_INS_ALL = "all"
VAL_TYPE = "vtype"
VAL_INS = "vtype_instance"
INTERVAL = "interval"
TIMESTAMP = "time"
AGGREGATE = "aggregate"
TOTAL = "total"
TAGS = "tags"
DUMMY = "dummy"
DUMMY_VAL = 10
DATADIR = "data"
NAN = "NaN"

# Interface Plugin Constants
IF_STATS = "nic_stats"
NICNAME = "name"
IPADDR = "ip"
MAC = "mac"
SPEED = "speed"
MTU = "mtu"
UP = "up"
TYPE = "type"
PHY = "physical"
VIRT = "virtual"
RX_PKTS = "rx_Pkts"
TX_PKTS = "tx_Pkts"
RX_DROPS = "rx_Drops"
TX_DROPS = "tx_Drops"
RX_BYTES = "rx_Bytes"
TX_BYTES = "tx_Bytes"
RX_RATE = "rx_Rate"
TX_RATE = "tx_Rate"

DEFAULT_INTERVAL = 10
TIME_DIFF_FACTOR = 3
FLOATING_FACTOR = 2

# CPU_Util Plugin Constants
CPU = "cpu_util"
CPU_UTILIZATION = "cpu_util"
CPU_UTIL = "CPUUtil"
CORE = "Cpu"  # for now let it be cpu
NUM_HIGH_ACTIVE = "numHighActive"
NUM_MEDIUM_ACTIVE = "numMedActive"
NUM_LOW_ACTIVE = "numLowActive"
LOW_RANGE_BEGIN = 0.0
LOW_RANGE_END = 10.0
MEDIUM_RANGE_END = 40.0
HIGH_RANGE_END = 100.0

# Ram_Util Plugin Constants
RAM = "ram_util"
RAM_UTILIZATION = "ram_util"
RAM_UTIL = "RAMUtil"
AVAILABLE = "available"

# TCP Plugin Constants
TCP = "tcp_stats"
TCP_STAT = "tcp_stat"
READ_TCPWIN_LOW = "readTcpWinLow"
READ_TCPWIN_MEDIUM = "readTcpWinMedium"
READ_TCPWIN_HIGH = "readTcpWinHigh"
WRITE_TCPWIN_LOW = "writeTcpWinLow"
WRITE_TCPWIN_MEDIUM = "writeTcpWinMedium"
WRITE_TCPWIN_HIGH = "writeTcpWinHigh"
TCPRESET = "tcpReset"
TCPRETRANS = "tcpRetrans"

# CPU_Static Plugin Constants
CPU_STATIC = "cpu_static"
CPU_TYPE = "CPUType"
SOCKET = "numSockets"
TOTAL_CORE = "numCores"
HT = "HT"
TOTAL_PHYSICAL_CPU = "numPhysicalCpu"
TOTAL_LOGICAL_CPU = "numLogicalCpu"
CLOCK = "clock"

# DiskStatic Plugin Constants
NAME = "name"
DISKSTAT = "disk_stat"
DISK = "disk"
TYPE = "type"
MOUNTPOINT = "mount"
CAPACITY = "capacity"
READIOPS = "readIOPS"
WRITEIOPS = "writeIOPS"
READTHROUGHPUT = "readThroughput"
WRITETHROUGHPUT = "writeThroughput"
READCOUNT = "readCount"
WRITECOUNT = "writeCount"
READBYTE = "readbyte"
WRITEBYTE = "writebyte"
USAGE = "usage"
SWAP = "SWAP"

INDEX_FILE = "index.txt"
INDEX_NEW_FILE = "-1"
ERRNO = -2
LOG_EXTENSION = ".txt"
MAX_LOG_ENTRIES = "MaxEntries"
PATH = "/opt/collectd/var/lib/"
WRITE_JSON_ENABLED = True
FACTOR = 1024
