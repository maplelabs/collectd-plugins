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
IF_STATS = "nicStats"
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
CPU = "cpuUtil"
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
RAM = "ramUtil"
RAM_UTILIZATION = "ram_util"
RAM_UTIL = "RAMUtil"
AVAILABLE = "available"

# TCP Plugin Constants
TCP = "tcpStats"
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
CPU_STATIC = "cpuStatic"
CPU_TYPE = "CPUType"
SOCKET = "numSockets"
TOTAL_CORE = "numCores"
HT = "HT"
TOTAL_PHYSICAL_CPU = "numPhysicalCpu"
TOTAL_LOGICAL_CPU = "numLogicalCpu"
CLOCK = "clock"

# DiskStatic Plugin Constants
NAME = "name"
DISKSTAT = "diskStat"
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

# MYSQL CONSTANTS

HOST = "host"
USER = "user"
PASSWORD = "password"
SERVER_DETAILS = "server_details"
TABLE_DETAILS = "table_details"
DB_DETAILS = "db_details"
MYSQL = "mysql"
server_query = 'SELECT count(*) FROM information_schema.SCHEMATA where schema_name not in ("information_schema", "mysql", "performance_schema")'
server_details_query = "select * from information_schema.global_status where VARIABLE_NAME like 'connections' or VARIABLE_NAME like 'aborted_connects'\
                        or VARIABLE_NAME like 'threads_connected' or VARIABLE_NAME like 'threads_cached' or VARIABLE_NAME like 'threads_created'\
                        or VARIABLE_NAME like 'threads_running' or VARIABLE_NAME like 'uptime' or VARIABLE_NAME like 'bytes_received'\
			or VARIABLE_NAME like 'bytes_sent'"
table_query = "select table_name as 'tableName' ,table_schema as 'databaseName', ENGINE as 'engine', TABLE_Rows as 'tableRows', DATA_LENGTH as 'dataLen',\
               INDEX_LENGTH as 'indexSize', DATA_FREE as 'dataFree' from information_schema.tables"
db_info_query = "select schema_name from information_schema.schemata WHERE schema_name not in ('mysql', 'information_schema', 'performance_schema')"
db_query_1 = "select Round(Sum(data_length + index_length) / 1024 / 1024, 1) 'dbSize' FROM information_schema.tables where table_schema='%s'"
db_query_2 = "SELECT COUNT(*) as numTables FROM information_schema.tables WHERE table_schema ='%s'"
db_query_3 = "select index_length FROM information_schema.tables where table_schema='%s'"
db_query_4 = "use %s"
db_query_5 = 'show session status where VARIABLE_NAME like "Created_tmp_files" or VARIABLE_NAME like "Created_tmp_tables"\
              or VARIABLE_NAME like "Queries" or VARIABLE_NAME like "Com_select" or VARIABLE_NAME like "Com_insert"\
	      or VARIABLE_NAME like "Com_update" or VARIABLE_NAME like "Com_delete" or VARIABLE_NAME like "Slow_queries"'

# JVM CONSTANTS
PROCESS = "process"
PROCESS_STATE = "processState"
NAME = "name"
JVM_STATS = "jvmStats"

# APACHE CONSTANTS
DEFAULT_LOCATION = "server-status"
LOCATION = "location"
PORT = "port"
SECURE = "secure"
APACHE_PERF = "apachePerf"
DEFAULT_LOG_FILE = "/var/log/apache2/access.log"
DEFAULT_LOG_FORMAT = '%a %A %B %T %h %H %p %>s %t \"%r\" \"%U\"'
ACCESS_LOG = "accesslog"
APACHE_TRANS = "apacheTrans"
