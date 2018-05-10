# Error Codes
SUCCESS = 1
FAILURE = -1
HOSTNAMEFAILURE = "Unknown"
AGG = ""
# Common Plugin Constants
HOSTNAME = "_hostName"
PLUGIN = "_plugin"
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
NODETYPE = "nodeType"
LINUX = "linux"
LINUX_STATIC = "linux_static"
LINUX_DYNAMIC = "linux_dynamic"
PLUGINTYPE = "_documentType"
ACTUALPLUGINTYPE = "_actual_collectd_plugin_type"

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
NIC_NAME = "_nicName"
NIC_TYPE = "_nicType"

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
CPU_TYPE = "_CPUType"
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
MOUNTPOINT = "mount"
CAPACITY = "capacity"
READIOPS = "readIOPS"
WRITEIOPS = "writeIOPS"
AVGQUEUESIZE = "avgQuSize"
READTHROUGHPUT = "readThroughput"
WRITETHROUGHPUT = "writeThroughput"
READCOUNT = "readCount"
WRITECOUNT = "writeCount"
READTIME = "readTime"
WRITETIME = "writeTime"
READLATENCY = "readLatency"
WRITELATENCY = "writeLatency"
READBYTE = "readbyte"
WRITEBYTE = "writebyte"
USAGE = "usage"
SWAP = "SWAP"
DISK_TYPE = "_diskType"
DISK_NAME = "_diskName"

INDEX_FILE = "index.txt"
INDEX_NEW_FILE = "-1"
ERRNO = -2
LOG_EXTENSION = ".txt"
MAX_LOG_ENTRIES = "MaxEntries"
PATH = "/opt/collectd/var/lib/"
WRITE_JSON_ENABLED = True
FACTOR = 1024
BITFACTOR = 8

# MYSQL CONSTANTS

HOST = "host"
USER = "user"
PASSWORD = "password"
SERVER_DETAILS = "server_details"
TABLE_DETAILS = "tableDetails"
DB_DETAILS = "db_details"
MYSQL = "mysql"
server_query = 'SELECT count(*) FROM information_schema.SCHEMATA where schema_name not in ("information_schema", "mysql", "performance_schema")'
server_details_query = "select * from performance_schema.global_status where VARIABLE_NAME like 'connections' or VARIABLE_NAME like 'aborted_connects'\
                        or VARIABLE_NAME like 'threads_connected' or VARIABLE_NAME like 'threads_cached' or VARIABLE_NAME like 'threads_created'\
                        or VARIABLE_NAME like 'threads_running' or VARIABLE_NAME like 'uptime' or VARIABLE_NAME like 'bytes_received'\
			or VARIABLE_NAME like 'bytes_sent'"
table_query = "select table_name as '_tableName' ,table_schema as '_dbName', ENGINE as '_engine', TABLE_Rows as 'tableRows', DATA_LENGTH as 'dataLen',\
               INDEX_LENGTH as 'indexSize', DATA_FREE as 'dataFree' from information_schema.tables where table_schema='%s'"
db_info_query = "select schema_name from information_schema.schemata WHERE schema_name not in ('mysql', 'information_schema', 'performance_schema')"
db_query_1 = "select Round(Sum(data_length + index_length) / 1024 / 1024, 1) 'dbSize' FROM information_schema.tables where table_schema='%s'"
db_query_2 = "SELECT COUNT(*) as numTables FROM information_schema.tables WHERE table_schema ='%s'"
db_query_3 = "select index_length FROM information_schema.tables where table_schema='%s'"
db_query_4 = "use %s"
db_query_5 = 'show global status where VARIABLE_NAME like "Created_tmp_files" or VARIABLE_NAME like "Created_tmp_tables"\
              or VARIABLE_NAME like "Queries" or VARIABLE_NAME like "Com_select" or VARIABLE_NAME like "Com_insert"\
	      or VARIABLE_NAME like "Com_update" or VARIABLE_NAME like "Com_delete" or VARIABLE_NAME like "Slow_queries"\
          or VARIABLE_NAME like "Qcache_hits" or VARIABLE_NAME like "Qcache_inserts"'

# JVM CONSTANTS
PROCESS = "process"
PROCESS_STATE = "_processState"
JVM_STATS = "jvm"

# APACHE CONSTANTS
DEFAULT_LOCATION = "server-status"
LOCATION = "location"
PORT = "port"
SECURE = "secure"
APACHE = "apache"
DEFAULT_LOG_FILE = "/var/log/apache2/access.log"
DEFAULT_LOG_FORMAT = '%a %A %B %T %h %H %p %>s %t \"%r\" \"%U\"'
ACCESS_LOG = "accesslog"
APACHE_TRANS = "apacheTrans"

#POSTGRES CONSTANTS
class Postgres(object):

    POSTGRES = "postgres"
    db_info_query = 'SELECT datname FROM pg_database WHERE datistemplate = false;'

    db_trans_query = "select xact_commit+xact_rollback as \"numTransactions\", blks_read as \"blocksRead\", blks_hit as \"blocksHit\", " \
                 "tup_returned as \"numReturn\", tup_inserted as \"numInsert\", tup_deleted as \"numDelete\", " \
                 "tup_updated as \"numUpdate\", tup_fetched as \"numFetch\", temp_files as \"numTempFile\", " \
                 "temp_bytes as \"tempFileSize\", blk_read_time as \"blkReadTime\", " \
                 "blk_write_time as \"blkWriteTime\" from pg_stat_database where datname='%s';"

    db_size_query = "select sum(pg_database_size(datname)) as db_size from pg_database where datname = '%s'"

    db_num_tables_query = "select count(*) from pg_stat_user_tables;"

    server_log_size_query = "select CAST(sum((pg_stat_file('pg_xlog/' || pg_ls_dir)).size) AS BIGINT) " \
                    "from pg_ls_dir('pg_xlog') " \
                    "where (pg_stat_file('pg_xlog/' || pg_ls_dir)).isdir = false"

    server_connections = "SELECT sum(numbackends) FROM pg_stat_database;"

    table_query = "select schemaname as \"_schemaName\", relname as \"_tableName\", " \
              "pg_table_size(relid)as \"tableSize\", " \
              "pg_indexes_size(relid) as \"indexSize\", " \
              "heap_blks_read as \"heapBlksRead\", heap_blks_hit as \"heapBlksHit\", " \
              "idx_blks_read as \"idxBlksRead\", idx_blks_hit as \"idxBlksHit\" FROM pg_catalog.pg_statio_user_tables ORDER BY pg_table_size(relid) DESC;"

    long_runn_query = "SELECT now() - query_start as \"runtime\", usename as \"userName\", datname as \"_dbName\", waiting, " \
                  "state, query as \"_queryName\" FROM pg_stat_activity WHERE now() - query_start > '250 milliseconds'::interval and state='active'" \
                  "ORDER BY runtime DESC LIMIT 20;"

    conn_str = "host=%s user=%s password=%s port=%s dbname=%s"

    cache_ratio = "SELECT sum(heap_blks_read) as heap_read, sum(heap_blks_hit)  as heap_hit, case sum(heap_blks_read) + sum(heap_blks_hit) when 0 then 0 " \
              "else (sum(heap_blks_hit)::float / (sum(heap_blks_hit) + sum(heap_blks_read)))*100 end as ratio FROM pg_statio_user_tables;"

    index_cache_ratio = "SELECT sum(idx_blks_read) as idx_read, sum(idx_blks_hit)  as idx_hit, case sum(idx_blks_read) + sum(idx_blks_hit) when 0 then 0 " \
                    "else (sum(idx_blks_hit)::float / (sum(idx_blks_hit) + sum(idx_blks_read)))*100 end as ratio FROM pg_statio_user_indexes;"

    shared_buffer = "SELECT name, setting, min_val, max_val, context FROM pg_settings WHERE name='shared_buffers';"

    effective_cache = "SELECT name, setting, min_val, max_val, context FROM pg_settings WHERE name = 'effective_cache_size';"

    stat_table_query = "SELECT relname as \"_tableName\", seq_scan as \"seqScan\", seq_tup_read as \"seqScanFetch\", idx_scan as \"indexScan\"," \
                    "idx_tup_fetch as \"indexScanFetch\", n_tup_ins as \"numInsert\", n_tup_del as \"numDelete\", n_tup_upd as \"numuUdate\"," \
                    "n_live_tup as \"numLiveTuple\", n_dead_tup as \"numDeadTuple\" FROM pg_stat_user_tables;"

    index_query = "SELECT schemaname as \"_schemaName\", relname as \"_tableName\", indexrelname as \"_indexName\", idx_scan as \"indexScan\"," \
              "idx_tup_read as \"numReturn\", idx_tup_fetch as \"numFetch\" FROM pg_stat_user_indexes;"

    stat_index_query = "SELECT schemaname as \"_schemaName\", relname as \"_tableName\", indexrelname as \"_indexName\", idx_blks_read as \"blksRead\"," \
                    "idx_blks_hit as \"blksHit\" FROM pg_statio_user_indexes;"

    check_superuser = "SELECT usename as \"userName\" from pg_user where usesuper = True;"

    max_connection = "SELECT setting::integer FROM pg_settings WHERE name = \'max_connections\';"

    up_time = "SELECT pg_postmaster_start_time() as upTime;"
    epoch_time = "SELECT EXTRACT(EPOCH FROM TIMESTAMP WITH TIME ZONE '%s');"

    active_process = "SELECT count(*) FROM pg_stat_activity WHERE state ='active';"

    idle_process = "SELECT count(*) FROM pg_stat_activity WHERE state ='idle';"

    server_version = "SHOW server_version;"

# TPCC Plugin constants
TPCC = "tpcc"

# Kafka_jmx plugin constants
KAFKA_JMX = "kafka_jmx"
PROCESSNAME = "_processName"
