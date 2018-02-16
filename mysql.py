""" A collectd-python plugin for retrieving
    metrics from MYSQL Database server.
    Plugin is valid for mysql version 5.7.6 onwards
    """

import collectd
import signal
import time
import json
import MySQLdb

# user imports
from constants import *
from utils import *
from libdiskstat import *
from copy import deepcopy


class MysqlStats:
    def __init__(self):
        self.interval = DEFAULT_INTERVAL
        self.host = None
        self.user = None
        self.password = None
        self.cur = None
        self.pollCounter = 0
        self.previousData = {"numCreatedTempFiles": 0, "numCreatedTempTables": 0, "numQueries": 0,
                             "numSelect": 0, "numInsert": 0, "numUpdate": 0, "numDelete": 0,
                             "slowQueries": 0 , "bytesReceivedMB" : 0, "bytesSentMB" : 0,
                             "qcacheHits": 0, "qcacheInserts" : 0
                             }

    def read_config(self, cfg):
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == HOST:
                self.host = children.values[0]
            if children.key == USER:
                self.user = children.values[0]
            if children.key == PASSWORD:
                self.password = children.values[0]

    def connect_mysql(self):
        try:
            db = MySQLdb.connect(host=self.host, user=self.user, passwd=self.password, db='information_schema')
            self.cur = db.cursor()
        except Exception as e:
            collectd.error("Couldn't connect to the MySQL server: %s" % e)
            return

    def get_sql_server_data(self):
        final_server_dict = {}
        server_dict = {}
        try:
            self.cur.execute(server_query)
            num_databases = int(self.cur.fetchall()[0][0])
            self.cur.execute(server_details_query)
            server_details = dict(self.cur.fetchall())
            if server_details:
                server_dict[TYPE] = SERVER_DETAILS
                server_dict['numDatabases'] = num_databases
                # server_dict['numConnections'] = long(server_details['Connections'])
                # server_dict['numAbortedConnects'] = long(server_details['Aborted_connects'])
                server_dict['threadsConnected'] = long(server_details['Threads_connected'])
                server_dict['threadsCached'] = long(server_details['Threads_cached'])
                # server_dict['threadsCreated'] = long(server_details['Threads_created'])
                server_dict['threadsRunning'] = long(server_details['Threads_running'])
                server_dict['upTime'] = round(float(server_details['Uptime'])/(60*60),2)
                if(self.pollCounter <= 1):
                    self.previousData["bytesReceivedMB"] = long(server_details['Bytes_received'])/(1024*1024)
                    self.previousData["bytesSentMB"] = long(server_details['Bytes_sent']) / (1024 * 1024)
                    self.previousData["numConnections"] = long(server_details['Connections'])
                    self.previousData["numAbortedConnects"] = long(server_details['Aborted_connects'])
                    self.previousData["threadsCreated"] = long(server_details['Threads_created'])
                else:
                    server_dict['bytesReceivedMB'] = long(server_details['Bytes_received'])/(1024*1024) - self.previousData["bytesReceivedMB"]
                    server_dict['bytesSentMB'] = long(server_details['Bytes_sent'])/(1024*1024) - self.previousData["bytesSentMB"]
                    server_dict['numConnections'] = long(server_details['Connections']) - self.previousData["numConnections"]
                    server_dict['numAbortedConnects'] = long(server_details['Aborted_connects']) - self.previousData["numAbortedConnects"]
                    server_dict['threadsCreated'] = long(server_details['Threads_created']) - self.previousData["threadsCreated"]
                    self.previousData["bytesReceivedMB"] = long(server_details['Bytes_received']) / (1024 * 1024)
                    self.previousData["bytesSentMB"] = long(server_details['Bytes_sent']) / (1024 * 1024)
                    self.previousData["numConnections"] = long(server_details['Connections'])
                    self.previousData["numAbortedConnects"] = long(server_details['Aborted_connects'])
                    self.previousData["threadsCreated"] = long(server_details['Threads_created'])
                server_dict[PLUGINTYPE] = "serverDetails"
            else:
                return
            self.cur.execute(db_query_5)
            server_details1 = dict(self.cur.fetchall())
            if server_details1:
                if(self.pollCounter <= 1):
                    self.previousData["numCreatedTempFiles"] = int(server_details1['Created_tmp_files'])
                    self.previousData["numCreatedTempTables"] = int(server_details1['Created_tmp_tables'])
                    self.previousData["numQueries"] = int(server_details1['Queries'])
                    self.previousData["numSelect"] = int(server_details1['Com_select'])
                    self.previousData["numInsert"] = int(server_details1['Com_insert'])
                    self.previousData["numUpdate"] = int(server_details1['Com_update'])
                    self.previousData["numDelete"] = int(server_details1['Com_delete'])
                    self.previousData["slowQueries"] = int(server_details1['Slow_queries'])
                    self.previousData["qcacheHits"] = int(server_details1['Qcache_hits'])
                    self.previousData["qcacheInserts"] = int(server_details1['Qcache_inserts'])
                else:
                    server_dict['numCreatedTempFiles'] = int(server_details1['Created_tmp_files']) - self.previousData["numCreatedTempFiles"]
                    server_dict['numCreatedTempTables'] = int(server_details1['Created_tmp_tables']) - self.previousData["numCreatedTempTables"]
                    server_dict['numQueries'] =  int(server_details1['Queries']) - self.previousData["numQueries"]
                    server_dict['numSelect'] =  int(server_details1['Com_select']) - self.previousData["numSelect"]
                    server_dict['numInsert'] =  int(server_details1['Com_insert']) - self.previousData["numInsert"]
                    server_dict['numUpdate'] =  int(server_details1['Com_update']) - self.previousData["numUpdate"]
                    server_dict['numDelete'] =  int(server_details1['Com_delete']) - self.previousData["numDelete"]
                    server_dict['slowQueries'] = int(server_details1['Slow_queries']) - self.previousData["slowQueries"]
                    server_dict['qcacheHits'] = int(server_details1['Qcache_hits']) - self.previousData["qcacheHits"]
                    server_dict['qcacheInserts'] = int(server_details1['Qcache_inserts']) - self.previousData["qcacheInserts"]
                    self.previousData["numCreatedTempFiles"] = int(server_details1['Created_tmp_files'])
                    self.previousData["numCreatedTempTables"] = int(server_details1['Created_tmp_tables'])
                    self.previousData["numQueries"] = int(server_details1['Queries'])
                    self.previousData["numSelect"] = int(server_details1['Com_select'])
                    self.previousData["numInsert"] = int(server_details1['Com_insert'])
                    self.previousData["numUpdate"] = int(server_details1['Com_update'])
                    self.previousData["numDelete"] = int(server_details1['Com_delete'])
                    self.previousData["slowQueries"] = int(server_details1['Slow_queries'])
                    self.previousData["qcacheHits"] = int(server_details1['Qcache_hits'])
                    self.previousData["qcacheInserts"] = int(server_details1['Qcache_inserts'])
            final_server_dict[SERVER_DETAILS] = server_dict
        except Exception as e:
            collectd.error("Unable to execute the provided query:%s" % e)
            return
        return final_server_dict

    def get_table_data(self, final_table_dict, db_name):
        try:
            final_table_query = table_query %db_name
            self.cur.execute(final_table_query)
            fields = map(lambda x: x[0], self.cur.description)
            table_details_list = [dict(zip(fields, row)) for row in self.cur.fetchall()]
            agg_db_data = {"dataFree" : 0, "dataLen" : 0, "indexSize" : 0}
            for item in table_details_list:
                table_dict = {}
                table_dict["_engine"] = str(0) if item["_engine"] is None else str(item["_engine"])
                table_dict["_dbName"] = str(0) if item["_dbName"] is None else str(item["_dbName"])
                table_dict["dataFree"] = float(0) if item["dataFree"] is None else round(float(item["dataFree"]) / (1024 * 1024), 2)
                agg_db_data["dataFree"] = agg_db_data["dataFree"] + table_dict["dataFree"]
                table_dict["dataLen"] = float(0) if item["dataLen"] is None else round(float(item["dataLen"]) / (1024 * 1024), 2)
                agg_db_data["dataLen"] = agg_db_data["dataLen"] + table_dict["dataLen"]
                table_dict["_tableName"] = str(None) if item["_tableName"] is None else str(item["_tableName"])
                table_dict["tableRows"] = long(0) if item["tableRows"] is None else long(item["tableRows"])
                table_dict["indexSize"] = float(0) if item["indexSize"] is None else round(float(item["indexSize"]) / (1024 * 1024), 2)
                agg_db_data["indexSize"] = agg_db_data["indexSize"] + table_dict["indexSize"]
                table_dict[TYPE] = TABLE_DETAILS
                table_dict[PLUGINTYPE] = TABLE_DETAILS
                final_table_dict[table_dict["_tableName"]] = table_dict
            final_table_dict[db_name]["dataFree"] = agg_db_data["dataFree"]
            final_table_dict[db_name]["dataLen"] = agg_db_data["dataLen"]
            final_table_dict[db_name]["indexSize"] = agg_db_data["indexSize"]
        except Exception as e:
            collectd.error("Unable to execute the query:%s" % e)
            return
        return final_table_dict

    def get_db_info(self):
        database_names = []
        try:
            self.cur.execute(db_info_query)
            for database_name in self.cur.fetchall():
                database_names.append(database_name[0])
        except Exception as e:
            collectd.info("Couldn't execute the Query:%s" % e)
            return
        return database_names

    def get_db_data(self, final_db_dict):
        db_list = self.get_db_info()
        agg_server_data = {"dbSize" : 0, "indexSize" : 0}
        if db_list:
            for db_name in db_list:
                db_dict = {}
                try:
                    db_query_1_org = db_query_1 % db_name
                    self.cur.execute(db_query_1_org)
                    db_size = self.cur.fetchall()[0][0]
                    db_query_2_org = db_query_2 % db_name
                    self.cur.execute(db_query_2_org)
                    num_tables = self.cur.fetchall()[0][0]
                    db_query_3_org = db_query_3 % db_name
                    self.cur.execute(db_query_3_org)
                    index_size = []
                    for ind_size in self.cur.fetchall():
                        if(ind_size[0] is None):
                            continue
                        else:
                            index_size.append(ind_size[0])
                    total_index_size = sum(index_size)
                    total_index_size = float(total_index_size) / (1024 * 1024)
                    db_query_4_org = db_query_4 % db_name
                    self.cur.execute(db_query_4_org)
                    self.cur.execute(db_query_5)
                    db_details = dict(self.cur.fetchall())
                    if db_details:
                        db_dict[TYPE] = DB_DETAILS
                        db_dict['_dbName'] = db_name
                        if db_size is None:
                            db_dict['dbSize'] = float(0)
                        else:
                            db_dict['dbSize'] = round(float(db_size), 2)
                        agg_server_data["dbSize"] = agg_server_data["dbSize"] + db_dict["dbSize"]
                        if num_tables is None:
                            db_dict['numTables'] = int(0)
                        else:
                            db_dict['numTables'] = int(num_tables)
                        db_dict['indexSize'] = total_index_size
                        agg_server_data["indexSize"] = agg_server_data["indexSize"] + db_dict["indexSize"]
                        db_dict[PLUGINTYPE] = "databaseDetails"
                    else:
                        collectd.info("Couldn't get the database details")
                        return
                except Exception as e:
                    print e
                    return
                final_db_dict[db_name] = db_dict
                final_db_dict[SERVER_DETAILS]["dbSize"] = agg_server_data["dbSize"]
                final_db_dict[SERVER_DETAILS]["indexSize"] = agg_server_data["indexSize"]
                final_db_dict = self.get_table_data(final_db_dict, db_name)
        else:
            collectd.info("Couldn't get the database list")
            return
        return final_db_dict

    @staticmethod
    def add_common_params(mysql_dict):
        hostname = gethostname()
        timestamp = int(round(time.time()))

        for details_type, details in mysql_dict.items():
            details[HOSTNAME] = hostname
            details[TIMESTAMP] = timestamp
            details[PLUGIN] = MYSQL
            details[ACTUALPLUGINTYPE] = MYSQL
            details[PLUGIN_INS] = details_type
            #details[PLUGINTYPE] = MYSQL

    def collect_data(self):
        # get data of MySQL
        server_details = self.get_sql_server_data()
        final_details = self.get_db_data(server_details)
        # final_details = self.get_table_data(db_details)
        if not final_details:
            collectd.error("Plugin MYSQL: Unable to fetch data information of MYSQL.")
            return

        # Add common parameters
        self.add_common_params(final_details)
        return final_details

    @staticmethod
    def dispatch_data(dict_disks_copy):
        for details_type, details in dict_disks_copy.items():
            collectd.debug("Plugin MySQL: Values: " + json.dumps(details))
            collectd.info("final details are : %s" % details)
            dispatch(details)

    def read(self):
        try:
            self.pollCounter += 1
            self.connect_mysql()
            # collect data
            dict_mysql = self.collect_data()
#            collectd.info(dict_mysql)
            if not dict_mysql:
                collectd.error("Plugin MySQL: Unable to fetch data for MySQL.")
                return

            # dispatch data to collectd, copying by value
            self.dispatch_data(deepcopy(dict_mysql))
        except Exception as e:
            collectd.error("Couldn't read and gather the SQL metrics due to the exception :%s" % e)
            return

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


def init():
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)


obj = MysqlStats()
collectd.register_config(obj.read_config)
collectd.register_read(obj.read_temp)

