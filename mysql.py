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
                server_dict['numConnections'] = server_details['Connections']
                server_dict['numAbortedConnects'] = server_details['Aborted_connects']
                server_dict['threadsConnected'] = server_details['Threads_connected']
                server_dict['threadsCached'] = server_details['Threads_cached']
                server_dict['threadsCreated'] = server_details['Threads_created']
                server_dict['threadsRunning'] = server_details['Threads_running']
                server_dict['upTime'] = server_details['Uptime']
                server_dict['bytesReceived'] = server_details['Bytes_received']
                server_dict['bytesSent'] = server_details['Bytes_sent']
            else:
                return
            final_server_dict[SERVER_DETAILS] = server_dict
        except Exception as e:
            collectd.error("Unable to execute the provided query:%s" % e)
            return
        return final_server_dict

    def get_table_data(self, final_table_dict):
        try:
            self.cur.execute(table_query)
            fields = map(lambda x: x[0], self.cur.description)
            table_details_list = [dict(zip(fields, row)) for row in self.cur.fetchall()]
            for table_dict in table_details_list:
                for key in table_dict:
                    if type(table_dict[key]) == long:
                        table_dict[key] = float(table_dict[key])
                    if type(table_dict[key]) is None:
                        table_dict[key] = int(0)
                table_dict[TYPE] = TABLE_DETAILS
                final_table_dict[table_dict["tableName"]] = table_dict
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
                    db_query_4_org = db_query_4 % db_name
                    self.cur.execute(db_query_4_org)
                    self.cur.execute(db_query_5)
                    db_details = dict(self.cur.fetchall())
                    if db_details:
                        db_dict[TYPE] = DB_DETAILS
                        db_dict['dbName'] = db_name
                        if db_size is None:
                            db_dict['dbSize'] = int(0)
                        else:
                            db_dict['dbSize'] = int(db_size)
                        if num_tables is None:
                            db_dict['numTables'] = int(0)
                        else:
                            db_dict['numTables'] = int(num_tables)
                        db_dict['indexSize'] = int(total_index_size)
                        db_dict['numCreatedTempFiles'] = int(db_details['Created_tmp_files'])
                        db_dict['numCreatedTempTables'] = int(db_details['Created_tmp_tables'])
                        db_dict['numQueries'] = int(db_details['Queries'])
                        db_dict['numSelect'] = int(db_details['Com_select'])
                        db_dict['numInsert'] = int(db_details['Com_insert'])
                        db_dict['numUpdate'] = int(db_details['Com_update'])
                        db_dict['numDelete'] = int(db_details['Com_delete'])
                        db_dict['slowQueries'] = int(db_details['Slow_queries'])
                    else:
                        collectd.info("Couldn't get the database details")
                        return
                except Exception as e:
                    print e
                    return
                final_db_dict[db_name] = db_dict
        else:
            collectd.info("Couldn't get the database list")
            return
        return final_db_dict

    @staticmethod
    def add_common_params(mysql_dict):
        hostname = gethostname()
        timestamp = time.time()

        for details_type, details in mysql_dict.items():
            details[HOSTNAME] = hostname
            details[TIMESTAMP] = timestamp
            details[PLUGIN] = MYSQL
            details[PLUGIN_INS] = details_type
            details[PLUGINTYPE] = MYSQL

    def collect_data(self):
        # get data of MySQL
        server_details = self.get_sql_server_data()
        db_details = self.get_db_data(server_details)
        final_details = self.get_table_data(db_details)
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

