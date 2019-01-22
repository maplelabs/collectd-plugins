""" A collectd-python plugin for retrieving
    metrics from POSTGRESQL Database server.
    """
import collectd
import signal
import time
import json
import psycopg2
import calendar
import traceback

# user imports
from constants import *
from utils import *
from libdiskstat import *
from copy import deepcopy

class PostgresStats:
    def __init__(self):
        self.interval = DEFAULT_INTERVAL
        self.host = None
        self.user = None
        self.password = None
        self.cur = None
        self.port = None
        self.pollCounter = 0
        self.pollDiff = {}
        self.cacheHitRatio = 0
        self.numdatabase = 0
        self.heapBlksRead = 0
        self.heapBlksHit = 0
        self.idxBlksHit = 0
        self.idxBlksRead = 0
        self.documentsTypes = []
        self.aggr_server_data = {'dbSize': 0, 'numDelete': 0, 'numUpdate': 0, 'numInsert': 0, 'numFetch': 0,
                                 'numTransactions': 0, 'cacheHits': 0, 'numCreatedTempFiles': 0, 'tempFileSize': 0,
                                 'indexSize': 0, 'cacheHitRatio': 0}
        self.aggr_db_data = {'numLiveTuple': 0, 'numDeadTuple': 0, 'indexSize': 0, 'cacheHitRatio': 0,
                             'indexHitRatio': 0}

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
            if children.key == PORT:
                self.port = children.values[0]
            if children.key == DOCUMENTSTYPES:
                self.documentsTypes = children.values[0]

    # Trying to connect to the postgres server
    def connect_postgres(self):
        try:
            conn_str_org = Postgres.conn_str % (self.host, self.user, self.password, self.port, 'postgres')
            retry_flag = True
            retry_count = 0
            while retry_flag and retry_count < 3:
                try:
                    self.conn = psycopg2.connect(conn_str_org)
                    self.cur = self.conn.cursor()
                    retry_flag = False
                    collectd.info("Connection to Postgres is successfull in attempt %s" % retry_count)
                except Exception as e:
                    collectd.debug("Retry after 5 sec as connection to Postgres failed in attempt %s" % retry_count)
                    retry_count += 1
                    time.sleep(5)
        except Exception as e:
            collectd.error("Exception in the connect_postgres due to %s" % e)
            return

    def connect_db(self, dbname):
        try:
            conn_str_db = Postgres.conn_str % (self.host, self.user, self.password, self.port, dbname)
            conn = psycopg2.connect(conn_str_db)
            collectd.info("Connection to %s database is successfull"% dbname)
            cursor = conn.cursor()
            conn_flag = True
        except Exception as e:
            collectd.error("Exception in the connect_db due to %s" % e)
            conn_flag = False
            cursor = None
        return cursor, conn_flag

    # Get the list of databases from the server
    def get_inventory(self):
        database_names = []
        try:
            self.cur.execute(Postgres.db_info_query)
            db_list = self.cur.fetchall()
            if db_list:
                for database_name in db_list:
                    database_names.append(database_name[0])
            else:
                collectd.info("No databases present")
        except Exception as e:
            collectd.error("Exception from the get_inventory due to %s in %s" %(e, traceback.format_exc()))
        return database_names

    # To get the postgres server details
    def get_server_details(self):
        final_server_dict = {}
        server_dict = {}
        try:
            # Getting Inventory details
            num_databases = self.get_inventory()
            server_dict["numDatabases"] = len(num_databases)
            self.numdatabase = server_dict["numDatabases"]

            # Getting postgreSQL server version
            self.cur.execute(Postgres.server_version)
            ser_version = self.cur.fetchall()
            if ser_version:
                server_dict["version"] = ser_version[0][0]
            else:
                collectd.debug("Unable to fetch server version")

            # Getting log size of the server
            self.cur.execute(Postgres.server_log_size_query)
            log_size = self.cur.fetchall()
            if log_size:
                server_dict["logSize"] = log_size[0][0]/(1024 * 1024)
            else:
                collectd.debug("Unable to fetch log size")

            # UpTime details of the server
            self.cur.execute(Postgres.up_time)
            uptime = self.cur.fetchall()
            if uptime:
                uptime_start = uptime[0][0]
                up_epoch_time = Postgres.epoch_time % uptime_start
                self.cur.execute(up_epoch_time)
                total_epoch = self.cur.fetchall()
                if total_epoch:
                    uptime_sec = calendar.timegm(time.gmtime()) - total_epoch[0][0]
                    server_dict["upTime"] = round(uptime_sec / (60 * 60.0), 2)
            else:
                collectd.debug("Unable to fetch UpTime")

            # Getting number of connections of the server
            self.cur.execute(Postgres.server_connections)
            active_conn = self.cur.fetchall()
            if active_conn:
                server_dict["numConnections"] = active_conn[0][0]
            else:
                collectd.debug("Unable to fetch connections")

            # Getting shared buffer size of the server
            self.cur.execute(Postgres.shared_buffer)
            shared_buffer_size = self.cur.fetchall()
            if shared_buffer_size:
                server_dict["bufferSize"] = self.get_size(int(shared_buffer_size[0][1]))
            else:
                collectd.debug("Unable to fetch buffer size")

            # Getting effective cache size of the server
            self.cur.execute(Postgres.effective_cache)
            effective_cache_size = self.cur.fetchall()
            if effective_cache_size:
                server_dict["cacheSize"] = self.get_size(int(effective_cache_size[0][1]))
            else:
                collectd.debug("Unable to fetch cache size")

            # Getting max server connection
            self.cur.execute(Postgres.max_connection)
            max_conn = self.cur.fetchall()
            if max_conn:
                server_dict["maxConnections"] = max_conn[0][0]
            else:
                collectd.debug("Unable to fetch max connections")

            # Getting active process details
            self.cur.execute(Postgres.active_process)
            activeCount = self.cur.fetchall()
            if activeCount:
                server_dict["activeProcesses"] = activeCount[0][0]
            else:
                collectd.debug("Unable to fetch number of active processes")

            # Getting idle process details
            self.cur.execute(Postgres.idle_process)
            idleCount = self.cur.fetchall()
            if idleCount:
                server_dict["idleProcesses"] = idleCount[0][0]
            else:
                collectd.debug("Unable to fetch number of idle processes")

            if server_dict:
                server_dict['_documentType'] = 'serverDetails'
                final_server_dict["serverDetails"] = server_dict
            else:
                collectd.error("Couldn't fetch any server details")
        except Exception as e:
            collectd.error("Exception from the server_details due to %s in %s" %( e, traceback.format_exc()))
        return final_server_dict

    # Get the TOP 20 longest running queries from the server
    def get_query_details(self, final_long_runn_dict):
        try:
            self.cur.execute(Postgres.long_runn_query)
            long_runn_res = self.cur.fetchall()
            if long_runn_res:
                columns = map(lambda x: x[0], self.cur.description)
                query_list = [dict((zip(columns, row))) for row in long_runn_res]
                for query_dict in query_list:
                    if 'runtime' in query_dict.keys():
                        query_dict['runtime'] = query_dict['runtime'].seconds
                    query_dict["_documentType"] = "queryDetails"
                    final_long_runn_dict[query_dict["_queryName"] + query_dict["_dbName"]] = query_dict
            else:
                collectd.info("Couldn't get any queries which are running for the longer time")
        except Exception as e:
            collectd.error("Exception from the query_details due to %s in %s" %(e, traceback.format_exc()))
        return final_long_runn_dict

    # Get the postgres data
    def get_postgres_data(self, db_dict):
        try:
            db_info = self.get_inventory()
            # Getting details for all the databases present in the server
            for db_name in db_info:
                self.get_db_details(db_dict, db_name)
                if self.pollCounter > 1:
                    # Calculating cache hit ratio
                    sumOfHeap = self.heapBlksRead + self.heapBlksHit
                    if sumOfHeap == 0:
                        self.aggr_db_data["cacheHitRatio"] = 0.0
                        self.cacheHitRatio += self.aggr_db_data["cacheHitRatio"]
                        self.numdatabase -= 1
                    else:
                        self.aggr_db_data["cacheHitRatio"] = round((float(self.heapBlksHit) / sumOfHeap) * 100, 2)
                        self.cacheHitRatio += self.aggr_db_data["cacheHitRatio"]
                    # Calculating index hit ratio
                    sumIndexHeap = self.idxBlksRead + self.idxBlksHit
                    if sumIndexHeap == 0:
                        self.aggr_db_data["indexHitRatio"] = 0.0
                    else:
                        self.aggr_db_data["indexHitRatio"] = round((float(self.idxBlksHit) / sumIndexHeap) * 100, 2)
                    self.heapBlksHit = 0
                    self.heapBlksRead = 0
                    self.idxBlksHit = 0
                    self.idxBlksRead = 0
                # Updating aggregate values to db details
                for key, value in self.aggr_db_data.items():
                    db_dict[db_name][key] = value
                # Re-initialising aggregate values
                self.aggr_db_data = {'numLiveTuple': 0, 'numDeadTuple': 0, 'indexSize': 0, 'cacheHitRatio': 0,
                                             'indexHitRatio': 0}
                continue
        except Exception as e:
            collectd.error("Exception from the get_postgres_data due to %s in %s"%( e, traceback.format_exc()))
        return db_dict

    # Get all details per database
    def get_db_details(self, final_db_dict, db_name):
        try:
            # Getting specific db conn
            cur, connection_flag = self.connect_db(db_name)
            if connection_flag is True:
                # Collecting all the details for the provided database
                db_trans_query_org = Postgres.db_trans_query % db_name
                self.cur.execute(db_trans_query_org)
                db_trans_details = self.cur.fetchall()
                if db_trans_details:
                    fields = map(lambda x: x[0], self.cur.description)
                    db_details_list = [dict(zip(fields, row)) for row in db_trans_details]
                    db_details_dict = db_details_list[0]
                    if db_details_dict["tempFileSize"]:
                        db_details_dict["tempFileSize"] = round(db_details_dict["tempFileSize"] / (1024 * 1024.0), 2)
                    final_db_dict[db_name] = db_details_dict
                else:
                    collectd.debug("Unable to fetch db details of db %s"% db_name)
                db_size_query_org = Postgres.db_size_query % db_name
                self.cur.execute(db_size_query_org)
                db_size_details = self.cur.fetchall()
                if db_size_details:
                    final_db_dict[db_name]['dbSize'] = round(float(db_size_details[0][0]) / (1024 * 1024.0), 2)
                    self.aggr_server_data['dbSize'] += final_db_dict[db_name]['dbSize']
                else:
                    collectd.debug("Unable to fetch db size of db %s"% db_name)

                # Using specific db conn to get the count of tables instead of global conn
                cur.execute(Postgres.db_num_tables_query)
                num_tables = cur.fetchall()
                if num_tables:
                    final_db_dict[db_name]['numTables'] = num_tables[0][0]
                else:
                    collectd.debug("Unable to fetch number of tables in the db %s"% db_name)

                # Checking whether there are any details in the final dict
                if final_db_dict[db_name]:
                    final_db_dict[db_name]['_documentType'] = 'databaseDetails'
                    final_db_dict[db_name]['_dbName'] = db_name
                    # Finding the difference of values between two polls
                    if self.pollCounter > 1:
                        try:
                            final_dict_copy = deepcopy(final_db_dict[db_name])
                            previousPoll = self.pollDiff
                            final_db_dict[db_name]["numTransactions"] -= previousPoll[db_name]["numTransactions"]
                            final_db_dict[db_name]["transPerSec"] = final_db_dict[db_name]["numTransactions"] / int(self.interval)

                            self.aggr_server_data["numTransactions"] += final_db_dict[db_name]["numTransactions"]

                            final_db_dict[db_name]["blocksRead"] -= previousPoll[db_name]["blocksRead"]

                            final_db_dict[db_name]["blocksHit"] -= previousPoll[db_name]["blocksHit"]
                            self.aggr_server_data["cacheHits"] += final_db_dict[db_name]["blocksHit"]

                            final_db_dict[db_name]["numReturn"] -= previousPoll[db_name]["numReturn"]

                            final_db_dict[db_name]["numInsert"] -= previousPoll[db_name]["numInsert"]
                            self.aggr_server_data["numInsert"] += final_db_dict[db_name]["numInsert"]

                            final_db_dict[db_name]["numDelete"] -= previousPoll[db_name]["numDelete"]
                            self.aggr_server_data["numDelete"] += final_db_dict[db_name]["numDelete"]

                            final_db_dict[db_name]["numFetch"] -= previousPoll[db_name]["numFetch"]
                            self.aggr_server_data["numSelect"] += final_db_dict[db_name]["numFetch"]

                            final_db_dict[db_name]["numUpdate"] -= previousPoll[db_name]["numUpdate"]
                            self.aggr_server_data["numUpdate"] += final_db_dict[db_name]["numUpdate"]

                            final_db_dict[db_name]["numTempFile"] -= previousPoll[db_name]["numTempFile"]
                            self.aggr_server_data["numCreatedTempFiles"] += final_db_dict[db_name]["numTempFile"]

                            final_db_dict[db_name]["tempFileSize"] -= previousPoll[db_name]["tempFileSize"]
                            self.aggr_server_data["tempFileSize"] += final_db_dict[db_name]["tempFileSize"]

                            final_db_dict[db_name]["blkReadTime"] -= previousPoll[db_name]["blkReadTime"]
                            final_db_dict[db_name]["blkWriteTime"] -= previousPoll[db_name]["blkWriteTime"]
                            self.pollDiff[db_name] = final_dict_copy
                        except Exception as e:
                            collectd.error("Exception from the db_details due to %s in %s"% (e, traceback.format_exc()))
                            self.pollDiff[db_name] = final_db_dict[db_name]
                            final_db_dict[db_name] = {}
                else:
                    collectd.info("Couldn't get any details for the given db %s"% db_name)
                # To get table details for the given database
                if final_db_dict[db_name]:
                    if final_db_dict[db_name]['numTables'] != 0:
                        self.get_table_details(final_db_dict, db_name, cur)
                    else:
                        collectd.info("No tables found in the db %s from the db_details"% db_name)
            else:
                collectd.error("Connection to this database %s is not successful to get table details" % db_name)
        except Exception as e:
            collectd.error("Exception from the db_details due to %s in %s"% (e, traceback.format_exc()))
            return

    def get_table_details(self, final_dict, db_name, cursor):
        try:
            # Get the table details per database
            cursor.execute(Postgres.table_query)
            table_info = cursor.fetchall()
            fields = map(lambda x: x[0], cursor.description)
            if table_info:
                table_details_list = [dict(zip(fields, row)) for row in table_info]
                for table_dict in table_details_list:
                    if table_dict['tableSize']:
                        table_dict['tableSize'] = round(table_dict['tableSize'] / (1024 * 1024.0), 2)
                    if table_dict['indexSize']:
                        table_dict['indexSize'] = round(table_dict['indexSize'] / (1024 * 1024.0), 2)
                        self.aggr_server_data["indexSize"] += table_dict['indexSize']
                        self.aggr_server_data["indexSize"] = round(self.aggr_server_data["indexSize"], 2)
                        self.aggr_db_data["indexSize"] += table_dict['indexSize']
                        self.aggr_db_data["indexSize"] = round(self.aggr_db_data["indexSize"], 2)
                    if not table_dict['idxBlksRead']:
                        table_dict['idxBlksRead'] = 0
                    if not table_dict['idxBlksHit']:
                        table_dict['idxBlksHit'] = 0
                    table_dict['_documentType'] = "tableDetails"
                    table_dict['_dbName'] = db_name
                    final_dict[table_dict["_tableName"] + db_name] = table_dict

                # Executing static input/output query for the given table
                cursor.execute(Postgres.stat_table_query)
                stat_table_info = cursor.fetchall()
                if stat_table_info:
                    for tableRow in stat_table_info:
                        for table_dict in final_dict:
                            if table_dict == tableRow[0] + db_name:
                                final_dict[table_dict]["seqScan"] = tableRow[1]
                                final_dict[table_dict]["seqScanFetch"] = tableRow[2]

                                if not tableRow[3]:
                                    final_dict[table_dict]["indexScan"] = 0
                                else:
                                    final_dict[table_dict]["indexScan"] = tableRow[3]

                                if not tableRow[4]:
                                    final_dict[table_dict]["indexScanFetch"] = 0
                                else:
                                    final_dict[table_dict]["indexScanFetch"] = tableRow[4]

                                final_dict[table_dict]["numInsert"] = tableRow[5]
                                final_dict[table_dict]["numDelete"] = tableRow[6]
                                final_dict[table_dict]["numUpdate"] = tableRow[7]
                                final_dict[table_dict]["numLiveTuple"] = tableRow[8]
                                self.aggr_db_data["numLiveTuple"] += final_dict[table_dict]["numLiveTuple"]
                                final_dict[table_dict]["numDeadTuple"] = tableRow[9]
                                self.aggr_db_data["numDeadTuple"] += final_dict[table_dict]["numDeadTuple"]
                else:
                    collectd.info("No static table details found in the database %s"% db_name)
                # Finding the difference of values between two polls
                if table_info and stat_table_info:
                    if self.pollCounter > 1:
                        try:
                            previousPoll = self.pollDiff
                            for table_dict in table_details_list:
                                final_dict_table_copy = deepcopy(final_dict[table_dict["_tableName"] + db_name])
                                final_dict[table_dict["_tableName"] + db_name]["heapBlksRead"] -= \
                                previousPoll[table_dict["_tableName"] + db_name]["heapBlksRead"]
                                self.heapBlksRead += final_dict[table_dict["_tableName"] + db_name]["heapBlksRead"]

                                final_dict[table_dict["_tableName"] + db_name]["heapBlksHit"] -= \
                                previousPoll[table_dict["_tableName"] + db_name]["heapBlksHit"]
                                self.heapBlksHit += final_dict[table_dict["_tableName"] + db_name]["heapBlksHit"]

                                sumHit = final_dict[table_dict["_tableName"] + db_name]["heapBlksRead"] + \
                                         final_dict[table_dict["_tableName"] + db_name]["heapBlksHit"]
                                if sumHit == 0:
                                    final_dict[table_dict["_tableName"] + db_name]["cacheHitRatio"] = 0.0
                                else:
                                    final_dict[table_dict["_tableName"] + db_name]["cacheHitRatio"] = \
                                    round((float(final_dict[table_dict["_tableName"] + db_name]["heapBlksHit"]) / sumHit) * 100, 2)

                                final_dict[table_dict["_tableName"] + db_name]["idxBlksRead"] -= \
                                previousPoll[table_dict["_tableName"] + db_name]["idxBlksRead"]
                                self.idxBlksRead += final_dict[table_dict["_tableName"] + db_name]["idxBlksRead"]

                                final_dict[table_dict["_tableName"] + db_name]["idxBlksHit"] -= \
                                previousPoll[table_dict["_tableName"] + db_name]["idxBlksHit"]
                                self.idxBlksHit += final_dict[table_dict["_tableName"] + db_name]["idxBlksHit"]

                                idxSumHit = final_dict[table_dict["_tableName"] + db_name]["idxBlksRead"] + \
                                            final_dict[table_dict["_tableName"] + db_name]["idxBlksHit"]

                                if idxSumHit == 0:
                                    final_dict[table_dict["_tableName"] + db_name]["indexHitRatio"] = 0.0
                                else:
                                    final_dict[table_dict["_tableName"] + db_name]["indexHitRatio"] = \
                                    round((float(final_dict[table_dict["_tableName"] + db_name]["idxBlksHit"]) / idxSumHit) * 100, 2)

                                final_dict[table_dict["_tableName"] + db_name]["numInsert"] -= \
                                previousPoll[table_dict["_tableName"] + db_name]["numInsert"]

                                final_dict[table_dict["_tableName"] + db_name]["numDelete"] -= \
                                previousPoll[table_dict["_tableName"] + db_name]["numDelete"]

                                final_dict[table_dict["_tableName"] + db_name]["numUpdate"] -= \
                                previousPoll[table_dict["_tableName"] + db_name]["numUpdate"]

                                final_dict[table_dict["_tableName"] + db_name]["seqScan"] -= \
                                previousPoll[table_dict["_tableName"] + db_name]["seqScan"]

                                final_dict[table_dict["_tableName"] + db_name]["seqScanFetch"] -= \
                                previousPoll[table_dict["_tableName"] + db_name]["seqScanFetch"]

                                final_dict[table_dict["_tableName"] + db_name]["indexScanFetch"] -= \
                                previousPoll[table_dict["_tableName"] + db_name]["indexScanFetch"]

                                final_dict[table_dict["_tableName"] + db_name]["indexScan"] -= \
                                previousPoll[table_dict["_tableName"] + db_name]["indexScan"]

                                self.pollDiff[table_dict["_tableName"] + db_name] = final_dict_table_copy
                        except:
                            for table_dict in table_details_list:
                                self.pollDiff[table_dict["_tableName"] + db_name] = final_dict[table_dict["_tableName"] + db_name]
                                final_dict[table_dict["_tableName"] + db_name] = {}
                else:
                    collectd.error("Couldn't find table diffference details for the db %s"% db_name)

                # To collect the index_details for the given table
                self.get_index_details(final_dict, db_name, cursor)
            else:
                collectd.info("No table details found in the database %s"% db_name)
        except Exception as e:
            collectd.error("Exception from the get_table_details due to %s in %s" %( e, traceback.format_exc()))
            return

    # To get index details for the given database
    def get_index_details(self, final_db_dict, db_name, cur):
        try:
            cur.execute(Postgres.index_query)
            index_query_info = cur.fetchall()
            fields = map(lambda x: x[0], cur.description)
            if index_query_info:
                index_details_list = [dict(zip(fields, row)) for row in index_query_info]
                for index_dict in index_details_list:
                    index_dict['_documentType'] = "indexDetails"
                    index_dict['_dbName'] = db_name
                    final_db_dict[index_dict["_indexName"] + db_name] = index_dict
                # Executing statio input/output query for the given table
                cur.execute(Postgres.stat_index_query)
                stat_index_query_info = cur.fetchall()
                if stat_index_query_info:
                    for stat_index in stat_index_query_info:
                        for index_dict in final_db_dict:
                            if index_dict == stat_index[2] + db_name:
                                final_db_dict[index_dict]["blksRead"] = stat_index[3]
                                final_db_dict[index_dict]["blksHit"] = stat_index[4]
                else:
                    collectd.debug("Couldn't get any statio index details for the given db %s"% db_name)
                # Finding the difference of values between two polls
                if index_query_info and stat_index_query_info:
                    if self.pollCounter > 1:
                        try:
                            previousPoll = self.pollDiff
                            for index_dict in index_details_list:
                                final_dict_index_copy = deepcopy(final_db_dict[index_dict["_indexName"] + db_name])
                                final_db_dict[index_dict["_indexName"] + db_name]["numReturn"] -= \
                                previousPoll[index_dict["_indexName"] + db_name]["numReturn"]

                                final_db_dict[index_dict["_indexName"] + db_name]["indexScan"] -= \
                                previousPoll[index_dict["_indexName"] + db_name]["indexScan"]

                                final_db_dict[index_dict["_indexName"] + db_name]["numFetch"] -= \
                                previousPoll[index_dict["_indexName"] + db_name]["numFetch"]

                                final_db_dict[index_dict["_indexName"] + db_name]["blksHit"] -= \
                                previousPoll[index_dict["_indexName"] + db_name]["blksHit"]

                                final_db_dict[index_dict["_indexName"] + db_name]["blksRead"] -= \
                                previousPoll[index_dict["_indexName"] + db_name]["blksRead"]

                                self.pollDiff[index_dict["_indexName"] + db_name] = final_dict_index_copy
                        except:
                            for index_dict in index_details_list:
                                self.pollDiff[index_dict["_indexName"] + db_name] = final_db_dict[index_dict["_indexName"] + db_name]
                                final_db_dict[index_dict["_indexName"] + db_name] = {}
                else:
                    collectd.debug("Couldn't get index difference details and statio details for the given db %s"% db_name)
            else:
                collectd.debug("Couldn't get any index details for the given db %s"% db_name)
        except Exception as e:
            collectd.error("Exception from the index_details due to %s in %s"%( e, traceback.format_exc()))
            return

    # To collect all the postgres data
    def collect_data(self):
        server_details = self.get_server_details()
        query_details = self.get_query_details(server_details)
        db_details = self.get_postgres_data(query_details)

        #Calculating cacheHitRatio in server level
        if self.pollCounter > 1:
            try:
                self.aggr_server_data["cacheHitRatio"] = round(float(self.cacheHitRatio / self.numdatabase), 2)
            except:
                self.aggr_server_data["cacheHitRatio"] = 0.0

        #Updating aggregate values to server details
        for key, value in self.aggr_server_data.items():
            db_details["serverDetails"][key] = value
        # Re-initialising aggregate values
        self.aggr_server_data = {'dbSize': 0, 'numDelete': 0, 'numUpdate': 0, 'numInsert': 0, 'numSelect': 0, \
                                 'numTransactions': 0, 'cacheHits': 0, 'numCreatedTempFiles': 0, 'indexSize': 0,
                                 'tempFileSize': 0, 'cacheHitRatio': 0}
        self.cacheHitRatio = 0
        self.numdatabase = 0

        if not db_details:
            collectd.error("Couldn't fetch any details from the Postgres server")
            return

        # Add common parameters
        self.add_common_params(db_details)
        return db_details

    @staticmethod
    def add_common_params(postgres_dict):
        hostname = gethostname()
        timestamp = int(round(time.time()))

        for details_type, details in postgres_dict.items():
            details[HOSTNAME] = hostname
            details[TIMESTAMP] = timestamp
            details[PLUGIN] = Postgres.POSTGRES
            details[ACTUALPLUGINTYPE] = Postgres.POSTGRES
            #Grouping all long running queries into single dir "queryDetails" in /data/postgres
            if details["_documentType"] == "queryDetails":
                details[PLUGIN_INS] = "queryDetails"
            else:
                details[PLUGIN_INS] = details_type

    def read(self):
        try:
            self.pollCounter += 1
            self.connect_postgres()
            # collect data
            dict_postgres = self.collect_data()
            #collectd.info(dict_postgres)
            if dict_postgres:
                if self.pollCounter == 1:
                    self.pollDiff = deepcopy(dict_postgres)
            else:
                collectd.error("Plugin Postgres: Unable to fetch data for Postgres.")
                return

            # dispatch data to collectd, copying by value
            if self.pollCounter > 1:
                # Deleteing documentsTypes which were not requetsed
                for doc in dict_postgres.keys():
                    if dict_postgres[doc]['_documentType'] not in self.documentsTypes:
                        del dict_postgres[doc]
                self.dispatch_data(deepcopy(dict_postgres))
        except Exception as e:
                #collectd.error("%s" % traceback.format_exc())
            collectd.error("Couldn't read and gather the postgres metrics due to the exception :%s" % e)
            return

    @staticmethod
    def dispatch_data(dict_disks_copy):
        for details_type, details in dict_disks_copy.items():
            collectd.info("Plugin Postgres: Values: " + json.dumps(details))
            collectd.info("final details are : %s" % details)
            dispatch(details)

    def get_size(self, data):
        byte_size = data * 8192
        return byte_size/(1024 * 1024)

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))

def init():
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)


obj = PostgresStats()
collectd.register_config(obj.read_config)
collectd.register_read(obj.read_temp)
