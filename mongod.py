import collectd
import pymongo
from pymongo import MongoClient
import signal
import json
import time
from copy import deepcopy
from constants import *
from utils import *
from libdiskstat import *
from pymongo import MongoClient

class MongoStats():
    def __init__(self):
        self.host = 'localhost'
        self.port = None 
        self.pollCounter = 0
        self.conn = None
        self.list_dbs = None
        self.mong_data = None
        self.status = {}
        self.user = None
        self.password = None 
        self.hosts = []
        self.interval = 0
        self.prev_slowqueries = {}
        self.aggr_server_data = {'dbSize':0, 'indexSize': 0}
        self.previous_data = {"bytesReceived":0,"virtualmem":0,"residentmem":0,"bytesSent":0,"numdelete":0,"numinsert":0,"numselect":0,"numupdate":0,"readQueue":0,"writeQueue":0,"readThreadsrun":0,"writeThreadsrun":0,"readThreadsavl":0,"writeThreadsavl":0,"numAbortedclients":0,"cachesize":0,"cacheusedsize":0,"cachedirtysize":0,"readrequest_queue":0,"numqueries":0
,"writerequest_queue":0,"msgasserts":0,"warningasserts":0,"regularasserts":0,"userasserts":0,"totalcursors":0,"pinnedcursors":0,"notimedoutcursors":0}

    def read_config(self, cfg):
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == PORT:
                self.port = children.values[0]
            if children.key == DOCUMENTSTYPES:
                self.documentsTypes = children.values[0]
            if children.key == USER:
                self.user = children.values[0]
            if children.key == PASSWORD:
                self.password = children.values[0]

    def connect_mongo(self):
        try:
            retry_flag = True
            retry_count = 0
            while retry_flag:
                try:
                    self.conn = MongoClient(self.host+':'+self.port,username=self.user,password=self.password,authMechanism='SCRAM-SHA-1') 
                    self.db = self.conn.admin
                    retry_flag = False
                    collectd.info("Connection to Mongo successfull in attempt %s" % (retry_count))
                except Exception as e:
                    collectd.error("Retry after 5 sec as connection to Mongo failed in attempt %s" % (retry_count))
                    retry_count += 1
                    time.sleep(5)
        except Exception as e:
            collectd.error("Exception in the connect_mongo due to %s" % e)
            return

    def get_all_db(self):
        db_name=[]
        try:
            db_details = self.db.command({'listDatabases': 1, 'nameOnly': True})
            if db_details:
                for each_db in db_details['databases']:
                    db_name.append(each_db['name'])
                    if not each_db['name'] in self.prev_slowqueries:
                        self.prev_slowqueries[each_db['name']] = 0 
            else:
                collectd.info("No databases present in the server: %s"% self.host)
        except Exception as e:
            collectd.error("Exception from the get_all_db")
        return db_name

    def connect_db(self, dbname):
        try:
            conn = MongoClient(self.host+':'+self.port,username=self.user,password=self.password,authMechanism='SCRAM-SHA-1') 
            db = conn[dbname]
            conn_flag = True
        except Exception as e:
            collectd.error("Exception in the connect_db due to %s" % e)
            conn_flag = False
            db = None
        return db, conn_flag


    def get_db_data(self, final_dict, db_name):
        try:
            db_cur, connection_flag = self.connect_db(db_name)
            if db_cur and connection_flag:
                dbStatus= db_cur.command('dbStats')
                if dbStatus:
                    final_dict[db_name] = dict()
                    final_dict[db_name]['dbSize'] = dbStatus['dataSize']
                    self.aggr_server_data['dbSize'] += final_dict[db_name]['dbSize']
                    #self.aggr_server_data["dbSize"] = self.aggr_server_data["dbSize"]
                    final_dict[db_name]["indexSize"] = dbStatus['indexSize']
                    self.aggr_server_data["indexSize"] += final_dict[db_name]["indexSize"]
                    #self.aggr_server_data["indexSize"] = self.aggr_server_data["indexSize"]
                    final_dict[db_name]["numCollections"] = dbStatus['collections']
                    final_dict[db_name]["storageSize"] = dbStatus['storageSize']
                else:
                    logging.debug("Unable to get database details")
                if final_dict[db_name]:
                    final_dict[db_name]['_documentType'] = 'databaseDetails'
                    final_dict[db_name]['_dbName'] = db_name
                    col = db_cur['system.profile']
                    final_dict[db_name]['slowqueries'] ,self.prev_slowqueries[db_name]= int(col.count_documents({})) - self.prev_slowqueries[db_name],int(col.count_documents({}))
                else:
                    collectd.info("Couldn't get any details for the given db ")

                    # To get table details for the given database
                if final_dict[db_name]['numCollections'] != 0:
                    final_dict = self.get_table_details(final_dict, db_name)
                else:
                    collectd.info("No tables found ")
            else:
                collectd.error("Connection to this database is not successful to get table details ")
        except Exception as e:
            collectd.error("Exception from the db_details")
            return
        return final_dict


    def get_table_details(self, final_dict, db_name):
        try:
            db, con_flag = self.connect_db(db_name)
            coll_list = db.collection_names(include_system_collections=False)
            agg_db_data = {"size" : 0, "indexSize" : 0,"numdoc":0,"storageSize":0}
            for coll_name in coll_list:
                coll_dict = {}
                coll_stats = db.command("collstats", coll_name)
                coll_dict["_dbname"] = db_name
                coll_dict["_numdoc"] = coll_stats["count"]
                agg_db_data["numdoc"] += coll_dict["_numdoc"]
                coll_dict["_dbstoragesize"] = coll_stats["storageSize"]
                agg_db_data["storageSize"] += coll_dict["_dbstoragesize"]
                coll_dict["_collname"] = coll_name
                coll_dict["_documentType"] = "tableDetails"
                coll_dict["_collsize"] = coll_stats["size"]
                agg_db_data["size"] += coll_dict["_collsize"]
                coll_dict["_totalindexsize"]= coll_stats["totalIndexSize"]
                agg_db_data["indexSize"] += coll_dict["_totalindexsize"]
                final_dict[coll_dict["_collname"]] = coll_dict
            final_dict[db_name]["size"] = long(agg_db_data["size"])
            #final_dict[db_name]["indexSize"] = long(agg_db_data["indexSize"])
            final_dict[db_name]["numdoc"] = long(agg_db_data["numdoc"])
            #final_dict[db_name]["storageSize"] = long(agg_db_data["storageSize"])
        except Exception as exe:
            collectd.error("Unable to execute the query under table:%s" % exe)
            return
        return final_dict


    def get_server_data(self):
        final_dict={}
        server_dict={}
        try:
            db, con_flag = self.connect_db("admin")
            db_list = self.get_all_db()
            if con_flag:
               server_stats = self.db.command("serverStatus")
               server_dict['uptime'] = server_stats['uptime']
               server_dict['version'] = server_stats['version']
               server_dict["cachesize"] =  int(server_stats['wiredTiger']['cache']["maximum bytes configured"])
               server_dict["cacheusedsize"] =  int(server_stats['wiredTiger']['cache']["bytes currently in the cache"])
               server_dict["cachedirtysize"] =  int(server_stats['wiredTiger']['cache']['tracked dirty bytes in the cache'])
               server_dict['numdatabases'] = len(db_list)
               server_dict['_documentType'] = "serverDetails"
               server_dict['numconnections'] = server_stats['connections']['current']
               server_dict['numunusedconnections'] = server_stats['connections']['available']
               server_dict['storage_engine'] = server_stats['storageEngine']['name']
               server_dict['totalcreatedconnections'] = server_stats['connections']['totalCreated']
               if self.pollCounter <= 1 or not "bytesReceived" in self.previous_data.keys():
                   self.previous_data["bytesReceived"] = int(server_stats['network']['bytesIn'])
                   self.previous_data["bytesSent"] = int(server_stats['network']['bytesOut'])
                   self.previous_data["numdelete"] = int(server_stats['opcounters']['delete'])
                   self.previous_data["numinsert"] = int(server_stats['opcounters']['insert'])
                   self.previous_data["numselect"] = int(server_stats['opcounters']['query'])
                   self.previous_data["numqueries"] = int(server_stats['opcounters']['command'])
                   self.previous_data["numupdate"] = int(server_stats['opcounters']['update'])
                   self.previous_data["readQueue"] = int(server_stats['globalLock']['activeClients']['readers'])
                   self.previous_data["writeQueue"] = int(server_stats['globalLock']['activeClients']['writers'])
                   self.previous_data["readThreadsrun"] = int(server_stats['wiredTiger']['concurrentTransactions']['read']['out'])
                   self.previous_data["writeThreadsrun"] = int(server_stats['wiredTiger']['concurrentTransactions']['write']['out'])
                   self.previous_data["readThreadsavl"] = int(server_stats['wiredTiger']['concurrentTransactions']['read']['available'])
                   self.previous_data["writeThreadsavl"] = int(server_stats['wiredTiger']['concurrentTransactions']['write']['available'])
                   self.previous_data["numAbortedclients"] = int(server_stats['connections']['totalCreated'] - server_stats['connections']['current'])
                   self.previous_data['virtualmem'] = int(server_stats['mem']['virtual'])
                   self.previous_data['residentmem'] = int(server_stats['mem']['resident'])
                   #self.previous_data["cachesize"] = int(server_stats['wiredTiger']['cache']["maximum bytes configured"])
                   #self.previous_data["cacheusedsize"] = int(server_stats['wiredTiger']['cache']['bytes currently in the cache'])
                   #self.previous_data["cachedirtysize"] = int(server_stats['wiredTiger']['cache']['tracked dirty bytes in the cache'])
                   self.previous_data['readrequest_queue'] = int(server_stats['globalLock']["currentQueue"]["readers"])
                   self.previous_data['writerequest_queue'] = int(server_stats['globalLock']["currentQueue"]["writers"])
                   self.previous_data['msgasserts'] = int(server_stats['asserts']['msg'])
                   self.previous_data['warningasserts'] = int(server_stats['asserts']['warning'])
                   self.previous_data['regularasserts'] = int(server_stats['asserts']['regular'])
                   self.previous_data['userasserts'] = int(server_stats['asserts']['user'])
                   self.previous_data['totalcursors'] = int(server_stats['metrics']['cursor']['open']['total'])
                   self.previous_data['pinnedcursors'] = int(server_stats['metrics']['cursor']['open']['pinned'])
                   self.previous_data['notimedoutcursors'] = int(server_stats['metrics']['cursor']['open']['noTimeout'])
                   server_dict["bytesReceived"] = 0
                   server_dict["bytesSent"] = 0
                   server_dict["numdelete"] = 0
                   server_dict["numinsert"] = 0
                   server_dict["numselect"] = 0
                   server_dict["numupdate"] = 0
                   server_dict["numqueries"] = 0
                   server_dict["readQueue"] = 0
                   server_dict["writeQueue"] = 0
                   server_dict["readThreadsrun"] = 0
                   server_dict["writeThreadsrun"] = 0
                   server_dict["readThreadsavl"] = 0
                   server_dict["writeThreadsavl"] = 0
                   server_dict["numAbortedclients"] = 0
                   #server_dict["cachesize"] = 0
                   server_dict["virtualmem"] = 0
                   server_dict["residentmem"] = 0
                   #server_dict["cacheusedsize"] = 0
                   #server_dict["cachedirtysize"] = 0
                   server_dict["readrequest_queue"] = 0
                   server_dict["writerequest_queue"] = 0
                   server_dict["msgasserts"] = 0
                   server_dict["warningasserts"] = 0
                   server_dict["regularasserts"] = 0
                   server_dict["userasserts"] = 0
                   server_dict["totalcursors"] = 0
                   server_dict["pinnedcursors"] = 0
                   server_dict["notimedoutcursors"] = 0
               else:
                   server_dict["bytesReceived"],self.previous_data["bytesReceived"] = int(server_stats['network']['bytesIn'])-self.previous_data["bytesReceived"],int(server_stats['network']['bytesIn'])
                   server_dict["virtualmem"],self.previous_data["virtualmem"] = int(server_stats['mem']['virtual']) - self.previous_data["virtualmem"],int(server_stats['mem']['virtual'])
                   server_dict["residentmem"],self.previous_data["residentmem"] = int(server_stats['mem']['resident']) -self.previous_data["residentmem"],int(server_stats['mem']['resident'])
                   server_dict["bytesSent"],self.previous_data["bytesSent"] =  int(server_stats['network']['bytesOut']) - self.previous_data["bytesSent"],int(server_stats['network']['bytesOut'])
                   server_dict["numdelete"],self.previous_data["numdelete"] =  int(server_stats['opcounters']['delete']) - self.previous_data["numdelete"],int(server_stats['opcounters']['delete'])
                   server_dict["numqueries"],self.previous_data["numqueries"] = int(server_stats['opcounters']['command']) - self.previous_data["numqueries"],int(server_stats['opcounters']['command'])
                   server_dict["numinsert"],self.previous_data["numinsert"] =  int(server_stats['opcounters']['insert']) - self.previous_data["numinsert"] ,int(server_stats['opcounters']['insert'])
                   server_dict["numselect"],self.previous_data["numselect"] =  int(server_stats['opcounters']['query'])-self.previous_data["numselect"],int(server_stats['opcounters']['query'])
                   server_dict["numupdate"],self.previous_data["numupdate"] =  int(server_stats['opcounters']['update']) -self.previous_data["numupdate"],int(server_stats['opcounters']['update'])
                   server_dict["readQueue"],self.previous_data["readQueue"] =  int(server_stats['globalLock']['activeClients']['readers']) - self.previous_data["readQueue"],int(server_stats['globalLock']['activeClients']['readers'])
                   server_dict["writeQueue"],self.previous_data["writeQueue"] =  int(server_stats['globalLock']['activeClients']['writers']) - self.previous_data["writeQueue"],int(server_stats['globalLock']['activeClients']['writers'])
                   server_dict["readThreadsrun"],self.previous_data["readThreadsrun"] =  int(server_stats['wiredTiger']['concurrentTransactions']['read']['out']) - self.previous_data["readThreadsrun"],int(server_stats['wiredTiger']['concurrentTransactions']['read']['out'])
                   server_dict["writeThreadsrun"],self.previous_data["writeThreadsrun"] =  int(server_stats['wiredTiger']['concurrentTransactions']['write']['out']) - self.previous_data["writeThreadsrun"] ,int(server_stats['wiredTiger']['concurrentTransactions']['write']['out'])
                   server_dict["readThreadsavl"],self.previous_data["readThreadsavl"] =  int(server_stats['wiredTiger']['concurrentTransactions']['read']['available']) - self.previous_data["readThreadsavl"],int(server_stats['wiredTiger']['concurrentTransactions']['read']['available'])
                   server_dict["writeThreadsavl"],self.previous_data["writeThreadsavl"] =  int(server_stats['wiredTiger']['concurrentTransactions']['write']['available']) - self.previous_data["writeThreadsavl"] ,int(server_stats['wiredTiger']['concurrentTransactions']['write']['available'])


                   server_dict["numAbortedclients"],self.previous_data["numAbortedclients"] =  int(server_stats['connections']['totalCreated'] - server_stats['connections']['current']) - self.previous_data["numAbortedclients"],(int(server_stats['connections']['totalCreated'] - server_stats['connections']['current']))
                   server_dict["readrequest_queue"],self.previous_data['readrequest_queue'] = int(server_stats['globalLock']["currentQueue"]["readers"]) -  self.previous_data['readrequest_queue'],int(server_stats['globalLock']["currentQueue"]["readers"])
                   server_dict["writerequest_queue"],self.previous_data['writerequest_queue'] =  int(server_stats['globalLock']["currentQueue"]["writers"]) - self.previous_data['writerequest_queue'] ,int(server_stats['globalLock']["currentQueue"]["writers"])
                   server_dict["msgasserts"],self.previous_data['msgasserts'] =  int(server_stats['asserts']['msg']) - self.previous_data['msgasserts'] ,int(server_stats['asserts']['msg'])
                   server_dict["warningasserts"],self.previous_data['warningasserts'] =  int(server_stats['asserts']['warning']) - self.previous_data['warningasserts'] ,int(server_stats['asserts']['warning'])
                   server_dict["regularasserts"],self.previous_data['regularasserts'] =  int(server_stats['asserts']['regular']) - self.previous_data['regularasserts'] ,int(server_stats['asserts']['regular'])
                   server_dict["userasserts"],self.previous_data['userasserts'] =  int(server_stats['asserts']['user']) - self.previous_data['userasserts'] ,int(server_stats['asserts']['user'])
                   server_dict["totalcursors"],self.previous_data['totalcursors'] =  int(server_stats['metrics']['cursor']['open']['total']) - self.previous_data['totalcursors'] ,int(server_stats['metrics']['cursor']['open']['total'])
                   server_dict["pinnedcursors"],self.previous_data['pinnedcursors'] =int(server_stats['metrics']['cursor']['open']['pinned']) - self.previous_data['pinnedcursors'] ,int(server_stats['metrics']['cursor']['open']['pinned'])
                   server_dict["notimedoutcursors"],self.previous_data['notimedoutcursors'] =  int(server_stats['metrics']['cursor']['open']['noTimeout']) - self.previous_data['notimedoutcursors'],int(server_stats['metrics']['cursor']['open']['noTimeout']
)
            else:
                collectd.info("Cannot connect to database")
            final_dict['serverDetails']=server_dict
        except Exception as err:
            collectd.error("Unable to execute the provided query:%s" % err)
            return
        return final_dict









    @staticmethod
    def add_common_params(mongo_dict):
        hostname = gethostname()
        timestamp = int(round(time.time()))
        for details_type, details in mongo_dict.items():
            details[HOSTNAME] = hostname
            details[TIMESTAMP] = timestamp
            details[PLUGIN] = MONGO
            details[ACTUALPLUGINTYPE] = MONGO
            details[PLUGIN_INS] = details_type

    def collect_data(self):
        # get data of Mongo
        server_details = self.get_server_data()
        db_list = self.get_all_db()
        for db_name in db_list:
            final_details = self.get_db_data(server_details, db_name)
        if not final_details:
            collectd.error("Plugin Mongo: Unable to fetch data information of Mongo.")
            return

        # Add common parameters
        self.add_common_params(final_details)
        return final_details

    @staticmethod
    def dispatch_data(dict_disks_copy):
        for details_type, details in dict_disks_copy.items():
            collectd.info("Plugin Mongo: Values: send successfully")
            collectd.info("final details are : %s" % json.dumps(details))
            dispatch(details)


    def read(self):
        try:
            self.pollCounter += 1
            self.connect_mongo()
            # collect data
            dict_mongo = self.collect_data()
            if not dict_mongo:
                collectd.error("Plugin Mongo: Unable to fetch data for Mongo.")
                return
            else:
                collectd.error("Writing data to ES")
                # Deleteing documentsTypes which were not requetsed
                collectd.info("Final Dict %s" % dict_mongo)
                for doc in dict_mongo.keys():
                    if dict_mongo[doc]['_documentType'] not in self.documentsTypes:
                        del dict_mongo[doc]
            # dispatch data to collectd, copying by value
            self.dispatch_data(dict_mongo)
          
            collectd.error("Done Writing data to ES")
        except Exception as e:
            collectd.error("Couldn't read and gather the SQL metrics due to the exception :%s" % e)
            return


    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))

    def init():
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)


obj = MongoStats()
collectd.register_config(obj.read_config)
collectd.register_read(obj.read_temp)


