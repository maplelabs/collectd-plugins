""" A collectd-python plugin for retrieving
    metrics from Redis Database server.
    """

import collectd
import signal
import time
import json
import redis as red
import traceback
import subprocess

# user imports
from constants import *
from utils import *
from copy import deepcopy


class RedisStats:
    def __init__(self):
        self.interval = DEFAULT_INTERVAL
        self.host = 'localhost'
        self.user = None
        self.password = None
        self.port = None
        self.redis_client = None
        self.pollCounter = 0
        self.documentsTypes = []
        self.previousData = {}

    def read_config(self, cfg):
        #with open('/home/ubuntu/read_config.txt', 'w') as file:
        #    file.write(cfg)
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == PORT:
                self.port = children.values[0]
                #with open('/home/ubuntu/in_port.txt', 'w') as file:
                #    file.write('Hi there!')
            if children.key == USER:
                self.user = children.values[0]
            if children.key == PASSWORD:
                self.password = children.values[0]
            if children.key == DOCUMENTSTYPES:
                self.documentsTypes = children.values[0]

    def connect_redis(self):
        try:
            retry_flag = True
            retry_count = 0
            password_flag = False
            while retry_flag and retry_count < 3:
                try:
                    if password_flag:
                        self.redis_client = red.StrictRedis(host=self.host, port=self.port, password=self.password, db=0)
                    else:
                        self.redis_client = red.StrictRedis(host=self.host, port = self.port, db=0)
                    server_details = self.redis_client.info(section="server")
                    retry_flag = False
                    collectd.info("Connection to Redis successfull in attempt %s" % (retry_count))
                except Exception as e:
                    collectd.error("Retry after 5 sec as connection to Redis failed in attempt %s" % (retry_count))
                    retry_count += 1
                    if (not password_flag) and retry_count == 3:
                        retry_count = 0
                        password_flag = True
                    time.sleep(5)
        except Exception as e:
            collectd.error("Exception in the connect_redis due to %s" % e)
            return

    def get_redis_server_data(self):
        final_redis_dict = {}
        server_dict = {}
        try:
            #for i in dir(self):
            #    collectd.error(i)
            #collectd.error(self['port'])
            server_details = self.redis_client.info(section="server")
            collectd.error('after call server function')
            if server_details:
                server_dict["version"] = server_details["redis_version"]
                server_dict["buildId"] = server_details["redis_build_id"]
                server_dict["mode"] = server_details["redis_mode"]
                server_dict["os"] = server_details["os"]
                server_dict["gccVersion"] = server_details["gcc_version"]
                server_dict["upTime"] = server_details["uptime_in_seconds"]
            else:
                collectd.error("No general details found")
                return final_redis_dict
            server_conn_details = self.redis_client.info("clients")
            if server_conn_details:
                server_dict["blockedClients"] = server_conn_details["blocked_clients"]
                server_dict["connectedClients"] = server_conn_details["connected_clients"]
            else:
                collectd.error("No connection details found")
                return final_redis_dict
            server_dict[PLUGINTYPE] = "generalDetails"
            final_redis_dict["generalDetails"] = server_dict
        except Exception as e:
            collectd.error(traceback.format_exc())
            collectd.error("Unable to get general  ggg details due to %s" % str(e))
            return final_redis_dict
        return final_redis_dict

    def get_server_stats(self, final_redis_dict):
        stats_dict = {}
        try:
            server_stats = self.redis_client.info(section="stats")
            if server_stats:
                input_bytes = None
                try:
                    input_bytes = round(server_stats["total_net_input_bytes"]/(1024.0*1024.0), 2)
                except Exception as e:
                    collectd.error("Error in getting total input bytes due to %s" % str(e))

                output_bytes = None
                try:
                    output_bytes = round(server_stats["total_net_output_bytes"]/ (1024.0 * 1024.0), 2)
                except Exception as e:
                    collectd.error("Error in getting total input bytes due to %s" % str(e))
                collectd.error('Before mettt')
                stats_dict["rejectedConn"] = server_stats["rejected_connections"]
                stats_dict["expiredKeys"] = server_stats["expired_keys"]
                stats_dict["evictedKeys"] = server_stats["evicted_keys"]
                stats_dict["instantaneousopspersec"] = server_stats["instantaneous_ops_per_sec"]
                if self.pollCounter <= 1:
                    self.previousData["totalConnReceived"] = server_stats["total_connections_received"]
                    self.previousData["totalCommandsProcessed"] = server_stats["total_commands_processed"]
                    self.previousData["totalNetInputBytes"] = input_bytes
                    self.previousData["totalNetOuputBytes"] = output_bytes
                    self.previousData["keyspaceHits"] = server_stats["keyspace_hits"]
                    self.previousData["keyspaceMisses"] = server_stats["keyspace_misses"]
                    stats_dict["totalConnReceived"] = 0
                    stats_dict["totalCommandsProcessed"] = 0
                    stats_dict["totalNetInputBytes"] = 0
                    stats_dict["totalNetOutputBytes"] = 0
                    stats_dict["keyspaceHits"] = 0
                    stats_dict["keyspaceMisses"] = 0
                    stats_dict["keyspaceHitRate"] = 0.0
                    stats_dict["keyspaceMissRate"] = 0.0
                    stats_dict["writeThroughput"] = 0.0
                    stats_dict["readThroughput"] = 0.0
                else:
                    stats_dict["totalConnReceived"] = server_stats["total_connections_received"]
                    stats_dict["totalCommandsProcessed"] = server_stats["total_commands_processed"]
                    stats_dict["totalNetInputBytes"] = self.previousData["totalNetInputBytes"] - input_bytes
                    stats_dict["totalNetOutputBytes"] = self.previousData["totalNetInputBytes"] - output_bytes
                    stats_dict["keyspaceHits"] = server_stats["keyspace_hits"]
                    stats_dict["keyspaceMisses"] = server_stats["keyspace_misses"]
                    if ((server_stats["keyspace_hits"] > 0) or (server_stats["keyspace_misses"] > 0)):
                        stats_dict["keyspaceHitRate"] = round(float(server_stats["keyspace_hits"]/(server_stats["keyspace_hits"] + server_stats["keyspace_misses"])), 2)
                        stats_dict["keyspaceMissRate"] = round(float(server_stats["keyspace_misses"] / (server_stats["keyspace_hits"] + server_stats["keyspace_misses"])), 2)
                    else:
                        stats_dict["keyspaceHitRate"] = 0
                        stats_dict["keyspaceMissRate"] = 0
                    #collectd.error(json.dumps(server_stats))
                    stats_dict["readThroughput"] = round(float(float(server_stats["total_net_input_bytes"]) / int(self.interval)), 2)
                    stats_dict["writeThroughput"] = round(float(float(server_stats["total_net_output_bytes"]) / int(self.interval)), 2)
                keyspace_details=self.redis_client.info("keyspace")
                if keyspace_details:
                    totalk = 0
                    for k,v in keyspace_details.items():
                        totalk += int(v["keys"])
                    stats_dict["totalKeys"] = totalk
                else:
                    collectd.error("No Key details found")
                    stats_dict["totalKeys"] = 0
                outlis = subprocess.check_output(["redis-cli", "--intrinsic-latency", "1"]).split( )
                if len(outlis) > 0:
                    try:
                        stats_dict["latency"] = float(outlis[-16])
                    except ValueError:
                        collectd.error("No latency details found")
                        stats_dict["latency"] = 0
                stats_dict[PLUGINTYPE] = "serverDetails"
                collectd.error('last one')
                final_redis_dict["serverDetails"] = stats_dict
            else:
                collectd.error("No server stats found")
                return final_redis_dict
        except Exception as e:
            collectd.error("Unable to get server details due to %s" % str(e))
            return final_redis_dict
        return final_redis_dict

    def get_memory_stats(self, final_redis_dict):
        memory_dict = {}
        try:
            memory_stats = self.redis_client.info(section="memory")
            if memory_stats:
                memory_dict["memoryUsed"] = round(memory_stats["used_memory"]/(1024.0*1024.0), 2)
                memory_dict["memoryUsedPeak"] = round(memory_stats["used_memory_peak"] / (1024.0 * 1024.0), 2)
                #memory_dict["memoryUsedPeakPer"] = memory_stats["used_memory_peak_perc"]
                #memory_dict["memoryUsedStartup"] = round(memory_stats["used_memory_startup"] / (1024.0 * 1024.0), 2)
                memory_dict["totalSystemMemory"] = round(memory_stats["total_system_memory"] / (1024.0 * 1024.0), 2)
                memory_dict["memFragmentationRatio"] = memory_stats["mem_fragmentation_ratio"]
                memory_dict["memoryAllocator"] = memory_stats["mem_allocator"]
            else:
                collectd.error("No memory stats found")
                return
            persistence_stats = self.redis_client.info(section="persistence")
            if persistence_stats:
                memory_dict["lastSaveTime"] = persistence_stats["rdb_last_save_time"]
                memory_dict["lastSaveChanges"] = persistence_stats["rdb_changes_since_last_save"]
            else:
                collectd.error("No persistence stats found")
                return final_redis_dict
            memory_dict[PLUGINTYPE] = "memoryDetails"
            final_redis_dict["memoryDetails"] = memory_dict
        except Exception as e:
            collectd.error("Unable to get memory details due to %s" % str(e))
            return final_redis_dict
        return final_redis_dict

    def get_replication_stats(self, final_redis_dict):
        rep_dict = {}
        try:
            rep_stats = self.redis_client.info(section="replication")
            if rep_stats:
                rep_dict["role"] = rep_stats["role"]
                rep_dict["connectedSlaves"] = rep_stats["connected_slaves"]
                rep_dict["masterLinkStatus"] = rep_stats.get("master_link_status", None)
                rep_dict["masterLastIOSecsAgo"] = rep_stats.get("master_last_io_seconds_ago", None)
                rep_dict["masterLinkDownSinceSecs"] = rep_stats.get("master_link_down_since_seconds", None)
                rep_dict[PLUGINTYPE] = "replicationDetails"
                final_redis_dict["replicationDetails"] = rep_dict
            else:
                return final_redis_dict
        except Exception as e:
            collectd.error("Unable to replication details due to %s" % str(e))
            return final_redis_dict
        return final_redis_dict

    def get_performance_details(self, final_redis_dict):
        per_dict = {}
        try:
            outlis = subprocess.check_output(["redis-cli", "--latency"]).split()
            if len(outlis) >= 3:
                try:
                    float(outlis[2])
                    per_dict["latency"] = outlis[2]
                except ValueError:
                    return final_redis_dict
                per_dict[PLUGINTYPE] = "performanceDetails"
                final_redis_dict["performanceDetails"] = per_dict
            else:
                return final_redis_dict
        except Exception as e:
            collectd.error("Unable to replication details due to %s" % str(e))
            return final_redis_dict
        return final_redis_dict

    @staticmethod
    def add_common_params(redis_dict):
        hostname = gethostname()
        timestamp = int(round(time.time()))

        for details_type, details in redis_dict.items():
            details[HOSTNAME] = hostname
            details[TIMESTAMP] = timestamp
            details[PLUGIN] = "redisdb"
            details[ACTUALPLUGINTYPE] = "redisdb"
            details[PLUGINTYPE] = details_type

    def collect_data(self):
        # get data of Redis
        general_details = self.get_redis_server_data()
        server_stats = self.get_server_stats(general_details)
        memory_details = self.get_memory_stats(server_stats)
        final_details = self.get_replication_stats(memory_details)
        #final_details = self.get_performance_details(replication_details)

        if not final_details:
            collectd.error("Plugin Redis: Unable to fetch data information of Redis.")
            return

        # Add common parameters
        self.add_common_params(final_details)
        return final_details

    @staticmethod
    def dispatch_data(dict_disks_copy):
        collectd.debug(json.dumps(dict_disks_copy.keys()))
        for details_type, details in dict_disks_copy.items():
            collectd.debug("Plugin Redis: Values: " + json.dumps(details))
            collectd.info("final details are : %s" % details)
            dispatch(details)

    def read(self):
        try:
            self.pollCounter += 1
            self.connect_redis()
            # collect data
            dict_redis = self.collect_data()
            if not dict_redis:
                collectd.error("Plugin Redis: Unable to fetch data for Redis.")
                return
            else:
                # Deleteing documentsTypes which were not requetsed
                for doc in dict_redis.keys():
                    if dict_redis[doc]['_documentType'] not in self.documentsTypes:
                        collectd.error('you are in')
                        del dict_redis[doc]
            # dispatch data to collectd, copying by value
            #self.dispatch_data(deepcopy(dict_redis))
            self.dispatch_data(dict_redis)
        except Exception as e:
            collectd.debug(traceback.format_exc())
            collectd.error("Couldn't read and gather the SQL metrics due to theexception :%s" % e)
            return

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


def init():
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)

obj = RedisStats()
collectd.register_config(obj.read_config)
collectd.register_read(obj.read_temp)
