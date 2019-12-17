"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
""" A collectd-python plugin for retrieving
    metrics from cassandra Database server.
    """
import collectd
import signal
import time
import json
import requests
import sys
import traceback
from copy import deepcopy

# user imports
from utils import *
from constants import *

#Cassandra metrics
metrics = [
    {
        "metric_name": "Latency",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "ClientRequest",
        "jmx_scope": "Read",
        "display_name": "readCount",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},scope={jmx_scope},name={name}"
    },
    {
        "metric_name": "Latency",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "ClientRequest",
        "jmx_scope": "Write",
        "display_name": "writeCount",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},scope={jmx_scope},name={name}"
    },
    {
        "metric_name": "TotalLatency",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "ClientRequest",
        "jmx_scope": "Read",
        "display_name": "totalReadLatency",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},scope={jmx_scope},name={name}"
    },
    {
        "metric_name": "TotalLatency",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "ClientRequest",
        "jmx_scope": "Write",
        "display_name": "totalWriteLatency",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},scope={jmx_scope},name={name}"
    },
    {
        "metric_name": "Latency",
        "metric_key": "FiveMinuteRate",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "ClientRequest",
        "jmx_scope": "Write",
        "display_name": "writeThroughput",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},scope={jmx_scope},name={name}"
    },
    {
        "metric_name": "Latency",
        "metric_key": "FiveMinuteRate",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "ClientRequest",
        "jmx_scope": "Read",
        "display_name": "readThroughput",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},scope={jmx_scope},name={name}"
    },
    {
        "metric_name": "Load",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "Storage",
        "display_name": "diskSpaceUsedLoad",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},name={name}"
    },
    {
        "metric_name": "TotalDiskSpaceUsed",
        "metric_key": "Value",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "ColumnFamily",
        "display_name": "totalDiskSpaceUsed",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},name={name}"
    },
    {
        "metric_name": "Hits",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "Cache",
        "jmx_scope": "KeyCache",
        "display_name": "cacheHitCount",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},scope={jmx_scope},name={name}"
    },
    {
        "metric_name": "Requests",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "Cache",
        "jmx_scope": "KeyCache",
        "display_name": "cacheRequestCount",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},scope={jmx_scope},name={name}"
    },
    {
        "metric_name": "PendingTasks",
        "metric_key": "Value",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "ThreadPools",
        "jmx_scope": "RequestResponseStage",
        "jmx_path": "request",
        "display_name": "threadPoolPendingTasks",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},path={jmx_path},scope={jmx_scope},name={name}"
    },
    {
        "metric_name": "TotalBlockedTasks",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "ThreadPools",
        "jmx_scope": "RequestResponseStage",
        "jmx_path": "request",
        "display_name": "threadPoolTotalBlockedTasks",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},path={jmx_path},scope={jmx_scope},name={name}"
    },
    {
        "metric_name": "ActiveTasks",
        "metric_key": "Value",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "ThreadPools",
        "jmx_scope": "RequestResponseStage",
        "jmx_path": "request",
        "display_name": "threadPoolActiveTasks",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},path={jmx_path},scope={jmx_scope},name={name}"
    },
    {
        "metric_name": "CompletedTasks",
        "metric_key": "Value",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "Compaction",
        "display_name": "compactionCompletedTasks",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},name={name}"
    },
    {
        "metric_name": "PendingTasks",
        "metric_key": "Value",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "Compaction",
        "display_name": "compactionPendingTasks",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},name={name}"
    },
    {
        "metric_name": "BytesCompacted",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "Compaction",
        "display_name": "totalCompactedSize",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},name={name}"
    },
    {
        "metric_name": "Exceptions",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "Storage",
        "display_name": "exceptions",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},name={name}"
    },
    {
        "metric_name": "Timeouts",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "ClientRequest",
        "jmx_scope": "Read",
        "display_name": "readTimeouts",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},scope={jmx_scope},name={name}"
    },
    {
        "metric_name": "Timeouts",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "ClientRequest",
        "jmx_scope": "Write",
        "display_name": "writeTimeouts",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},scope={jmx_scope},name={name}"
    },
    {
        "metric_name": "Unavailables",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "ClientRequest",
        "jmx_scope": "Read",
        "display_name": "readUnavailables",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},scope={jmx_scope},name={name}"
    },
    {
        "metric_name": "Unavailables",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "ClientRequest",
        "jmx_scope": "Write",
        "display_name": "writeUnavailables",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},scope={jmx_scope},name={name}"
    },
    ]

# JVM metrics
jvm_metrics =[
    {
        "metric_name": "ParNew",
        "metric_key": "CollectionCount",
        "jmx_domain": "java.lang",
        "jmx_type": "GarbageCollector",
        "display_name": "gCParNewCollectionCount",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},name={name}"
    },
    {
        "metric_name": "ParNew",
        "metric_key": "CollectionTime",
        "jmx_domain": "java.lang",
        "jmx_type": "GarbageCollector",
        "display_name": "gCParNewCollectionTime",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},name={name}"
    },
    {
        "metric_name": "ConcurrentMarkSweep",
        "metric_key": "CollectionCount",
        "jmx_domain": "java.lang",
        "jmx_type": "GarbageCollector",
        "display_name": "gCMarkSweepCollectionCount",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},name={name}"
    },
    {
        "metric_name": "ConcurrentMarkSweep",
        "metric_key": "CollectionTime",
        "jmx_domain": "java.lang",
        "jmx_type": "GarbageCollector",
        "display_name": "gCMarkSweepCollectionTime",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},name={name}"
    },
    {
        "metric_name": "",
        "metric_key": "LoadedClassCount",
        "jmx_domain": "java.lang",
        "jmx_type": "ClassLoading",
        "display_name": "loadedClassCount",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type}"
    },
    {
        "metric_name": "",
        "metric_key": "UnloadedClassCount",
        "jmx_domain": "java.lang",
        "jmx_type": "ClassLoading",
        "display_name": "unLoadedClassCount",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type}"
    },
    {
        "metric_name": "",
        "metric_key": "HeapMemoryUsage",
        "jmx_domain": "java.lang",
        "jmx_type": "Memory",
        "display_name": "heapMemoryUsage",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type}"
    },
    {
        "metric_name": "",
        "metric_key": "LiveNodes",
        "jmx_domain": "org.apache.cassandra.db",
        "jmx_type": "StorageService",
        "display_name": "liveNodes",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type}"
    },
    {
        "metric_name": "",
        "metric_key": "NonSystemKeyspaces",
        "jmx_domain": "org.apache.cassandra.db",
        "jmx_type": "StorageService",
        "display_name": "userKeyspaces",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type}"
    },
    {
        "metric_name": "",
        "metric_key": "OperationMode",
        "jmx_domain": "org.apache.cassandra.db",
        "jmx_type": "StorageService",
        "display_name": "operationMode",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type}"
    },
    {
        "metric_name": "",
        "metric_key": "Joined",
        "jmx_domain": "org.apache.cassandra.db",
        "jmx_type": "StorageService",
        "display_name": "joined",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type}"
    },
    {
        "metric_name": "",
        "metric_key": "ClusterName",
        "jmx_domain": "org.apache.cassandra.db",
        "jmx_type": "StorageService",
        "display_name": "clusterName",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type}"
    }
]

#Keyspace metrics
ks_metrics = [
    {
        "metric_name": "LiveDiskSpaceUsed",
        "metric_key": "Value",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "Keyspace",
        "display_name": "diskSpaceUsed",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},keyspace={jmx_keyspace},name={name}"
    },
    {
        "metric_name": "ReadLatency",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "Keyspace",
        "display_name": "readLatencyCount",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},keyspace={jmx_keyspace},name={name}"
    },
    {
        "metric_name": "WriteLatency",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "Keyspace",
        "display_name": "writeLatencyCount",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},keyspace={jmx_keyspace},name={name}"
    },
    {
        "metric_name": "ReadTotalLatency",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "Keyspace",
        "display_name": "readTotalLatency",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},keyspace={jmx_keyspace},name={name}"
    },
    {
        "metric_name": "WriteTotalLatency",
        "metric_key": "Count",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "Keyspace",
        "display_name": "writeTotalLatency",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},keyspace={jmx_keyspace},name={name}"
    },
    {
        "metric_name": "ReadLatency",
        "metric_key": "FiveMinuteRate",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "Keyspace",
        "display_name": "readThroughput",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},keyspace={jmx_keyspace},name={name}"
    },
    {
        "metric_name": "WriteLatency",
        "metric_key": "FiveMinuteRate",
        "jmx_domain": "org.apache.cassandra.metrics",
        "jmx_type": "Keyspace",
        "display_name": "writeThroughput",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},keyspace={jmx_keyspace},name={name}"
    },
    {
        "metric_name": "",
        "metric_key": "TableName",
        "jmx_domain": "org.apache.cassandra.db",
        "jmx_type": "Tables",
        "display_name": "tablesList",
        "format_str": "http://{host}:{port}/jolokia/read/{jmx_domain}:type={jmx_type},keyspace={jmx_keyspace},table=*"
    }
]

exclude_ks = ["system_auth", "system_distributed", "system_schema", "system_traces"]

class CassandraStats(object):
    """Plugin object will be created only once and collects cassandra statistics info every interval."""

    def __init__(self):
        """Initializes interval and previous dictionary variable."""
        self.interval = DEFAULT_INTERVAL
        self.pollCounter = 0
        self.user = None
        self.password = None
        self.port = 8778
        self.keyspaces = []
        self.previousData = {}

    def config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == HOST:
                self.host = children.values[0]
                self.password = children.values[0]

    @staticmethod
    def add_common_params(doc, cassandra_dict):
        hostname = gethostname()
        timestamp = int(round(time.time()* 1000))
        cassandra_dict[HOSTNAME] = hostname
        cassandra_dict[TIMESTAMP] = timestamp
        cassandra_dict[PLUGIN] = CASSANDRA
        cassandra_dict[ACTUALPLUGINTYPE] = CASSANDRA
        cassandra_dict[PLUGINTYPE] = doc

    def fetch_and_update(self,
            name,
            keyname,
            jmx_domain,
            jmx_type,
            jmx_scope,
            jmx_path,
            format_str,
            jmx_keyspace=""
    ):
        """
        Fetch a metric from jolokia jmx agent.
        The jolokia service has a get request made to it to read a metric that
        is passed in as name and keyname from the server.
        :param name: Name of the metric
        :param keyname: Keyname of the metric
        :param jmx_domain: Domain of the metric
        :param jmx_type: Type of the metric
        :param jmx_scope: Scope of the metric
        :param jmx_path: Specified path of the metric
        :param jmx_keyspace: keyspace name of cassandra
        :param format_str: GET URL formed with the help of above params
        :return: Metric value
        """

        auth = requests.auth.HTTPBasicAuth(self.user, self.password) \
            if self.user is not None and self.password is not None else None

        try:
            resp = requests.get(
                format_str.format(jmx_domain=jmx_domain, jmx_type=jmx_type, jmx_scope=jmx_scope, jmx_path=jmx_path,
                                  jmx_keyspace=jmx_keyspace, host=self.host, name=name, port=self.port),
                auth=auth
            )
        except requests.exceptions.ConnectionError:
            collectd.error(
                "The application recieved a connection error, failed to connect jolokia JVM agent"
            )
            sys.exit(1)
        if resp.status_code >= 400 or resp.json().get("error"):
            collectd.error("ERROR the jolokia agent returned an error trying to access the following metric")
            collectd.error(format_str.format(jmx_domain=jmx_domain, jmx_type=jmx_type, jmx_scope=jmx_scope, jmx_path=jmx_path,
                                    jmx_keyspace=jmx_keyspace, name=name, key=keyname, host=self.host, port=self.port))
            return
        else:
            jmx_data = resp.json()["value"]
            if (keyname == "TableName"):
                metric = jmx_data.values()[0][keyname]
            else:
                metric = jmx_data[keyname]
            return (metric)

    def get_cassandra_details(self):
        """
        Get Cassandra stats from jolokia agent
        :return: dict of cassandra stats
        """
        cassandra_results = {}
        cassandraMetrics = {}
        try:
            for metric in metrics:
                value = self.fetch_and_update(
                    name=metric["metric_name"],
                    keyname=metric["metric_key"],
                    jmx_domain=metric["jmx_domain"],
                    jmx_type=metric["jmx_type"],
                    jmx_scope=metric.get("jmx_scope"),
                    jmx_path=metric.get("jmx_path"),
                    format_str=metric["format_str"]
                )
                if value:
                    if metric["display_name"] in ["totalDiskSpaceUsed", "diskSpaceUsedLoad", "totalCompactedSize"]:
                        cassandra_results[metric["display_name"]] = round(value / (1024.0 * 1024.0), 2)
                    elif metric["display_name"] in ["writeThroughput", "readThroughput"]:
                        cassandra_results[metric["display_name"]] = round(value, 2)
                    else:
                        cassandra_results[metric["display_name"]] = value
                else:
                    cassandra_results[metric["display_name"]] = 0
            if not cassandra_results:
                collectd.error("Unable to collect Cassandra metrics")
                return cassandraMetrics
            else:
                if self.pollCounter <= 1:
                    cassandraMetrics["cassandraStats"] = {}
                    cassandraMetrics["cassandraStats"] = deepcopy(cassandra_results)
                    cassandraMetrics["cassandraStats"]["cacheHitCount"] = 0
                    cassandraMetrics["cassandraStats"]["cacheRequestCount"] = 0
                    cassandraMetrics["cassandraStats"]["compactionCompletedTasks"] = 0
                    cassandraMetrics["cassandraStats"]["readLatency"] = 0
                    cassandraMetrics["cassandraStats"]["writeLatency"] = 0
                    cassandraMetrics["cassandraStats"]["cacheMissCount"] = 0
                    cassandraMetrics["cassandraStats"]["cacheHitRatio"] = 0.0
                    cassandraMetrics["cassandraStats"]["cacheMissRatio"] = 0.0
                    cassandraMetrics["cassandraStats"]["exceptions"] = 0
                    cassandraMetrics["cassandraStats"]["readTimeouts"] = 0
                    cassandraMetrics["cassandraStats"]["writeTimeouts"] = 0
                    cassandraMetrics["cassandraStats"]["readUnavailables"] = 0
                    cassandraMetrics["cassandraStats"]["writeUnavailables"] = 0
                    self.previousData["cassandraStats"] = deepcopy(cassandra_results)
                else:
                    cassandraMetrics["cassandraStats"] = deepcopy(cassandra_results)
                    cassandraStats = cassandraMetrics["cassandraStats"]
                    previousStats = self.previousData["cassandraStats"]

                    cassandraStats["cacheHitCount"] = cassandra_results["cacheHitCount"] - previousStats["cacheHitCount"]
                    cassandraStats["cacheRequestCount"] = cassandra_results["cacheRequestCount"] - previousStats["cacheRequestCount"]
                    cassandraStats["compactionCompletedTasks"] = cassandra_results["compactionCompletedTasks"] - previousStats["compactionCompletedTasks"]
                    cassandraStats["exceptions"] = cassandra_results["exceptions"] - previousStats["exceptions"]
                    cassandraStats["readTimeouts"] = cassandra_results["readTimeouts"] - previousStats["readTimeouts"]
                    cassandraStats["writeTimeouts"] = cassandra_results["writeTimeouts"] - previousStats["writeTimeouts"]
                    cassandraStats["readUnavailables"] = cassandra_results["readUnavailables"] - previousStats["readUnavailables"]
                    cassandraStats["writeUnavailables"] = cassandra_results["writeUnavailables"] - previousStats["writeUnavailables"]
                    cassandraStats["cacheMissCount"] = cassandraStats["cacheRequestCount"] - cassandraStats["cacheHitCount"]

                    totalReadLatency = cassandra_results["totalReadLatency"] - previousStats["totalReadLatency"]
                    totalWriteLatency = cassandra_results["totalWriteLatency"] - previousStats["totalWriteLatency"]
                    readCount = cassandra_results["readCount"] - previousStats["readCount"]
                    writeCount = cassandra_results["writeCount"] - previousStats["writeCount"]

                    if cassandraStats["cacheHitCount"] and cassandraStats["cacheRequestCount"]:
                        cassandraStats["cacheHitRatio"] = round(float(cassandraStats["cacheHitCount"]) / float(cassandraStats["cacheRequestCount"]), 2)
                    else:
                        cassandraStats["cacheHitRatio"] = 0.0

                    if cassandraStats["cacheMissCount"] and cassandraStats["cacheRequestCount"]:
                        cassandraStats["cacheMissRatio"] = round(float(cassandraStats["cacheMissCount"]) / float(cassandraStats["cacheRequestCount"]), 2)
                    else:
                        cassandraStats["cacheMissRatio"] = 0.0

                    if totalReadLatency and readCount:
                        cassandraStats["readLatency"] = round(float(totalReadLatency) / float(readCount), 2)
                    else:
                        cassandraStats["readLatency"] = 0.0

                    if totalWriteLatency and writeCount:
                        cassandraStats["writeLatency"] = round(float(totalWriteLatency) / float(writeCount), 2)
                    else:
                        cassandraStats["writeLatency"] = 0.0

                    self.previousData["cassandraStats"] = cassandra_results

            # Removing readCount, writeCount, totalReadLatency, totalReadLatency from cassandraMetrics dict.
            # As it needed only for internal calculation of  read/write Latency
            for key in ["readCount", "writeCount", "totalReadLatency", "totalWriteLatency"]:
                if key in cassandraMetrics["cassandraStats"].keys():
                    del cassandraMetrics["cassandraStats"][key]
        except Exception as e:
            collectd.error("Error in collecting cassandraStats due to %s " % str(e))

        return cassandraMetrics

    def get_jvm_details(self, cassandraMetrics):
        """
        Get JVM stats from jolokia agent
        :param cassandraMetrics: collected cassandra stats dict
        :return: cassandraMetrics dict contains both cassandra stats and jvm stats
        """
        jvm_results = {}
        try:
            for metric in jvm_metrics:
                value = self.fetch_and_update(
                    name=metric["metric_name"],
                    keyname=metric["metric_key"],
                    jmx_domain=metric["jmx_domain"],
                    jmx_type=metric["jmx_type"],
                    jmx_scope=metric.get("jmx_scope"),
                    jmx_path=metric.get("jmx_path"),
                    format_str=metric["format_str"]
                )
                if value:
                    if metric["metric_key"] == "NonSystemKeyspaces":
                        for ks in exclude_ks:
                            if ks in value:
                                value.remove(ks)
                        self.keyspaces = [x.encode() for x in value]
                    elif metric["metric_key"] == "HeapMemoryUsage":
                        heapMemoryUsageInit = value.get("init", 0)
                        if heapMemoryUsageInit:
                            jvm_results["heapMemoryUsageInit"] = round(heapMemoryUsageInit / (1024.0 * 1024.0), 2)
                        heapMemoryUsageCommitted = value.get("committed", 0)
                        if heapMemoryUsageCommitted:
                            jvm_results["heapMemoryUsageCommitted"] = round(heapMemoryUsageCommitted / (1024.0 * 1024.0), 2)
                        heapMemoryUsageMax = value.get("max", 0)
                        if heapMemoryUsageMax:
                            jvm_results["heapMemoryUsageMax"] = round(heapMemoryUsageMax / (1024.0 * 1024.0), 2)
                        heapMemoryUsageUsed = value.get("used", 0)
                        if heapMemoryUsageUsed:
                            jvm_results["heapMemoryUsageUsed"] = round(heapMemoryUsageUsed / (1024.0 * 1024.0), 2)
                    elif metric["metric_key"] == "LiveNodes":
                        liveNodes = [x.encode() for x in value]
                        #jvm_results[metric["display_name"]] = liveNodes
                    elif metric["metric_key"] in ["ClusterName", "OperationMode"]:
                        jvm_results[metric["display_name"]] = value.encode()
                    else:
                        jvm_results[metric["display_name"]] = value
                else:
                    if metric["metric_key"] == "HeapMemoryUsage":
                        jvm_results["heapMemoryUsageInit"] = 0
                        jvm_results["heapMemoryUsageCommitted"] = 0
                        jvm_results["heapMemoryUsageMax"] = 0
                        jvm_results["heapMemoryUsageUsed"] = 0
                    else:
                        jvm_results[metric["display_name"]] = 0

            if not jvm_results:
                collectd.error("Unable to collect jmxStats")
            else:
                if self.pollCounter <=1:
                    cassandraMetrics["jvmStats"] = deepcopy(jvm_results)
                    cassandraMetrics["jvmStats"]["unLoadedClassCount"] = 0
                    cassandraMetrics["jvmStats"]["loadedClassCount"] = 0
                    self.previousData["jvmStats"] = {}
                    self.previousData["jvmStats"]["unLoadedClassCount"] = jvm_results["unLoadedClassCount"]
                    self.previousData["jvmStats"]["loadedClassCount"] = jvm_results["loadedClassCount"]
                else:
                    cassandraMetrics["jvmStats"] = deepcopy(jvm_results)
                    jvmStats = cassandraMetrics["jvmStats"]
                    previousStats = self.previousData["jvmStats"]
                    jvmStats["unLoadedClassCount"] = jvm_results["unLoadedClassCount"] - previousStats["unLoadedClassCount"]
                    jvmStats["loadedClassCount"] = jvm_results["loadedClassCount"] - previousStats["loadedClassCount"]
                    self.previousData["jvmStats"]["unLoadedClassCount"] = jvm_results["unLoadedClassCount"]
                    self.previousData["jvmStats"]["loadedClassCount"] = jvm_results["loadedClassCount"]

        except Exception as e:
            collectd.error("Error in collecting jmxStats due to %s " % str(e))
        return cassandraMetrics

    def get_keyspace_details(self, cassandraMetrics):
        """
        Get keyspace stats from jolokia agent
        :param cassandraMetrics: collected cassandra and jvm stats dict
        :return: CassandraMetrics dict contains cassandra stats, jvm stats and one or more keyspace stats
        """
        ks_results = {}
        if self.keyspaces:
            for keyspace in self.keyspaces:
                try:
                    for metric in ks_metrics:
                        value = self.fetch_and_update(
                            name=metric["metric_name"],
                            keyname=metric["metric_key"],
                            jmx_domain=metric["jmx_domain"],
                            jmx_type=metric["jmx_type"],
                            jmx_scope=metric.get("jmx_scope"),
                            jmx_path=metric.get("jmx_path"),
                            format_str=metric["format_str"],
                            jmx_keyspace=keyspace
                        )
                        if value:
                            if metric["display_name"] == "diskSpaceUsed":
                                ks_results[metric["display_name"]] = round(value / (1024.0 * 1024.0), 2)
                            elif metric["display_name"] in ["writeThroughput", "readThroughput"]:
                                ks_results[metric["display_name"]] = round(value, 2)
                            else:
                                ks_results[metric["display_name"]] = value
                        else:
                            if metric["metric_key"] != "TableName":
                                ks_results[metric["display_name"]] = 0
                    if ks_results:
                        ks_results["_keyspaceName"] = keyspace
                        cassandraMetrics[keyspace] = {}
                        if self.pollCounter <= 1:
                            cassandraMetrics[keyspace].update(deepcopy(ks_results))
                            cassandraMetrics[keyspace]["readLatency"] = 0.0
                            cassandraMetrics[keyspace]["writeLatency"] = 0.0
                            self.previousData[keyspace] = {}
                            self.previousData[keyspace].update(deepcopy(ks_results))
                        else:
                            cassandraMetrics[keyspace].update(deepcopy(ks_results))
                            keyspaceStats = cassandraMetrics[keyspace]
                            previousStats = self.previousData[keyspace]

                            readLatencyCount = ks_results["readLatencyCount"] - previousStats["readLatencyCount"]
                            writeLatencyCount = ks_results["writeLatencyCount"] - previousStats["writeLatencyCount"]
                            readTotalLatency = ks_results["readTotalLatency"] - previousStats["readTotalLatency"]
                            writeTotalLatency = ks_results["writeTotalLatency"] - previousStats["writeTotalLatency"]

                            if readTotalLatency and readLatencyCount:
                                keyspaceStats["readLatency"] = round(float(readTotalLatency) / float(readLatencyCount), 2)
                            else:
                                keyspaceStats["readLatency"] = 0.0

                            if writeTotalLatency and writeLatencyCount:
                                keyspaceStats["writeLatency"] = round(float(writeTotalLatency) / float(writeLatencyCount), 2)
                            else:
                                keyspaceStats["writeLatency"] = 0.0

                            self.previousData[keyspace].update(ks_results)
                    else:
                        collectd.error("Unable to collectd metrics for the keyspace %s" % keyspace)

                    # Removing readLatencyCount, writeLatencyCount, readTotalLatency, writeTotalLatency from cassandraMetrics dict.
                    # As it needed only for internal calculation of  read/write Latency
                    for key in ["readLatencyCount", "writeLatencyCount", "readTotalLatency", "writeTotalLatency"]:
                        if key in cassandraMetrics[keyspace].keys():
                            del cassandraMetrics[keyspace][key]
                except Exception as e:
                    collectd.error("Error in collecting KeyspaceStats due to %s in the keyspace %s" % (str(e), keyspace))
                    continue
        else:
            collectd.info("No keyspaces found!!!")
        return cassandraMetrics

    def collect_data(self):
        """
        Collect cassandrStats, jvmStats and keyspaceStats using jolokia JVM agent
        :return: Dict contains cassandra stats, jvm stats and keyspace stats
        """
        cassandra_details = self.get_cassandra_details()
        jvm_details = self.get_jvm_details(cassandra_details)
        final_cassandra_details = self.get_keyspace_details(jvm_details)

        #Adding common parameters to the collected stats
        if final_cassandra_details:
            for key, value in final_cassandra_details.items():
                if key not in [CASSANDRA_STATS, JVMSTATS]:
                    self.add_common_params(KEYSPACE_STATS, value)
                else:
                    self.add_common_params(key, value)
        else:
            collectd.error("Error: cassandra metrics not found")

        return final_cassandra_details

    @staticmethod
    def dispatch_data(dict_disks_copy):
        for details_type, details in dict_disks_copy.items():
            collectd.debug("Plugin cassandra: Values: " + json.dumps(details))
            collectd.info("final details are : %s" % details)
            dispatch(details)

    def read(self):
        try:
            self.pollCounter += 1
            # collect data
            dict_cassandra = self.collect_data()
            if not dict_cassandra:
                collectd.error("Plugin CASSANDRA: Unable to fetch data for CASSANDRA.")
                return

            # dispatch data to collectd, copying by value
            self.dispatch_data(deepcopy(dict_cassandra))
        except Exception as e:
            collectd.error("Couldn't read and gather the cassandra metrics due to the exception :%s due to %s" % (e, traceback.format_exc()))
            return

    def read_temp(self):
        """Collectd first calls register_read. At that time default interval is taken,
        hence temporary function is made to call, the read callback is unregistered
        and read() is called again with interval obtained from conf by register_config callback."""
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))

def init():
    """When new process is formed, action to SIGCHLD is reset to default behavior."""
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)

OBJ = CassandraStats()
collectd.register_init(init)
collectd.register_config(OBJ.config)
collectd.register_read(OBJ.read_temp)
