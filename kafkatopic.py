"""Python plugin for collectd to fetch kafkaStats and topicStats for kafka_topic process"""

#!/usr/bin/python
import os
import signal
import json
import time
import collectd
import Queue
import multiprocessing
import kafka
from copy import deepcopy
from pyjolokia import Jolokia
# user imports
import utils
from constants import *
from libjolokia import JolokiaClient

KAFKA_DOCS = ["kafkaStats", "topicStats", "partitionStats"]
BROKER_STATES = {0: "NotRunning", 1: "Starting", 2: "RecoveringFromUncleanShutdown", 3: "RunningAsBroker", \
                 6: "PendingControlledShutdown", 7: "BrokerShuttingDown"}

class JmxStat(object):
    """Plugin object will be created only once and collects JMX statistics info every interval."""

    def __init__(self):
        """Initializes interval and previous dictionary variable."""
        self.interval = DEFAULT_INTERVAL
        self.process = 'kafka.Kafka'
        self.listenerip = 'localhost'
        self.prev_topic_data = {}
        self.prev_data = {}
        self.port = None
        self.documentsTypes = []
        self.jclient = JolokiaClient(os.path.basename(__file__)[:-3], self.process)

    def config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == LISTENERIP:
                self.listenerip = children.values[0]
            if children.key == PORT:
                self.port = children.values[0]
            if children.key == DOCUMENTSTYPES:
                self.documentsTypes = children.values[0]

    def get_jmx_parameters(self, jolokiaclient, doc, dict_jmx):
        """Fetch stats based on doc_type"""
        if doc == "kafkaStats":
            self.add_kafka_parameters(jolokiaclient, dict_jmx)
        elif doc == "topicStats":
            self.add_topic_parameters(jolokiaclient, dict_jmx, "topic")
        elif doc == "partitionStats":
            self.add_topic_parameters(jolokiaclient, dict_jmx, "partition")

    def add_default_rate_value(self, dict_jmx, doctype):
        """Add default value to rate key based on type"""
        if doctype == "kafka":
            keylist = ["messagesIn", "bytesIn", "bytesOut", "isrExpands", "isrShrinks", "leaderElection", \
                       "uncleanLeaderElections", "producerRequests", "fetchConsumerRequests", "fetchFollowerRequests"]
        else:
            keylist = ["messagesInRate", "bytesOutRate", "bytesInRate", "totalFetchRequestsRate", "totalProduceRequestsRate", \
                       "produceMessageConversionsRate", "failedProduceRequestsRate", "fetchMessageConversionsRate", "failedFetchRequestsRate",\
                       "bytesRejectedRate"]
        for key in keylist:
            dict_jmx[key] = 0

    def get_rate(self, key, curr_data, prev_data):
        """Calculate and returns rate. Rate=(current_value-prev_value)/time."""
        #TODO The code is similar to the one in utils.py.
        rate = NAN
        if not prev_data:
            return rate

        if key not in prev_data:
            collectd.error("%s key not in previous data. Shouldn't happen." % key)
            return rate

        if TIMESTAMP not in curr_data or TIMESTAMP not in prev_data:
            collectd.error("%s key not in previous data. Shouldn't happen." % key)
            return rate

        curr_time = curr_data[TIMESTAMP]
        prev_time = prev_data[TIMESTAMP]

        if curr_time <= prev_time:
            collectd.error("Current data time: %s is less than previous data time: %s. "
                           "Shouldn't happen." % (curr_time, prev_time))
            return rate

        rate = (curr_data[key] - prev_data[key]) / float(self.interval)
        # rate can get negative if the topic(s) are deleted and created again with the same name
        # intializing rate to 0 if rates are negative value.
        if rate < 0:
            rate = 0.0
        return rate

    def add_rate(self, pid, dict_jmx):
        """Rate only for kafka jmx metrics"""
        rate = self.get_rate("messagesInPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["messagesIn"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("bytesInPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["bytesIn"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("bytesOutPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["bytesOut"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("isrExpandsPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["isrExpands"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("isrShrinksPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["isrShrinks"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("leaderElectionPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["leaderElection"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("uncleanLeaderElectionPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["uncleanLeaderElections"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("producerRequestsPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["producerRequests"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("fetchConsumerRequestsPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["fetchConsumerRequests"] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate("fetchFollowerRequestsPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["fetchFollowerRequests"] = round(rate, FLOATING_FACTOR)

    def add_topic_rate(self, pid, topic_name, topic_info):
        """Rate only for kafka topic metrics"""
        rate = self.get_rate('messagesIn', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['messagesInRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('bytesOut', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['bytesOutRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('bytesIn', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['bytesInRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('totalFetchRequests', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['totalFetchRequestsRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('totalProduceRequests', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['totalProduceRequestsRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('produceMessageConversions', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['produceMessageConversionsRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('failedProduceRequests', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['failedProduceRequestsRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('fetchMessageConversions', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['fetchMessageConversionsRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('failedFetchRequests', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['failedFetchRequestsRate'] = round(rate, FLOATING_FACTOR)
        rate = self.get_rate('bytesRejected', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['bytesRejectedRate'] = round(rate, FLOATING_FACTOR)

    def add_kafka_parameters(self, jolokiaclient, dict_jmx):
        """Add jmx stats specific to kafka metrics"""
        jolokiaclient.add_request(type='read', mbean='kafka.server:name=UnderReplicatedPartitions,type=ReplicaManager', attribute='Value')
        jolokiaclient.add_request(type='read', mbean='kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics', attribute='Count')
        jolokiaclient.add_request(type='read', mbean='kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics', attribute='Count')
        jolokiaclient.add_request(type='read', mbean='kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics', attribute='Count')
        jolokiaclient.add_request(type='read', mbean='kafka.server:name=PartitionCount,type=ReplicaManager', attribute='Value')
        jolokiaclient.add_request(type='read', mbean='kafka.server:name=IsrExpandsPerSec,type=ReplicaManager', attribute='Count')
        jolokiaclient.add_request(type='read', mbean='kafka.server:name=IsrShrinksPerSec,type=ReplicaManager', attribute='Count')
        jolokiaclient.add_request(type='read', mbean='kafka.server:name=RequestHandlerAvgIdlePercent,type=KafkaRequestHandlerPool', attribute='MeanRate')
        jolokiaclient.add_request(type='read', mbean='kafka.controller:name=OfflinePartitionsCount,type=KafkaController', attribute='Value')
        jolokiaclient.add_request(type='read', mbean='kafka.controller:name=ActiveControllerCount,type=KafkaController', attribute='Value')
        jolokiaclient.add_request(type='read', mbean='kafka.controller:name=LeaderElectionRateAndTimeMs,type=ControllerStats', attribute='Count')
        jolokiaclient.add_request(type='read', mbean='kafka.controller:name=UncleanLeaderElectionsPerSec,type=ControllerStats', attribute='Count')
        jolokiaclient.add_request(type='read', mbean='kafka.network:name=RequestsPerSec,request=Produce,type=RequestMetrics', attribute='Count')
        jolokiaclient.add_request(type='read', mbean='kafka.network:name=RequestsPerSec,request=FetchConsumer,type=RequestMetrics', attribute='Count')
        jolokiaclient.add_request(type='read', mbean='kafka.network:name=RequestsPerSec,request=FetchFollower,type=RequestMetrics', attribute='Count')
        jolokiaclient.add_request(type='read', mbean='kafka.network:name=NetworkProcessorAvgIdlePercent,type=SocketServer', attribute='Value')
        jolokiaclient.add_request(type='read', mbean='kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchFollower', attribute='Mean')
        jolokiaclient.add_request(type='read', mbean='kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer', attribute='Mean')
        jolokiaclient.add_request(type='read', mbean='kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce', attribute='Mean')
        jolokiaclient.add_request(type='read', mbean='kafka.network:name=ResponseSendTimeMs,request=FetchFollower,type=RequestMetrics', attribute='Mean')
        jolokiaclient.add_request(type='read', mbean='kafka.network:name=ResponseSendTimeMs,request=FetchConsumer,type=RequestMetrics', attribute='Mean')
        jolokiaclient.add_request(type='read', mbean='kafka.network:name=ResponseSendTimeMs,request=Produce,type=RequestMetrics', attribute='Mean')
        jolokiaclient.add_request(type='read', mbean='kafka.server:name=BrokerState,type=KafkaServer', attribute='Value')
        bulkdata = jolokiaclient.getRequests()
        dict_jmx['underReplicatedPartitions'] = bulkdata[0].get('value', 0)
        dict_jmx['messagesInPerSec'] = bulkdata[1].get('value', 0)
        dict_jmx['bytesInPerSec'] = bulkdata[2].get('value', 0)
        dict_jmx['bytesOutPerSec'] = bulkdata[3].get('value', 0)
        dict_jmx['partitionCount'] = bulkdata[4].get('value', 0)
        dict_jmx['isrExpandsPerSec'] = bulkdata[5].get('value', 0)
        dict_jmx['isrShrinksPerSec'] = bulkdata[6].get('value', 0)
        dict_jmx['requestHandlerAvgIdle'] = float(str(bulkdata[7].get('value', 0))[:4])
        dict_jmx['offlinePartitions'] = bulkdata[8].get('value', 0)
        dict_jmx['activeController'] = bulkdata[9].get('value', 0)
        dict_jmx['leaderElectionPerSec'] = bulkdata[10].get('value', 0)
        dict_jmx['uncleanLeaderElectionPerSec'] = bulkdata[11].get('value', 0)
        dict_jmx['producerRequestsPerSec'] = bulkdata[12].get('value', 0)
        dict_jmx['fetchConsumerRequestsPerSec'] = bulkdata[13].get('value', 0)
        dict_jmx['fetchFollowerRequestsPerSec'] = bulkdata[14].get('value', 0)
        dict_jmx['networkProcessorAvgIdlePercent'] = float(str(bulkdata[15].get('value', 0))[:4])
        dict_jmx['followerRequestTime'] = round(bulkdata[16].get('value', 0), 2)
        dict_jmx['consumerRequestTime'] = round(bulkdata[17].get('value', 0), 2)
        dict_jmx['producerRequestTime'] = round(bulkdata[18].get('value', 0), 2)
        dict_jmx['followerResponseTime'] = round(bulkdata[19].get('value', 0), 2)
        dict_jmx['consumerResponseTime'] = round(bulkdata[20].get('value', 0), 2)
        dict_jmx['producerResponseTime'] = round(bulkdata[21].get('value', 0), 2)
        if bulkdata[22]['status'] == 200:
            dict_jmx['brokerState'] = BROKER_STATES[bulkdata[22]['value']]
        else:
            dict_jmx['brokerState'] = "NotAvailable"

    def add_topic_parameters(self, jolokiaclient, dict_jmx, flag="topic"):
        """JMX stats specific to topic and partition"""
        def get_partitions(topic):
            client = kafka.KafkaClient(self.listenerip+':'+self.port)
            topic_partition_ids = client.get_partition_ids_for_topic(topic)
            return len(topic_partition_ids)

        def get_topics():
            consumer = kafka.KafkaConsumer(bootstrap_servers=self.listenerip+':'+self.port)
            return consumer.topics()

        parti_list = []
        topics = get_topics()
        for topic in topics:
            partitions = get_partitions(topic)
            if flag == "topic":
                dict_topic = {}
                msgIn = jolokiaclient.request(type='read', mbean='kafka.server:name=MessagesInPerSec,topic=%s,type=BrokerTopicMetrics'%topic, attribute='Count')
                bytesOut = jolokiaclient.request(type='read', mbean='kafka.server:name=BytesOutPerSec,topic=%s,type=BrokerTopicMetrics'%topic, attribute='Count')
                bytesIn = jolokiaclient.request(type='read', mbean='kafka.server:name=BytesInPerSec,topic=%s,type=BrokerTopicMetrics'%topic, attribute='Count')
                totalFetch = jolokiaclient.request(type='read', mbean='kafka.server:name=TotalFetchRequestsPerSec,topic=%s,type=BrokerTopicMetrics'%topic, attribute='Count')
                totalProduce = jolokiaclient.request(type='read', mbean='kafka.server:name=TotalProduceRequestsPerSec,topic=%s,type=BrokerTopicMetrics'%topic, attribute='Count')
                produceMsg = jolokiaclient.request(type='read', mbean='kafka.server:name=ProduceMessageConversionsPerSec,topic=%s,type=BrokerTopicMetrics'%topic, attribute='Count')
                failedProduce = jolokiaclient.request(type='read', mbean='kafka.server:name=FailedProduceRequestsPerSec,topic=%s,type=BrokerTopicMetrics'%topic, attribute='Count')
                fetchMsg = jolokiaclient.request(type='read', mbean='kafka.server:name=FetchMessageConversionsPerSec,topic=%s,type=BrokerTopicMetrics'%topic, attribute='Count')
                failedFetch = jolokiaclient.request(type='read', mbean='kafka.server:name=FailedFetchRequestsPerSec,topic=%s,type=BrokerTopicMetrics'%topic, attribute='Count')
                bytesReject = jolokiaclient.request(type='read', mbean='kafka.server:name=BytesRejectedPerSec,topic=%s,type=BrokerTopicMetrics'%topic, attribute='Count')
                if msgIn['status'] == 200:
                    dict_topic['messagesIn'] = msgIn['value']
                if bytesOut['status'] == 200:
                    dict_topic['bytesOut'] = bytesOut['value']
                if bytesIn['status'] == 200:
                    dict_topic['bytesIn'] = bytesIn['value']
                if totalFetch['status'] == 200:
                    dict_topic['totalFetchRequests'] = totalFetch['value']
                if totalProduce['status'] == 200:
                    dict_topic['totalProduceRequests'] = totalProduce['value']
                if produceMsg['status'] == 200:
                    dict_topic['produceMessageConversions'] = produceMsg['value']
                if failedProduce['status'] == 200:
                    dict_topic['failedProduceRequests'] = failedProduce['value']
                if fetchMsg['status'] == 200:
                    dict_topic['fetchMessageConversions'] = fetchMsg['value']
                if failedFetch['status'] == 200:
                    dict_topic['failedFetchRequests'] = failedFetch['value']
                if bytesReject['status'] == 200:
                    dict_topic['bytesRejected'] = bytesReject['value']
                if dict_topic:
                    dict_topic['_topicName'] = topic
                    dict_topic['partitionCount'] = partitions
                    dict_jmx[topic] = {}
                    dict_jmx[topic].update(dict_topic)
            else:
                for partition in range(0, partitions):
                    dict_parti = {}
                    numLogSegments = jolokiaclient.request(type='read', mbean='kafka.log:name=NumLogSegments,partition=%s,topic=%s,type=Log'%(partition, topic), attribute='Value')
                    logSize = jolokiaclient.request(type='read', mbean='kafka.log:name=Size,partition=%s,topic=%s,type=Log'%(partition, topic), attribute='Value')
                    if numLogSegments['status'] == 200:
                        dict_parti['_topicName'] = topic
                        dict_parti['_partitionNum'] = partition
                        dict_parti['partitionLogSegments'] = numLogSegments['value']
                        try:
                            dict_parti['partitionLogSize'] = round(logSize['value'] / 1024.0/1024.0, 2)
                        except:
                            dict_parti['partitionLogSize'] = 0
                    if dict_parti:
                        parti_list.append(dict_parti)

        if flag == "partition":
            dict_jmx["partitionStats"] = parti_list

    def add_common_params(self, doc, dict_jmx):
        """Adds TIMESTAMP, PLUGIN, PLUGITYPE to dictionary."""
        timestamp = int(round(time.time()))
        dict_jmx[TIMESTAMP] = timestamp
        dict_jmx[PLUGIN] = KAFKA_TOPIC
        dict_jmx[PLUGINTYPE] = doc
        dict_jmx[ACTUALPLUGINTYPE] = KAFKA_TOPIC
        #dict_jmx[PLUGIN_INS] = doc
        collectd.info("Plugin kafkatopic: Added common parameters successfully for %s doctype" % doc)

    def add_rate_dispatch_topic(self, pid, doc, dict_jmx):
        """Rate calculation for topic metrics"""
        for topic, topic_info in dict_jmx.items():
            self.add_common_params(doc, topic_info)
            if pid in self.prev_topic_data:
                if topic in self.prev_topic_data[pid]:
                    self.add_topic_rate(pid, topic, topic_info)
                else:
                    self.add_default_rate_value(topic_info, "topic")
            else:
                self.prev_topic_data[pid] = {}
                self.add_default_rate_value(topic_info, "topic")

            self.prev_topic_data[pid][topic] = topic_info
            self.dispatch_data(doc, deepcopy(topic_info))

    def add_rate_dispatch_kafka(self, pid, doc, dict_jmx):
        if pid in self.prev_data:
            self.add_rate(pid, dict_jmx)
        else:
            self.add_default_rate_value(dict_jmx, "kafka")

        self.prev_data[pid] = dict_jmx
        self.dispatch_data(doc, deepcopy(dict_jmx))

    def dispatch_partitions(self, doc, dict_jmx):
        partition_list = dict_jmx["partitionStats"]
        for partition in partition_list:
            self.add_common_params(doc, partition)
            self.dispatch_data(doc, partition)

    def get_pid_jmx_stats(self, pid, port, output):
        """Call get_jmx_parameters function for each doc_type and add dict to queue"""
        jolokiaclient = JolokiaClient.get_jolokia_inst(port)
        for doc in KAFKA_DOCS:
            try:
                dict_jmx = {}
                self.get_jmx_parameters(jolokiaclient, doc, dict_jmx)
                if not dict_jmx:
                    raise ValueError("No data found")

                collectd.info("Plugin kafkatopic: Added %s doctype information successfully for pid %s" % (doc, pid))
                if doc in ["topicStats", "partitionStats"]:
                    output.put((pid, doc, dict_jmx))
                    continue

                self.add_common_params(doc, dict_jmx)
                output.put((pid, doc, dict_jmx))
            except Exception as err:
                collectd.error("Plugin kafkatopic: Error in collecting stats of %s doctype: %s" % (doc, str(err)))

    def run_pid_process(self, list_pid):
        """Spawn process for each pid"""
        procs = []
        output = multiprocessing.Queue()
        for pid in list_pid:
            port = self.jclient.get_jolokia_port(pid)
            if port and self.jclient.connection_available(port):
                proc = multiprocessing.Process(target=self.get_pid_jmx_stats, args=(pid, port, output))
                procs.append(proc)
                proc.start()

        for proc in procs:
            proc.join()
#       for p in procs:
#          collectd.debug("%s, %s" % (p, p.is_alive()))
        return procs, output

    def collect_jmx_data(self):
        """Collects stats and spawns process for each pids."""
        list_pid = self.jclient.get_pid()
        if not list_pid:
            collectd.error("Plugin kafkatopic: No %s processes are running" % self.process)
            return

        procs, output = self.run_pid_process(list_pid)
        for _ in procs:
            for _ in KAFKA_DOCS:
                try:
                    pid, doc_name, doc_result = output.get_nowait()
                except Queue.Empty:
                    collectd.error("Failed to send one or more doctype document to collectd")
                    continue
                # Dispatching documentsTypes which are requetsed alone
                if doc_name in self.documentsTypes:
                    if doc_name == "topicStats":
                        self.add_rate_dispatch_topic(pid, doc_name, doc_result)
                    elif doc_name == "kafkaStats":
                        self.add_rate_dispatch_kafka(pid, doc_name, doc_result)
                    else:
                        self.dispatch_partitions(doc_name, doc_result)
        output.close()

    def dispatch_data(self, doc_name, result):
        """Dispatch data to collectd."""
        if doc_name == "topicStats":
            for item in ['messagesIn', 'bytesOut', 'bytesIn', 'totalFetchRequests', 'totalProduceRequests', 'produceMessageConversions',\
                         'failedProduceRequests', 'fetchMessageConversions', 'failedFetchRequests', 'bytesRejected']:
                try:
                    del result[item]
                except KeyError:
                    pass
                    #collectd.error("Key %s deletion error in topicStats doctype for topic %s: %s" % (item, result['_topicName'], str(err)))
            collectd.info("Plugin kafkatopic: Succesfully sent topicStats: %s" % result['_topicName'])

        elif doc_name == "kafkaStats":
            for item in ["messagesInPerSec", "bytesInPerSec", "bytesOutPerSec", "isrExpandsPerSec", "isrShrinksPerSec", "leaderElectionPerSec",\
                             "uncleanLeaderElectionPerSec", "producerRequestsPerSec", "fetchConsumerRequestsPerSec", "fetchFollowerRequestsPerSec"]:
                try:
                    del result[item]
                except KeyError:
                    pass
                    #collectd.error("Key %s deletion error in kafkaStats doctype: %s" % (item, str(err)))

            collectd.info("Plugin kafkatopic: Succesfully sent %s doctype to collectd." % doc_name)
            collectd.debug("Plugin kafkatopic: Values dispatched =%s" % json.dumps(result))

        else:
            collectd.info("Plugin kafkatopic: Succesfully sent topic %s of partitionStats: %s." % (result['_topicName'], result['_partitionNum']))
        utils.dispatch(result)

    def read_temp(self):
        """Collectd first calls register_read. At that time default interval is taken,
        hence temporary function is made to call, the read callback is unregistered
        and read() is called again with interval obtained from conf by register_config callback."""
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.collect_jmx_data, interval=int(self.interval))

def init():
    """When new process is formed, action to SIGCHLD is reset to default behavior."""
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)

OBJ = JmxStat()
collectd.register_init(init)
collectd.register_config(OBJ.config)
collectd.register_read(OBJ.read_temp)
