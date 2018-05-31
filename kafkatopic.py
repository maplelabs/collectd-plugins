"""Python plugin for collectd to fetch kafkaStats and topicStats for kafka_topic process"""

#!/usr/bin/python
import os
import subprocess
import re
import signal
import json
import time
import collectd
import requests
import socket
import Queue
import multiprocessing
import kafka
from copy import deepcopy
from pyjolokia import Jolokia
from contextlib import closing
# user imports
import utils
from constants import *

KAFKA_DOCS = ["kafkaStats", "topicStats"]
BROKER_STATES = {0: "NotRunning", 1: "Starting", 2: "RecoveringFromUncleanShutdown", 3: "RunningAsBroker", \
                 6: "PendingControlledShutdown", 7: "BrokerShuttingDown"}
JOLOKIA_PATH = "/opt/collectd/plugins/"

class JmxStat(object):
    """Plugin object will be created only once and collects JMX statistics info every interval."""

    def __init__(self):
        """Initializes interval and previous dictionary variable."""
        self.interval = DEFAULT_INTERVAL
        self.process = None
        self.listenerip = 'localhost'
        self.prev_topic_data = {}
        self.prev_data = {}
        self.port = None

    def config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == PROCESS:
                self.process = children.values[0]
            if children.key == LISTENERIP:
                self.listenerip = children.values[0]
            if children.key == PORT:
                self.port = children.values[0]

    def get_cmd_output(self, cmd, shell_value=True, stdout_value=subprocess.PIPE,
                       stderr_value=subprocess.PIPE):
        """Returns subprocess output and return code of cmd passed in argument."""
        call = subprocess.Popen(cmd, shell=shell_value,
                                stdout=stdout_value, stderr=stderr_value)
        call.wait()
        status, err = call.communicate()
        returncode = call.returncode
        return status, err, returncode

    def check_prerequiste(self):
        """Need to run plugin as root."""
        if not os.geteuid() == 0:
            collectd.error("Please run collectd as root. Jmx_stats plugin requires root privileges")
            return False
        return True

    @staticmethod
    def get_free_port():
        """To get free port to run jolokia client."""
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sockt:
            sockt.bind(('localhost', 0))
            return sockt.getsockname()[1]

    def get_pid(self):
        """Get PIDs of all java process."""
        pid_cmd = "jcmd | awk '{print $1 \" \" $2}' | grep -w \"%s\"" % self.process
        pids, err = utils.get_cmd_output(pid_cmd)
        if err:
            collectd.error("Plugin kafka_topic: Error in collecting pid: %s" % err)
            return False
        pids = pids.splitlines()
        pid_list = []
        for pid in pids:
            if pid is not "":
                pidval = pid.split()
                pid_list.append(pidval[0])
        return pid_list

    def get_uid(self, pid):
        """Jolokia needs to be run with same user of the process attached"""
        uid_cmd = "awk '/^Uid:/{print $2}' /proc/%s/status" % pid
        uid, err = utils.get_cmd_output(uid_cmd)
        if err:
            collectd.error("Plugin kafka_topic:Failed to retrieve uid for pid %s, %s" % (pid, err))
            return False
        return uid.strip()

    def run_jolokia_cmd(self, cmd, pid, port=None):
        """Common logic to run jolokia cmds."""
        process_uid = self.get_uid(pid)
        jolokia_cmd = "sudo -u '#{0}' java -jar {1}jolokia.jar --host=127.0.0.1 {2} {3}".format(process_uid, JOLOKIA_PATH, cmd, pid)
        if port:
            jolokia_cmd += " --port=%s" % port
        return self.get_cmd_output(jolokia_cmd)

    def get_pid_port(self, pid):
        """check if jmx jolokia agent already running, if running get port"""
        status, err, ret = self.run_jolokia_cmd("status", pid)
        if err or ret:
            port = self.get_free_port()
            status, err, ret = self.run_jolokia_cmd("start", pid, port)
            if err or ret:
                collectd.error("Plugin kafka_topic: Unable to start jolokia client for pid %s, %s" % (pid, err))
                return False
            collectd.info("Plugin kafka_topic: started jolokia client for pid %s" % pid)
            return port

        # jolokia id already started and return port
        joloip = status.splitlines()[1]
        port = re.findall('\d+', joloip.split(':')[2])[0]
        collectd.debug("Plugin kafka_topic: jolokia client is already running for pid %s" % pid)
        return port

    def connection_available(self, port):
        """Check if jolokia client is up."""
        try:
            jolokia_url = "http://127.0.0.1:%s/jolokia/version" % port
            resp = requests.get(jolokia_url)
            if resp.status_code == 200:
                return True
        except requests.exceptions.ConnectionError:
            return False

    def get_jmx_parameters(self, port, doc, dict_jmx):
        """Fetch stats based on doc_type"""
        if not self.connection_available(port):
            return None
        jolokia_url = "http://127.0.0.1:%s/jolokia/" % port
        jolokiaclient = Jolokia(jolokia_url)
        if doc == "kafkaStats":
            self.add_kafka_parameters(jolokiaclient, dict_jmx)
        elif doc == "topicStats":
            self.add_topic_parameters(jolokiaclient, dict_jmx)

    def add_rate(self, pid, dict_jmx):
        """Rate only for kafka jmx metrics"""
        rate = utils.get_rate("messagesInPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["messagesIn"] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate("bytesInPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["bytesIn"] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate("bytesOutPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["bytesOut"] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate("isrExpandsPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["isrExpands"] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate("isrShrinksPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["isrShrinks"] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate("leaderElectionPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["leaderElection"] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate("uncleanLeaderElectionPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["uncleanLeaderElections"] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate("producerRequestsPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["producerRequests"] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate("fetchConsumerRequestsPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["fetchConsumerRequests"] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate("fetchFollowerRequestsPerSec", dict_jmx, self.prev_data[pid])
        if rate != NAN:
            dict_jmx["fetchFollowerRequests"] = round(rate, FLOATING_FACTOR)

    def add_topic_rate(self, pid, topic_name, topic_info):
        """Rate only for kafka topic metrics"""
        rate = utils.get_rate('messagesIn', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['messagesInRate'] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate('bytesOut', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['bytesOutRate'] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate('bytesIn', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['bytesInRate'] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate('totalFetchRequests', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['totalFetchRequestsRate'] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate('totalProduceRequests', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['totalProduceRequestsRate'] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate('produceMessageConversions', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['produceMessageConversionsRate'] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate('failedProduceRequests', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['failedProduceRequestsRate'] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate('fetchMessageConversions', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['fetchMessageConversionsRate'] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate('failedFetchRequests', topic_info, self.prev_topic_data[pid][topic_name])
        if rate != NAN:
            topic_info['failedFetchRequestsRate'] = round(rate, FLOATING_FACTOR)
        rate = utils.get_rate('bytesRejected', topic_info, self.prev_topic_data[pid][topic_name])
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
        jolokiaclient.add_request(type='read', mbean='kafka.server:name=RequestHandlerAvgIdlePercent,type=KafkaRequestHandlerPool', attribute='Count')
        jolokiaclient.add_request(type='read', mbean='kafka.controller:name=OfflinePartitionsCount,type=KafkaController', attribute='Value')
        jolokiaclient.add_request(type='read', mbean='kafka.controller:name=ActiveControllerCount,type=KafkaController', attribute='Value')
        jolokiaclient.add_request(type='read', mbean='kafka.controller:name=LeaderElectionRateAndTimeMs,type=ControllerStats', attribute='Count')
        jolokiaclient.add_request(type='read', mbean='kafka.controller:name=UncleanLeaderElectionsPerSec,type=ControllerStats', attribute='Count')
        jolokiaclient.add_request(type='read', mbean='kafka.network:name=RequestsPerSec,request=Produce,type=RequestMetrics', attribute='Count')
        jolokiaclient.add_request(type='read', mbean='kafka.network:name=RequestsPerSec,request=FetchConsumer,type=RequestMetrics', attribute='Count')
        jolokiaclient.add_request(type='read', mbean='kafka.network:name=RequestsPerSec,request=FetchFollower,type=RequestMetrics', attribute='Count')
        jolokiaclient.add_request(type='read', mbean='kafka.network:name=NetworkProcessorAvgIdlePercent,type=SocketServer', attribute='Value')
        jolokiaclient.add_request(type='read', mbean='kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchFollower', attribute='Mean,Max,Min')
        jolokiaclient.add_request(type='read', mbean='kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer', attribute='Mean,Max,Min')
        jolokiaclient.add_request(type='read', mbean='kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce', attribute='Mean,Max,Min')
        jolokiaclient.add_request(type='read', mbean='kafka.server:name=BrokerState,type=KafkaServer', attribute='Value')
        bulkdata = jolokiaclient.getRequests()
        if bulkdata[0]['status'] == 200:
            dict_jmx['underReplicatedPartitions'] = bulkdata[0]['value']
        if bulkdata[1]['status'] == 200:
            dict_jmx['messagesInPerSec'] = bulkdata[1]['value']
        if bulkdata[2]['status'] == 200:
            dict_jmx['bytesInPerSec'] = bulkdata[2]['value']
        if bulkdata[3]['status'] == 200:
            dict_jmx['bytesOutPerSec'] = bulkdata[3]['value']
        if bulkdata[4]['status'] == 200:
            dict_jmx['partitionCount'] = bulkdata[4]['value']
        if bulkdata[5]['status'] == 200:
            dict_jmx['isrExpandsPerSec'] = bulkdata[5]['value']
        if bulkdata[6]['status'] == 200:
            dict_jmx['isrShrinksPerSec'] = bulkdata[6]['value']
        if bulkdata[7]['status'] == 200:
            dict_jmx['requestHandlerAvgIdle'] = round(bulkdata[7]['value']/1000000000.0, 2)
        if bulkdata[8]['status'] == 200:
            dict_jmx['offlinePartitions'] = bulkdata[8]['value']
        if bulkdata[9]['status'] == 200:
            dict_jmx['activeController'] = bulkdata[9]['value']
        if bulkdata[10]['status'] == 200:
            dict_jmx['leaderElectionPerSec'] = bulkdata[10]['value']
        if bulkdata[11]['status'] == 200:
            dict_jmx['uncleanLeaderElectionPerSec'] = bulkdata[11]['value']
        if bulkdata[12]['status'] == 200:
            dict_jmx['producerRequestsPerSec'] = bulkdata[12]['value']
        if bulkdata[13]['status'] == 200:
            dict_jmx['fetchConsumerRequestsPerSec'] = bulkdata[13]['value']
        if bulkdata[14]['status'] == 200:
            dict_jmx['fetchFollowerRequestsPerSec'] = bulkdata[14]['value']
        if bulkdata[15]['status'] == 200:
            dict_jmx['networkProcessorAvgIdlePercent'] = round(bulkdata[15]['value'], 2)
        if bulkdata[16]['status'] == 200:
            dict_jmx['followerTimeMsMax'] = bulkdata[16]['value']['Max']
            dict_jmx['followerTimeMsMin'] = bulkdata[16]['value']['Min']
            dict_jmx['followerTimeMsMean'] = round(bulkdata[16]['value']['Mean'], 2)
        if bulkdata[17]['status'] == 200:
            dict_jmx['consumerTimeMsMax'] = bulkdata[17]['value']['Max']
            dict_jmx['consumerTimeMsMin'] = bulkdata[17]['value']['Min']
            dict_jmx['consumerTimeMsMean'] = round(bulkdata[17]['value']['Mean'], 2)
        if bulkdata[18]['status'] == 200:
            dict_jmx['producerTimeMsMax'] = bulkdata[18]['value']['Max']
            dict_jmx['producerTimeMsMin'] = bulkdata[18]['value']['Min']
            dict_jmx['producerTimeMsMean'] = round(bulkdata[18]['value']['Mean'], 2)
        if bulkdata[19]['status'] == 200:
            dict_jmx['brokerState'] = BROKER_STATES[bulkdata[19]['value']]

    def add_topic_parameters(self, jolokiaclient, dict_jmx):
        def get_partitions(topic):
            client = kafka.KafkaClient(self.listenerip+':'+self.port)
            topic_partition_ids = client.get_partition_ids_for_topic(topic)
            return len(topic_partition_ids)

        def get_topics():
            consumer = kafka.KafkaConsumer(bootstrap_servers=self.listenerip+':'+self.port)
            return consumer.topics()

        topics = get_topics()
        for topic in topics:
            # get topic partitions
            dict_jmx[topic] = {}
            dict_jmx[topic]['_topicName'] = topic
            partitions = get_partitions(topic)
            dict_jmx[topic]['partitionCount'] = partitions
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
                dict_jmx[topic]['messagesIn'] = msgIn['value']
            if bytesOut['status'] == 200:
                dict_jmx[topic]['bytesOut'] = bytesOut['value']
            if bytesIn['status'] == 200:
                dict_jmx[topic]['bytesIn'] = bytesIn['value']
            if totalFetch['status'] == 200:
                dict_jmx[topic]['totalFetchRequests'] = totalFetch['value']
            if totalProduce['status'] == 200:
                dict_jmx[topic]['totalProduceRequests'] = totalProduce['value']
            if produceMsg['status'] == 200:
                dict_jmx[topic]['produceMessageConversions'] = produceMsg['value']
            if failedProduce['status'] == 200:
                dict_jmx[topic]['failedProduceRequests'] = failedProduce['value']
            if fetchMsg['status'] == 200:
                dict_jmx[topic]['fetchMessageConversions'] = fetchMsg['value']
            if failedFetch['status'] == 200:
                dict_jmx[topic]['failedFetchRequests'] = failedFetch['value']
            if bytesReject['status'] == 200:
                dict_jmx[topic]['bytesRejected'] = bytesReject['value']

            for partition in range(0, partitions):
                numLogSegments = jolokiaclient.request(type='read', mbean='kafka.log:name=NumLogSegments,partition=%s,topic=%s,type=Log'%(partition, topic), attribute='Value')
                logSize = jolokiaclient.request(type='read', mbean='kafka.log:name=Size,partition=%s,topic=%s,type=Log'%(partition, topic), attribute='Value')
                if numLogSegments['status'] == 200:
                    dict_jmx[topic]['partition_'+str(partition)+'_LogSegments'] = numLogSegments['value']
                if logSize['status'] == 200:
                    dict_jmx[topic]['partition_'+str(partition)+'_LogSize'] = round(logSize['value'] / 1024.0/1024.0, 2)

    def add_common_params(self, doc, dict_jmx):
        """Adds TIMESTAMP, PLUGIN, PLUGITYPE to dictionary."""
        timestamp = int(round(time.time()))
        dict_jmx[TIMESTAMP] = timestamp
        dict_jmx[PLUGIN] = KAFKA_TOPIC
        dict_jmx[PLUGINTYPE] = doc
        dict_jmx[ACTUALPLUGINTYPE] = KAFKA_TOPIC
        dict_jmx[PROCESSNAME] = self.process
        dict_jmx[PLUGIN_INS] = doc
        collectd.info("Plugin kafka_topic: Added common parameters successfully")

    def add_rate_dispatch_topic(self, pid, doc, dict_jmx):
        """Rate calculation for topic metrics"""
        for topic, topic_info in dict_jmx.items():
            topic_info['_processPid'] = pid
            self.add_common_params(doc, topic_info)
            if pid in self.prev_topic_data:
                if topic in self.prev_topic_data[pid]:
                    self.add_topic_rate(pid, topic, topic_info)
                    self.dispatch_data(doc, deepcopy(topic_info))
                self.prev_topic_data[pid][topic] = topic_info
            else:
                self.prev_topic_data[pid] = {}
                self.prev_topic_data[pid][topic] = topic_info

    def get_pid_jmx_stats(self, pid, port, output):
        """Call get_jmx_parameters function for each doc_type and add dict to queue"""
        for doc in KAFKA_DOCS:
            try:
                dict_jmx = {}
                self.get_jmx_parameters(port, doc, dict_jmx)
                if not dict_jmx:
                    raise Exception("No data found")

                collectd.info("Plugin kafka_topic: Added doctype %s of pid %s information successfully" % (doc, pid))
                dict_jmx['_processPid'] = pid
                if doc == "topicStats":
                    output.put((doc, dict_jmx))
                    continue

                self.add_common_params(doc, dict_jmx)
                output.put((doc, dict_jmx))
            except Exception as err:
                collectd.error("Plugin kafka_topic: Error in collecting stats of doc type %s: %s" % (doc, str(err)))

    def run_pid_process(self, list_pid):
        """Spawn process for each pid"""
        procs = []
        output = multiprocessing.Queue()
        for pid in list_pid:
            port = self.get_pid_port(pid)
            if port:
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
        if not self.check_prerequiste():
            return

        list_pid = self.get_pid()
        if not list_pid:
            collectd.error("Plugin kafka_topic: No %s processes are running" % self.process)
            return

        procs, output = self.run_pid_process(list_pid)
        for _ in procs:
            for _ in KAFKA_DOCS:
                try:
                    doc_name, doc_result = output.get_nowait()
                except Queue.Empty:
                    collectd.error("Failed to send one or more doctype document to collectd")
                    continue

                pid = doc_result["_processPid"]
                if doc_name == "topicStats":
                    del doc_result["_processPid"]
                    self.add_rate_dispatch_topic(pid, doc_name, doc_result)
                else:
                    if pid in self.prev_data:
                        self.add_rate(pid, doc_result)
                        self.dispatch_data(doc_name, deepcopy(doc_result))
                    self.prev_data[pid] = doc_result
        output.close()

    def dispatch_data(self, doc_name, result):
        """Dispatch data to collectd."""
        if doc_name == "topicStats":
            for item in ['messagesIn', 'bytesOut', 'bytesIn', 'totalFetchRequests', 'totalProduceRequests', 'produceMessageConversions',\
                         'failedProduceRequests', 'fetchMessageConversions', 'failedFetchRequests', 'bytesRejected']:
                try:
                    del result[item]
                except Exception as err:
                    pass
                    #collectd.error("Key %s deletion error in topicStats doctype for topic %s: %s" % (item, result['_topicName'], str(err)))
            collectd.info("Plugin kafka_topic: Succesfully sent topicStats: %s" % result['_topicName'])
            utils.dispatch(result)
        else:
            for item in ["messagesInPerSec", "bytesInPerSec", "bytesOutPerSec", "isrExpandsPerSec", "isrShrinksPerSec", "leaderElectionPerSec",\
                             "uncleanLeaderElectionPerSec", "producerRequestsPerSec", "fetchConsumerRequestsPerSec", "fetchFollowerRequestsPerSec"]:
                try:
                    del result[item]
                except Exception as err:
                    pass
                    #collectd.error("Key %s deletion error in kafkaStats doctype: %s" % (item, str(err)))

            collectd.info("Plugin kafka_topic: Succesfully sent doctype %s to collectd." % doc_name)
            collectd.debug("Plugin kafka_topic: Values dispatched =%s" % json.dumps(result))
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
