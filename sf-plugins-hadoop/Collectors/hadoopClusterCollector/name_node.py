"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""

from library.elastic import *
from library.rest_api import *
import logging
from library.kerberos_utils import *
from library.log import configure_logger
from configuration import *

logger = logging.getLogger(__name__)


def initialize_app():
    log_config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logginghadoop.conf")
    configure_logger(log_config_file, logging_config['namenode'])
    if kerberos["enabled"]:
        if kinit_tgt_for_user():
            kerberos_initialize()
        else:
            logging.error("Failed to kinit with kerberos enabled. Exiting")
            exit(1)

def run_application(index):
    logger.info("Processing name node stats start for iteration {0}".format(index + 1))
    collect_name_node_metrics()
    logger.info("Processing name node stats end for iteration {0}".format(index + 1))
    handle_kerberos_error()

def get_name_node_stats():

    host = get_active_nn(name_node['hosts'])
    if host == None:
       logger.error("Active name node not found")
       return None

    port = name_node['port']
    path = "/jmx?qry=Hadoop:service=NameNode,name={}".format('JvmMetrics')
    json_name_node = http_request(host, port, path, scheme=name_node['scheme'])

    hostname = ''
    if json_name_node is not None:
        json_name_node = json_name_node['beans']
        if json_name_node:
            json_name_node[0]['time'] = int(time.time() * 1000)
            json_name_node[0]['_plugin'] = plugin_name['namenode']
            json_name_node[0]['_documentType'] = "nameNodeStats" + 'JvmMetrics'
            json_name_node[0]['_tag_appName'] = tag_app_name['namenode']
            hostname = json_name_node[0]['tag.Hostname']
        else:
            json_name_node = []
        for f in json_name_node[0].keys():
            if '.' in f:
                remove_dot(json_name_node[0], f)
    else:
        return None


    for a in ['FSNamesystemState', 'FSNamesystem', 'RpcActivityForPort8020']:
        path = "/jmx?qry=Hadoop:service=NameNode,name={}".format(a)
        json_doc = http_request(host, port, path, scheme=name_node['scheme'])
        try:
            if json_doc['beans'] == []:
                continue
            doc = json_doc['beans'][0]
        except KeyError as e:
           logger.exception("Error: ")
           return None
        if 'TopUserOpCounts' in doc:
            doc.pop('TopUserOpCounts')
        if 'tag.Hostname' not in doc:
            doc['tag.Hostname'] = hostname
        doc['time'] = int(time.time() * 1000)
        doc['_plugin'] = plugin_name['namenode']

        if 'RpcActivity' in a:
            doc['_documentType'] = "nameNodeStats" + 'RpcActivity'
        else:
            doc['_documentType'] = "nameNodeStats" + a

        doc['_tag_appName'] = tag_app_name['namenode']

        if a == 'FSNamesystem':
            doc.pop('CapacityUsed', None)
            doc.pop('CapacityRemaining', None)
            doc.pop('CapacityTotal', None)
        if a == 'FSNamesystemState':
            doc.pop('FilesTotal', None)

        for f in doc.keys():
            if '.' in f:
                remove_dot(doc, f)

        json_name_node.append(doc)

    logger.debug(json.dumps(json_name_node))
    return json_name_node

def collect_name_node_metrics():
    docs = get_name_node_stats()
    fields_for_diff = {'nameNodeStatsRpcActivity':
                           ['ReceivedBytes', 'SentBytes']}
    docs_for_diff = docs[:]
    find_diff(docs_for_diff, fields_for_diff, previous_json_nn)
    logger.debug("Docs with diff {0}".format(json.dumps(docs_for_diff)))

    for doc in docs_for_diff:
        send_to_elasticsearch(json.dumps(doc), indices['namenode'])

def main():
    while True:
        collect_name_node_metrics()
        time.sleep(name_node_stats_time_interval)


if __name__ == '__main__':
    main()

