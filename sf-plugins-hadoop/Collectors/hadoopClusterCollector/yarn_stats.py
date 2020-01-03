"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""

from library.elastic import *
from library.kerberos_utils import *
from library.rest_api import *
from library.log import configure_logger
from configuration import *
from containers import get_containers_node

logger = logging.getLogger(__name__)

def initialize_app():
    log_config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logginghadoop.conf")
    configure_logger(log_config_file, logging_config['yarn'])
    if kerberos["enabled"]:
        if kinit_tgt_for_user():
            kerberos_initialize()
        else:
            logging.error("Failed to kinit with kerberos enabled. Exiting")
            exit(1)

def run_application(index):
    logger.info("Processing yarn stats start for iteration {0}".format(index + 1))

    collect_yarn_metrics()

    container_docs = get_containers_node()
    if container_docs:
        logger.debug("CONTAINER DOCS: {0}".format(container_docs))
        for doc in container_docs:
            send_to_elasticsearch(json.dumps(doc), indices['yarn'])
    logger.info("Processing yarn stats end for iteration {0}".format(index + 1))
    handle_kerberos_error()

def get_yarn_stats():
    host = get_active_resource_manager(resource_manager['hosts'])
    if host == None:
        logger.error("Active resource manager not found")
        return None

    port = resource_manager['port']
    path = "/jmx?qry=Hadoop:service=ResourceManager,name={}".format('JvmMetrics')
    hostname = ""
    json_yarn_node = http_request(host, port, path, scheme=resource_manager['scheme'])
    if json_yarn_node is not None:
        json_yarn_node = json_yarn_node['beans']
        if json_yarn_node:
            json_yarn_node[0]['time'] = int(time.time() * 1000)
            json_yarn_node[0]['_plugin'] = plugin_name['yarn']
            json_yarn_node[0]['_documentType'] = "yarnStats" + 'JvmMetrics'
            json_yarn_node[0]['_tag_appName'] = tag_app_name['yarn']
            hostname = json_yarn_node[0]['tag.Hostname']
            for f in json_yarn_node[0].keys():
                if '.' in f:
                    remove_dot(json_yarn_node[0], f)
        else:
            json_yarn_node = []
    else:
        return None

    for a in ['RpcActivityForPort8031', 'RpcActivityForPort8032', 'RpcActivityForPort8033', 'RpcActivityForPort8025',
              'RpcActivityForPort8050', 'QueueMetrics,q0=root', 'ClusterMetrics']:
        path = "/jmx?qry=Hadoop:service=ResourceManager,name={}".format(a)
        json_doc = http_request(host, port, path, scheme=resource_manager['scheme'])
        try:
            if json_doc['beans'] == []:
                continue
            if 'QueueMetrics' in a:
                for bean in json_doc['beans']:
                    if bean['tag.Queue'] == "root":
                        doc = bean
                        break
            else:
                doc = json_doc['beans'][0]
        except KeyError as e:
            logger.exception("Exception: ")
            return None

        doc['time'] = int(time.time() * 1000)
        doc['_plugin'] = plugin_name['yarn']
        doc['_tag_appName'] = tag_app_name['yarn']

        if 'tag.Hostname' not in doc:
            doc['tag.Hostname'] = hostname

        if 'RpcActivity' in a:
            doc['_documentType'] = "yarnStats" + 'RpcActivity'
        else:
            doc['_documentType'] = "yarnStats" + a.split(',')[0]

        for f in doc.keys():
            if '.' in f:
                remove_dot(doc, f)

        json_yarn_node.append(doc)

    #logger.debug(json.dumps(json_yarn_node))
    return json_yarn_node

def collect_yarn_metrics():
    docs1 = get_yarn_stats()
    docs_for_diff = docs1[:]

    fields_for_diff = {
        'yarnStatsJvmMetrics': ['GcCount', 'GcTimeMillis', 'LogFatal', 'LogError', 'LogWarn', 'LogInfo'],
        'yarnStatsQueueMetrics': ['AppsSubmitted', 'AppsCompleted', 'AppsKilled', 'AppsFailed']
    }

    find_diff(docs_for_diff, fields_for_diff, previous_json_yarn)
    logger.debug("Docs with diff {0}".format(json.dumps(docs_for_diff)))

    for doc in docs_for_diff:
        send_to_elasticsearch(json.dumps(doc), indices['yarn'])
        logger.debug(doc)

def main():
    initialize_app()
    index = 0
    while True:
        try:
            run_application(index)
        except Exception as e:
            logger.debug("Received Exception")
        finally:
            index = index +1
            time.sleep(yarn_stats_time_interval)


if __name__ == '__main__':
    main()
