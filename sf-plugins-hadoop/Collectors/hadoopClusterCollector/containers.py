"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""

from library.elastic import *
from library.rest_api import get_active_resource_manager
import logging

logger = logging.getLogger(__name__)


def get_containers_node():
    host = get_active_resource_manager(resource_manager['hosts'])

    if host == None:
        logger.error("Active resource manager not found")
        return []

    port = resource_manager['port']
    path = '/ws/v1/cluster/nodes'
    nodes_json = http_request(host, port, path, scheme=resource_manager['scheme'])
    if nodes_json is None:
        return None

    logger.debug("Node JSON:{0}".format(json.dumps(nodes_json)))
    nodes_list = nodes_json["nodes"]["node"]
    for node in nodes_list:
        node['time'] = int(round(time.time()))
        node['_plugin'] = plugin_name['yarn']
        node['_documentType'] = "containerStats"
        node['_tag_appName'] = tag_app_name['yarn']

    return nodes_list


def get_cluster_metrics():
    host = resource_manager['host']
    port = resource_manager['port']
    path = '/ws/v1/cluster/metrics'
    metrics_json = http_request(host, port, path, scheme=resource_manager['scheme'])
    if metrics_json is None:
        return None

    metrics_json['time'] = int(round(time.time()))
    metrics_json['_plugin'] = plugin_name['yarn']
    metrics_json['_documentType'] = "clusterMetrics"
    metrics_json['_tag_appName'] = tag_app_name['yarn']

    logger.debug("Cluster metrics: {0}".format(json.dumps(metrics_json)))
    return metrics_json

def main():
    while True:
        docs = get_containers_node()
        for doc in docs:
            send_to_elasticsearch(json.dumps(doc), indices['yarn'])
        time.sleep(container_stats_time_interval)


if __name__ == '__main__':
    main()


