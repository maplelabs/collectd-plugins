from input import *
from library.http_request import *
import time
import os
import json
import logging
from library.log import configure_logger

logger = logging.getLogger(__name__)

def read_file(file):
        with open (file,"r") as file_pointer:
            contents = json.load(file_pointer)
        logger.debug("Last status:{0}".format(contents))

        return contents


def write_to_file(file, status):
     file_pointer = open(file,"w")
     json.dump(status,file_pointer)
     logger.debug("Updated current status:{0}".format(status))
     file_pointer.close()


def get_active_es_cluster_nodes():
    rest_nodes_path = '/_nodes'
    result = None
    for host in elastic['hosts']:
        result = http_request(host, elastic['port'], path=rest_nodes_path, scheme=elastic['scheme'])
        logger.debug(result)
        if result:
            break

    if result:
        return [node['host'] for node in result['nodes'].values()]
    else:
        return None

def is_active_nodes_changed(active_nodes , last_status):
    changed = False
    for host in active_nodes:
        if host not in last_status:
            changed = True
            break
    if changed:
        return True
    elif last_status and len(active_nodes) != len(last_status):
        return True
    else:
        return False

def is_kafka_connector_active():
    connectors_path = "/connectors"
    for node in confluent_kafka_rest_server['hosts']:
        result = http_request(node, confluent_kafka_rest_server['port'], path=connectors_path, scheme=elastic['scheme'])
        if result:
            break

    if result:
        logger.debug("Kafka connector active: {0}".format(result))
        return result
    else:
        logger.error("Failed to find a active Kafka connector ")
        return None


def get_kafka_connector_sink_info():
    config_path = "/connectors/{0}".format(kafka_sink_name)
    for node in confluent_kafka_rest_server['hosts']:
        result = http_request(node, confluent_kafka_rest_server['port'], path=config_path, scheme=elastic['scheme'])
        if result:
            break

    if result:
        logger.debug("Kafka sink info: {0}".format(result))
        return result
    else:
        logger.error("Failed to get a active Kafka sink information ")
        return None

def is_task_running(taskId):
    task_status_path = "/connectors/{0}/tasks/{1}/status".format(kafka_sink_name, taskId)
    for node in confluent_kafka_rest_server['hosts']:
        result = http_request(node, confluent_kafka_rest_server['port'], path=task_status_path, scheme=elastic['scheme'])
        if result:
            break

    if result and 'state' in result:
        return result['state'].lower() == "running"
    else:
        return False

def update_config(config):
    headers = {'Content-Type': 'application/json'}
    es_sink_config_path =   '/connectors/{0}/config'.format(kafka_sink_name)
    for node in confluent_kafka_rest_server['hosts']:
        result = http_put(node, confluent_kafka_rest_server['port'], data=json.dumps(config),path=es_sink_config_path, headers=headers, scheme=elastic['scheme'])
        if result:
            break

    if result is not None:
        return True
    else:
        return False

def restart_task(taskId):
    restart_path = "/connectors/{0}/tasks/{1}/restart".format(kafka_sink_name, taskId)
    for node in confluent_kafka_rest_server['hosts']:
        result = http_post(node, confluent_kafka_rest_server['port'], data="", path=restart_path, headers={}, scheme=elastic['scheme'])
        if result:
            break

    if result is not None:
        return True
    else:
        return False


def perform_connector_tasks_validation(elastic_nodes_changed, connection_url):
    result = True
    if is_kafka_connector_active():
        elastic_sink_info = get_kafka_connector_sink_info()
        if elastic_sink_info and elastic_sink_info['config'] and elastic_sink_info['tasks']:
            config = elastic_sink_info['config']
            if elastic_nodes_changed:
                logger.debug("Old config:{0}".format(config))
                config['connection.url'] = connection_url
                logger.debug("Modified config:{0}".format(config))
                logger.debug("Elastic nodes have changed. Updating config")
                if update_config(config):
                    logger.debug("Sink configuration updated successfully")
                else:
                    logger.error("Failed to update sink configuration")
                    return False
            for index, task in enumerate(elastic_sink_info['tasks']):
                task_state = is_task_running(task['task'])
                logger.debug("Is task {0} running: {1}".format(task["task"], task_state))
                if elastic_nodes_changed or not task_state:
                     logger.debug("Elastic nodes changed or task not running. Restarting task:{0}".format(task['task']))
                     if restart_task(task['task']):
                         logger.debug("Task {0} restart successful".format(task['task']))
                     else:
                         logger.error("Task {0} restart failed".format(task['task']))
                         result = False
        else:
            logger.error("Failed to get sink configuration or active tasks.")
    return result


def initialize_app():    
    log_config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'loggingmonitorkafkaelk.conf')
    configure_logger(log_config_file, logging_config['monitorkafkaelk'])

def run_application():
    while True:
        last_status = {}
        if os.path.isfile(status_file):
            last_status = read_file(status_file)
        else:
            last_status = elastic['hosts']
        current_active_nodes = get_active_es_cluster_nodes()
        if current_active_nodes:
            connection_url = ','.join(["{0}://{1}:{2}".format
                                                 (elastic['scheme'], node, elastic['port']) for node in current_active_nodes ])
            validation_status = False
            if is_active_nodes_changed(current_active_nodes, last_status):
                logger.debug("New Change in elastic nodes:{0} compared to last_status:{1}".format(current_active_nodes, last_status))
                validation_status = perform_connector_tasks_validation(True, connection_url)
            else:
                logger.debug("No change in elastic nodes:{0} compared to last_status:{1}".format(current_active_nodes, last_status))
                validation_status = perform_connector_tasks_validation(False, connection_url)
            if validation_status:
                write_to_file(status_file, current_active_nodes)
        else:
            logger.error("No Elastic nodes are active or unable to reach the elk cluster")
        time.sleep(30)

if __name__ == "__main__":
    initialize_app()
    run_application()
