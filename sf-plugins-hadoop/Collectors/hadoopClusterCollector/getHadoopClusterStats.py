from name_node import collect_name_node_metrics
from yarn_stats import collect_yarn_metrics
from containers import get_containers_node
from library.log import configure_logger
from library.kerberos_utils import *
from library import graceful_exit
from configuration import logging_config
from library.elastic import send_to_elasticsearch
import json
import logging
import time

logger = logging.getLogger(__name__)

def initialize_app():
    configure_logger("logginghadoop.conf", logging_config['hadoopCluster'])
    if kerberos["enabled"]:
        if kinit_tgt_for_user():
            kerberos_initialize()
        else:
            logging.error("Failed to kinit with kerberos enabled. Exiting")
            exit(1)

def run_application(index):
    logger.info("Processing hadoop cluster stats start for iteration {0}".format(index + 1))

    collect_name_node_metrics()

    collect_yarn_metrics()

    container_docs = get_containers_node()
    if container_docs:
        logger.debug("CONTAINER DOCS: {0}".format(container_docs))
        for doc in container_docs:
            send_to_elasticsearch(json.dumps(doc), indices['yarn'])
    logger.info("Processing hadoop cluster stats end for iteration {0}".format(index + 1))
    handle_kerberos_error()

def main():
    initialize_app()
    index = 0
    while True:
        try:
            run_application(index)
        except Exception:
            logger.exception("Received Exception")
        finally:
            index += 1

        time.sleep(yarn_stats_time_interval)

if __name__ == "__main__":
    main()

