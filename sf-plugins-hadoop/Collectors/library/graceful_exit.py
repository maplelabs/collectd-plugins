"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""

import signal
import logging
stop_iteration = False


def setup_handlers():
    signal.signal(signal.SIGINT, graceful_exit_handler)
    signal.signal(signal.SIGTERM, graceful_exit_handler)

def graceful_exit_handler(signum, frame):
    global stop_iteration
    logger = logging.getLogger(__name__)
    logger.info("Received Term signal {0}. Will exit gracefully".format(signum))
    stop_iteration = True