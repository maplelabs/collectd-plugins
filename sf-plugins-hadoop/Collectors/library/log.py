"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""

import os
from configuration import *
import logging
import logging.config
from utilities import mkdir_p

def configure_logger(logging_config_file, log_file):
    try:
        if not os.path.exists(os.path.dirname(os.path.abspath(log_file))):
            mkdir_p(os.path.dirname(os.path.abspath(log_file)))
        logging.config.fileConfig(logging_config_file, disable_existing_loggers=False, defaults={'logfilename': log_file})
        logging.getLogger("urllib3").setLevel(logging.ERROR)
        logging.getLogger("requests_kerberos").setLevel(logging.ERROR)
    except Exception as e:
        print("Error in configuring logger %s" % e)
        exit(1)
