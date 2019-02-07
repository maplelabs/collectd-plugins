"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""

from subprocess import check_call, CalledProcessError
from requests_kerberos import HTTPKerberosAuth, DISABLED
from configuration import *
import logging

logger = logging.getLogger(__name__)
kerberos_error = False
kerberos_auth = None

def kinit_tgt_for_user():
    try:
        result = check_call(['kinit', '-k', '-t', kerberos['keytab_file'], kerberos['principal']])
        return True if result == 0 else False
    except CalledProcessError as e:
        logging.exception("Failed to kinit")
    except Exception as e:
        logging.exception("Unknown Failure during kinit")

def handle_kerberos_error():
    global kerberos_error
    if kerberos['enabled'] and kerberos_error:
        while True:
            logger.debug("Kerberos error is True. Reinitializing TGT")
            if not kinit_tgt_for_user():
                logger.error("Failed to kinit and hence TGT is not obtained. Retrying")
                continue
            else:
                kerberos_initialize()
                kerberos_error = False
                break

def kerberos_initialize():
    global kerberos_auth
    global kerberos_error
    kerberos_error = False
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=DISABLED, force_preemptive=True, principal=kerberos['principal'])
    logger.debug("Initialized kerberos_auth")
    logger.debug(kerberos_auth)
