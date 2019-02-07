"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""

import requests
import json
from requests_kerberos import MutualAuthenticationError
from configuration import modify_user_agent_header, kerberos
import kerberos_utils
import logging

logger = logging.getLogger(__name__)

def http_request(address, port, path, user="", pw="", scheme='http', params=None):
    uri = '{0}://{1}:{2}{3}'.format(scheme, address, port, path)
    if modify_user_agent_header:
        headers = {'user-agent': 'curl/2.19.0'}
    else:
        headers = {}
    try:
        if kerberos['enabled']:
            r = requests.get(uri, auth=kerberos_utils.kerberos_auth, verify=False, params=params, headers=headers)
        elif user == "":
            r = requests.get(uri, verify=False, params=params)
        else:
            r = requests.get(uri, auth=(user, pw), params=params)
        r.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        kerberos_utils.kerberos_error = True
        logger.exception("HTTP Error")
        logger.error("Http Error: {0}".format(uri))
        return
    except requests.exceptions.ConnectionError as errc:
        logger.exception("Error Connecting: {0}".format(uri))
        return
    except requests.exceptions.Timeout as errt:
        logger.exception("Timeout Error: {0}".format(uri))
        return
    except MutualAuthenticationError as errk:
        logger.exception(errk)
        kerberos_utils.kerberos_error = True
        return
    except requests.exceptions.RequestException as err:
        kerberos_utils.kerberos_error = True
        logger.exception("Oops: Some HTTP error {0}".format(uri))
        return


    json_data = r.json()
    return json_data


def http_post(address, port, path, data, headers, scheme='http'):

    uri = '{0}://{1}:{2}{3}'.format(scheme, address, port, path)
    timeout = 30

    try:
        #logger.debug("Body for http_post {0}".format(data))
        #logger.debug("Uri for http_post {0}".format(uri))
        r = requests.post(uri, data=data, headers=headers, timeout=timeout)
        r.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        logger.exception("Http Error: {0}".format(uri))
        return
    except requests.exceptions.ConnectionError as errc:
        logger.error("Error Connecting: {0}".format(uri))
        return
    except requests.exceptions.Timeout as errt:
        logger.error("Timeout Error: {0}".format(uri))
        return
    except requests.exceptions.RequestException as err:
        logger.error("Oops: Some HTTP error {0}".format(uri))
        return

    logger.debug("HTTP post was successful")
    if r.status_code != 204:
        json_data = r.json()
    else:
        json_data = json.dumps({"status_code": 204})
    return json_data

def http_put(address, port, path, data, headers, scheme='http'):

    uri = '{0}://{1}:{2}{3}'.format(scheme, address, port, path)
    timeout = 30

    try:
        #logger.debug("Body for http_post {0}".format(data))
        #logger.debug("Uri for http_post {0}".format(uri))
        r = requests.put(uri, data=data, headers=headers, timeout=timeout)
        r.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        logger.exception("Http Error: {0}".format(uri))
        return
    except requests.exceptions.ConnectionError as errc:
        logger.error("Error Connecting: {0}".format(uri))
        return
    except requests.exceptions.Timeout as errt:
        logger.error("Timeout Error: {0}".format(uri))
        return
    except requests.exceptions.RequestException as err:
        logger.error("Oops: Some HTTP error {0}".format(uri))
        return

    logger.debug("HTTP put was successful")
    if r.status_code != 204:
        json_data = r.json()
    else:
        json_data = json.dumps({"status_code": 204})
    return json_data
