import requests
import collectd
#from requests_kerberos import HTTPKerberosAuth, DISABLED
#from configuration import *
#import logging

#collectd.= logging.getLogger(__name__)

kerberos_auth = None

def kerberos_initialize():
    global kerberos_auth
    if not kerberos_auth:
        kerberos_auth = HTTPKerberosAuth(mutual_authentication=DISABLED, force_preemptive=True, principal=kerberos['principal'])

def http_request(address, port, location, user="", pw="", kerberos=False, scheme='https', params=None):
    global kerberos_auth
    uri = '{0}://{1}:{2}{3}'.format(scheme, address, port, location)
    try:
        if kerberos:
            r = requests.get(uri, auth=kerberos_auth, verify=False, params=params)
        elif user == "":
            r = requests.get(uri, verify=False, params=params)
        else:
            r = requests.get(uri, auth=(user, pw), params=params)
        r.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        collectd.error("Http Error: {0}".format(uri))
        return
    except requests.exceptions.ConnectionError as errc:
        collectd.error("Error Connecting: {0}".format(uri))
        return
    except requests.exceptions.Timeout as errt:
        collectd.error("Timeout Error: {0}".format(uri))
        return
    except requests.exceptions.RequestException as err:
        collectd.error("Oops: Some HTTP error {0}".format(uri))
        return

    json_data = r.json()
    return json_data


def http_post(address, port, location, data, headers):

    uri = 'http://{0}:{1}{2}'.format(address, port, location)
    timeout = 30

    try:
        collectd.debug("Data from http_post {0}".format(data))
        r = requests.post(uri, data=data, headers=headers, timeout=timeout)
        r.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        collectd.error("Http Error: {0}".format(uri))
        return
    except requests.exceptions.ConnectionError as errc:
        collectd.error("Error Connecting: {0}".format(uri))
        return
    except requests.exceptions.Timeout as errt:
        collectd.error("Timeout Error: {0}".format(uri))
        return
    except requests.exceptions.RequestException as err:
        collectd.error("Oops: Some HTTP error {0}".format(uri))
        return

    collectd.debug("HTTP post was successful")
    json_data = r.json()
    return json_data
