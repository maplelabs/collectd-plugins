import requests
from requests_kerberos import HTTPKerberosAuth, DISABLED
import sys

kerberos_auth = None

def kerberos_initialize():
    global kerberos_auth
    if not kerberos_auth:
        kerberos_auth = HTTPKerberosAuth(mutual_authentication=DISABLED, force_preemptive=True, principal=sys.argv[1])
#        kerberos_auth = HTTPKerberosAuth(principal=sys.argv[1])
        print kerberos_auth


def http_request(address, port, location, user="", pw=""):
    global kerberos_auth
    uri = 'https://{0}:{1}{2}'.format(address, port, location)
    try:
        r = requests.get(uri, auth=kerberos_auth, verify=False)
        print r.status_code
        print r.content
    except Exception as e:
        print "Exception occurred"
        print e

if __name__ == "__main__":
    kerberos_initialize()
    http_request(sys.argv[2], sys.argv[3], sys.argv[4])
