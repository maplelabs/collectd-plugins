import sys
from hdfs.ext.kerberos import KerberosClient
from hdfs.client import InsecureClient
from requests import Session
from requests_kerberos import HTTPKerberosAuth, DISABLED

session = Session()
session.verify = False
kerberos_auth = HTTPKerberosAuth(mutual_authentication=DISABLED, force_preemptive=True, principal='')
session.auth = kerberos_auth
client = KerberosClient("", session=session)
#client = InsecureClient("", session=session)
file = sys.argv[1]
destfile = sys.argv[2]

print client.list('/mr-history/done')

client.download(file, destfile, overwrite=True)

