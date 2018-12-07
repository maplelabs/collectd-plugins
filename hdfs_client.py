#from hdfs.ext.kerberos import KerberosClient
from hdfs.client import InsecureClient
from requests import Session
from utilities import *
import json
#from configuration import *
import logging
import os
import collectd

logger = logging.getLogger(__name__)

client = None
kerberos = {"enabled": False}
hdfs_jobhistory_directory = "/mr-history/done"
jobhistory_copy_dir = "/tmp/"
hueristics_out_dir = ".var/hueristics"
use_rest_api = False

def initialize_hdfs_client(url):
    global client
    if not client:
        session = Session()
        session.verify = False
        if kerberos['enabled']:
            client = KerberosClient(url, session=session)
        else:
            client = InsecureClient(url, user="root", session=session)

def download_file(source, destination):
    #logger.info("Source file: {0}, Destination file: {1}".format(source,destination))
    if not os.path.exists(destination):
        client.download(source, destination, overwrite=True)

def _find_index_of_dest_dir(subdirs, job_finish_time):
    index = 0
    found = False
    for dirstatus in subdirs:
        if job_finish_time > dirstatus['modificationTime']:
            index += 1
        else:
            found = True
            break
    return index if found else -1

def copy_to_local(job_id, job_finish_time):
    global client
    try:
        file = None
        dest_file = None
        #job_finish_datetime = datetime.fromtimestamp(job_finish_time)
        job_finish_datetime = get_localized_datetime(job_finish_time, "US/Eastern")
        dir_to_search = "{0}/{1}/{2:02d}/{3:02d}/".format(hdfs_jobhistory_directory, job_finish_datetime.year,
                                                          job_finish_datetime.month, job_finish_datetime.day)
        initialize_hdfs_client("http://172.31.13.172:50070")
        subdirs_of_the_day = [status[1] for status in client.list(dir_to_search, status=True) ]
        for subdirstatus in subdirs_of_the_day:
            subdirstatus['modificationTime'] = int(subdirstatus['modificationTime'] / 1000)  # convert to epoch seconds
        subdirs_sorted_by_mod_time = sorted(subdirs_of_the_day, key=lambda k: k['modificationTime'])
        dir_index = _find_index_of_dest_dir(subdirs_sorted_by_mod_time, job_finish_time)
        if (dir_index != -1):
            result_sub_dir = subdirs_sorted_by_mod_time[dir_index]['pathSuffix']
            file_status_list = client.list(dir_to_search + subdirs_sorted_by_mod_time[dir_index]['pathSuffix'], status=True)
            file_list = [ filestatus[1] for filestatus in file_status_list ]
            result_list = [x for x in file_list if job_id in x['pathSuffix'] and 'jhist' in x['pathSuffix']]
            if result_list:
                file = dir_to_search+result_sub_dir+'/'+result_list[0]['pathSuffix']
                dest_file = jobhistory_copy_dir+result_list[0]['pathSuffix']
            if file:
                download_file(file, dest_file)
    except Exception as e:
        collectd.error(e)
	return None
    finally:
        return dest_file
