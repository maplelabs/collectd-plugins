from library.elastic import *
from library.buildData import *
from library.log import configure_logger
from library import graceful_exit
from library.kerberos_utils import *
from library.redis_utils import *

logger = logging.getLogger(__name__)

def get_oozie_wfs_processing_status():
    if app_status['use_redis']:
        return read_from_redis("oozieStatus")
    else:
        return get_processing_status("oozieStatus")

def get_last_runinfo(lastProcessedCreatedTime=0,lastProcessWorkflowId=None,runStatus='init'):
    lastRun = get_oozie_wfs_processing_status()
    logger.debug("lastrun: {0}".format(lastRun))
    if lastRun:
        return lastRun
    else:
        return init_last_runinfo()

def init_last_runinfo():
    document = {
                   "lastProcessedCreatedTime": int(math.floor(time.time())),
                   "lastProcessWorkflowId": "",
                   "runStatus": "init",
                   "erroString": " ",
                   "lastRunTime": int(math.floor(time.time())),
                   "type": "oozieStatus",
                }
    if not app_status['use_redis']:
        result = write_document_to_elastic(document, index=indices['workflowmonitor'])
        if result:
            document['id'] = result['_id']
        else:
            logger.error("Unable to init status in elastic.. Exiting")
            exit(1)
    else:
        result = write_to_redis("oozieStatus", json.dumps(document))
        if not result:
            logger.error("Unable to init status in redis. Exiting")
            exit(1)
    return document


def read_oozie_wfs(lastRunDetails, lastRunDocumentId):
    allWorkFlowsToWrite = []
    url_path = "/oozie/v1/jobs"
    minutesToRead = int(math.ceil((time.time() - lastRunDetails['lastProcessedCreatedTime'])/ 60))
    params = {"offset": 1}
    params['len'] = oozie['jobs_size']
    #params['filter'] = "startCreatedTime=-" + str(minutesToRead) + 'm'
    try:
        while True:
            logger.debug("url: {0}, params={1} current_time:{2} lastProcessedCreatedTime:{3}".format(url_path, params, time.time(), lastRunDetails['lastProcessedCreatedTime']))
            res_data = http_request(address=oozie['host'], port=oozie['port'], scheme=oozie['scheme'], path=url_path, params=params)
            index = 0
            if res_data:
                logger.debug("The oozie workflows are received successfully Total:%s Workflows length:%s" % (res_data['total'],
                                                                                     len(res_data['workflows'])))
                found = False
                if res_data['total'] > 0 and len(res_data['workflows']) > 0:
                    logger.debug(res_data)
                    for workflow in res_data['workflows']:
                        if (workflow['id'] == lastRunDetails['lastProcessWorkflowId'] or
                                                get_unix_timestamp(workflow['createdTime']) < lastRunDetails['lastProcessedCreatedTime']):
                            found = True
                            break
                        else:
                            index = index + 1
                    logger.debug("Index returned %s" % index)
                    if index == len(res_data['workflows']) and not found: # we have more workflows to process
                        params['offset'] = res_data['offset'] + len(res_data['workflows'])
                        allWorkFlowsToWrite.extend(res_data['workflows'][0:index]) if index != 0 else []
                    else:
                        #Always process from the last processed
                        allWorkFlowsToWrite.extend(res_data['workflows'][0:index]) if index != 0 else []
                        break;
                else:
                    break
            else:
                logger.debug("The oozie server responded with error")
                break
        numberOfDocs = len(allWorkFlowsToWrite)
        logger.info("Number of workflows to write %s" % numberOfDocs)
        result = bulk_write_to_elastic(allWorkFlowsToWrite) if numberOfDocs > 0 else True
        statusData = prepare_workflow_monitor(True, allWorkFlowsToWrite[0]) if (result and numberOfDocs > 0) else prepare_workflow_monitor(True, lastRunDetails)
        result = update_status(statusData, lastRunDocumentId)
        if result:
            logger.debug("Status successfully updated")
    except Exception as e:
        logger.error("Exception in the readOozieWorkflows due to %s" % e)

def update_status(statusData, lastRunDocumentId):
    if app_status['use_redis']:
        return write_to_redis('oozieStatus', json.dumps(statusData))
    else:
        return update_document_in_elastic({"doc": statusData}, lastRunDocumentId, indices['workflowmonitor'])


def initialize_app():
    log_config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'loggingoozie.conf')
    configure_logger(log_config_file, logging_config['ozzieWorkflows'])
    if kerberos["enabled"]:
        if kinit_tgt_for_user():
            kerberos_initialize()
        else:
            logging.error("Failed to kinit with kerberos enabled. Exiting")
            exit(1)

def run_application(index=0):
    try:
        logger.info("Processing Oozie workflows started for iteration {0}".format(index + 1))
        lastRunDocument = get_last_runinfo()
        lastRunDocumentId = None
        lastRunInfo = None
        if lastRunDocument is not None:
            lastRunInfo = lastRunDocument
            lastRunDocumentId =  None if app_status['use_redis'] else lastRunDocument['id']
        else:
            logger.error("Could not get application status . Exiting")
            exit(1)
        logger.debug(lastRunInfo)
        read_oozie_wfs(lastRunInfo, lastRunDocumentId)
        logger.info("Processing Oozie workflows end for iteration {0}".format(index + 1))
        while kerberos["enabled"] and kerberos_error:
            time.sleep(30)
            handle_kerberos_error()
        while app_status['use_redis'] and redis_error:
            time.sleep(30)
            handle_redis_error()
    except Exception:
        logger.exception("Received Exception")


if __name__ == "__main__":
    initialize_app()
    index = 0
    while True:
        run_application(index)
        logger.info("Sleeping for {0} seconds".format(sleep_time_in_secs["oozieWorkflows"]))
        time.sleep(sleep_time_in_secs["oozieWorkflows"])
        index = index + 1

