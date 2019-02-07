"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""

from library.elastic import *
from library.rest_api import *
from library.log import configure_logger
from metrics import *
import os
import math
from multiprocessing.dummy import Pool as ThreadPool
from library.kerberos_utils import *
from process_jhist import process_jhist
from library.hdfs_client import initialize_hdfs_client, copy_to_local
from  library.redis_utils import *
import argparse

THREAD_COUNT = 15
pool = None
with_threading = False
error_wfs_processed = 0

logger = logging.getLogger(__name__)

def read_unprocessed_workflows():
    try:
        global with_threading
        res_json = search_workflows_in_elastic()
        logger.debug(res_json)
        if res_json:
            if with_threading:
                process_workflows_with_threads(res_json)
            else:
                process_workflows(res_json)
    except Exception as e:
        logger.error("Exception in the read_unprocessed_workflows due to %s" % e)


def process_workflows(workFlowRes):
    logger.debug("Processing Workflows")
    workflows_processed = 0
    for workflow in workFlowRes['hits']['hits']:
        if process_workflow(workflow):
            workflows_processed += 1
        else:
            logger.debug("Failed to process workflow {0}".format(workflow['_source']['wfId']))
        #if workflows_processed == 1:
        #    logger.info("Number of Workflows processed %s" % workflows_processed)
        #    exit(0)
    logger.info("Number of Workflows processed %s" % workflows_processed)

def callback_wf_processed(status):
    global wfs_processed
    logger.debug("Callback called from async processing")
    logger.info(status)
    wfs_processed += len(status)

def _callback_error_wf_processed(status):
    global error_wfs_processed
    logger.debug("Callback called from error async processing")
    error_wfs_processed += 1

def parse_args_for_config():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="config file")
    args = parser.parse_args()
    return args

def process_workflows_with_threads(workFlowRes):
    logger.debug("Processing Workflows with threads")
    global pool
    global wfs_processed
    r = pool.map_async(process_workflow, workFlowRes['hits']['hits'], callback=callback_wf_processed)
    r.wait()
    logger.info("Number of workflows processed with threading {0}".format(wfs_processed))

def process_workflow(workflow):
    wf_process_start_time = time.time()
    url_format = "/oozie/v1/job/%s"
    url = url_format % (workflow['_source']['wfId'])
    workflowData = workflow['_source']
    logger.info("WF {0} processing start time is {1}".format(workflow['_source']['wfId'], wf_process_start_time))
    result = False
    try:
        res_data = http_request(address=oozie['host'], port=oozie['port'], path=url, scheme=oozie['scheme'])
        if res_data:
            #logger.debug("The oozie job detail received successfully for {0}, response: {1}".format( workflowData['wfId'], res_data))
            post_data = {"workflow" : workflow,
                         "workflowActions": [],
                         }
            if res_data['status'] == "SUCCEEDED" or res_data['status'] == "KILLED" or res_data['status'] == "FAILED":
                for action in res_data['actions']:
                    actionData = {"action": None,
                                  "yarnJobs": []}
                    #Process Launcher job or if this is the only job with no childIDs
                    workflowActionData = prepare_workflow_action_data(action, 
                                                                      workflowData['wfId'], workflowData['wfName'])
                    actionData['action'] = workflowActionData
                    if action['externalId'] and action['externalId'] != '-':
                        yarnJobInfo = processYarnJob(action['externalId'], workflowData['wfId'], workflowData['wfName'],
                                                     action['id'], action['name'])
                        if yarnJobInfo:
                            actionData['yarnJobs'].append(yarnJobInfo)
                        else:
                            logger.error("Don't have all info for wfaId %s" % action['id'])
                    if action['externalChildIDs']:
                        for externalChildID in action['externalChildIDs'].split(','):
                            yarnJobInfo = processYarnJob(externalChildID, workflowData['wfId'], workflowData['wfName'],action['id'], action['name'])
                            if yarnJobInfo:
                                actionData['yarnJobs'].append(yarnJobInfo)
                            else:
                                logger.error("Don't have all info for wfaId %s" % action['id'])
                    if actionData['action']:
                        post_data["workflowActions"].append(actionData)
                if post_data["workflowActions"]:
                    calculate_wf_metrics(res_data, post_data['workflowActions'])
                    try:
                        calculate_scheduling_delays_from_critical_path(res_data, post_data['workflowActions'])
                    except Exception as e:
                        logger.exception("Failed to calculate delays from critical path")
                    #logger.debug("Logging workflow details")
                    #logger.debug(post_data)
                    for wfa in post_data['workflowActions']:
                        if wfa['action']['endTime'] == -1:
                            wfa['action']['endTime'] = res_data['endTime']
                    result = send_bulk_workflow_to_elastic(res_data, post_data)
                    if result:
                        result = send_bulk_map_reduce_info_to_elastic(res_data, post_data)
            elif res_data['status'] != "RUNNING":
                logger.debug("Current status: {0}, existing status:{1}".format(res_data['status'], workflowData['status']))
                if res_data['status'] != workflowData['status'] or int(math.floor(time.time())) - workflowData['time'] > 86400:
                    wf_data_to_send = prepare_workflow(res_data)
                    wf_data_to_send['status'] = res_data['status']
                    if int(math.floor(time.time())) - workflowData['time'] > 86400:
                        wf_data_to_send['workflowMonitorStatus'] = 'processed'
                    doc_data = {"doc": wf_data_to_send}
                    update_document_in_elastic(doc_data, workflow['_id'], indices['workflow'])
        else:
            logger.error("Could not get workflow details for %s, received error from oozie server" % (workflowData['wfId']))
            result = False
    except Exception as e:
        logger.exception('Error processing workflow => ' + traceback.format_exc().splitlines()[-1])
        result = False
    finally:
        wf_process_end_time = time.time()
        logger.info("Time taken to process wf {0} is {1}".format(workflow['_source']['wfId'], wf_process_end_time - wf_process_start_time))
        logger.info("WF {0} processing end time is {1}".format(workflow['_source']['wfId'], wf_process_end_time))
        return result


def send_bulk_map_reduce_info_to_elastic(workflowData, post_data, index=indices['workflow']):
    result = False
    if post_data and 'workflowActions' in post_data:
        for workflowAction in post_data['workflowActions']:
            for yarnJobInfo in workflowAction['yarnJobs']:
                # task Information
                if yarnJobInfo['taskInfo'] is not None and yarnJobInfo['taskInfo']:
                    result = build_and_send_bulk_docs_to_elastic(yarnJobInfo['taskInfo'], "taskInfo", indices['workflow'])
                else:
                    result = True

                # task Attempt
                if result and yarnJobInfo['taskAttemptsCounters'] is not None and yarnJobInfo['taskAttemptsCounters']:
                    result = build_and_send_bulk_docs_to_elastic(yarnJobInfo['taskAttemptsCounters'],
                                                                 "taskAttemptsCounters", indices['workflow'])
                else:
                    result = True

                # time point Task counts
                if result and yarnJobInfo['tpTaskStats']:
                    result = build_and_send_bulk_docs_to_elastic(yarnJobInfo['tpTaskStats'], "tpTaskStats", indices['workflow'])
                else:
                    result = True

    return result


def compute_job_stats(job_info, app_info, task_info, tasks_map, tasks_reduce, oozieWorkflowId, oozieWorkflowActionId, oozieWorkflowName, oozieWorkflowActionName):
    yarnJobInfo = None
    hueristics = {}
    if job_info:
        job = job_info['jobId']
        job_info['waitTime'] = 0
        if tasks_reduce or tasks_map:
            job_info["waitTime"] = get_wait_time(job_info, tasks_reduce, tasks_map)
        if tasks_map:
            find_stragglers_runtime(tasks_map)
        if tasks_reduce:
            find_stragglers_runtime(tasks_reduce)
        tpTaskStats = calculate_taskcount_by_time_points(job_info, tasks_map + tasks_reduce, wfId=oozieWorkflowId,
                                                         wfaId=oozieWorkflowActionId, wfName=oozieWorkflowName,
                                                         wfaName=oozieWorkflowActionName)
    yarnJobInfo = {
        'job': job_info,
        'appInfo': app_info,
        'taskInfo': task_info,
        'taskAttemptsCounters': tasks_map + tasks_reduce,
        'tpTaskStats': tpTaskStats
    }
    return yarnJobInfo


def processYarnJob(yarnJobId, oozieWorkflowId, oozieWorkflowName, oozieWorkflowActionId, oozieWorkflowActionName):
    logger.debug("Processing yarnJobId %s of workflow %s workflowId: %s ActionId:%s ActionName:%s" % (
    yarnJobId, oozieWorkflowName, oozieWorkflowId,
    oozieWorkflowActionId, oozieWorkflowActionName))
    job = yarnJobId
    app = yarnJobId.replace("job_", "application_")
    tasks_map = []
    tasks_reduce = []
    task_info = None
    yarnJobInfo = None
    job_info = get_job_info(job, oozieWorkflowName, oozieWorkflowId, oozieWorkflowActionId, oozieWorkflowActionName)
    if job_info:
        app_info = get_app_info(app, oozieWorkflowName, oozieWorkflowId, oozieWorkflowActionId, oozieWorkflowActionName)
        if use_rest_api:
            task_info = get_task_info(job, oozieWorkflowName, oozieWorkflowId, oozieWorkflowActionId,
                                      oozieWorkflowActionName)
            task_ids = get_task_ids_by_job(job)
            taskattempt_container_info = get_taskattempt_container_info(job, task_ids, oozieWorkflowName, oozieWorkflowId,
                                                                        oozieWorkflowActionId, oozieWorkflowActionName)
            if taskattempt_container_info:
                for task in taskattempt_container_info:
                    for task_attempt in task:
                        if task_attempt['type'] == 'MAP':
                            tasks_map.append(task_attempt)
                        elif task_attempt['type'] == 'REDUCE':
                            tasks_reduce.append(task_attempt)
        else:
            jhist_file = copy_to_local(job_id=job, job_finish_time=job_info['finishTime'])
            if jhist_file and os.path.exists(jhist_file):
                result = process_jhist(jhist_file, job_id=job, wfId=oozieWorkflowId, wfName=oozieWorkflowName,
                                       wfaId=oozieWorkflowActionId, wfaName=oozieWorkflowActionName)
                task_info = result['taskInfo'].values()
                tasks_map = result['tasksMap'].values()
                tasks_reduce = result['tasksReduce'].values()
            else:
                logger.error("Job history file for job {0} does not exist in {1}".format(job, jhist_file))
        try:
            yarnJobInfo = compute_job_stats(job_info, app_info, task_info, tasks_map, tasks_reduce, oozieWorkflowId,
                                        oozieWorkflowActionId, oozieWorkflowName, oozieWorkflowActionName)
        except Exception as e:
            logger.error("Failed to compute job stats for {0}".format(job))

    if yarnJobInfo: 
        return yarnJobInfo
    else:
        return None

def initialize_app():
    global pool
    args = parse_args_for_config()
    initialize_configuration(args.config)

    log_config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'loggingelastic.conf')
    configure_logger(log_config_file, logging_config['elasticWorkflows'])
    if not os.path.exists(os.path.abspath(jobhistory_copy_dir)):
        logger.debug("Directory {0} does not exist.Creating it".format(os.path.abspath(jobhistory_copy_dir)))
        if not mkdir_p(os.path.abspath(jobhistory_copy_dir)):
            logger.error("Failed to create jobhistory files copy dir:{0}. Exiting".format(jobhistory_copy_dir))
            exit(1)
        else:
            logger.debug("Directory {0} created successfully".format(os.path.abspath(jobhistory_copy_dir)))
    else:
        logger.debug("Directory {0} exist".format(os.path.abspath(jobhistory_copy_dir)))
        
                
    if kerberos["enabled"]:
        if kinit_tgt_for_user():
            kerberos_initialize()
        else:
            logging.error("Failed to kinit with kerberos enabled. Exiting")
            exit(1)
    if with_threading:
        pool = ThreadPool(THREAD_COUNT)
    if not use_rest_api:
        initialize_hdfs_client(hdfs['url'])

def run_application(index):
    global wfs_processed
    logger.info("Processing Elastic workflows start for iteration {0}".format(index + 1))
    iter_start_time = time.time()
    logger.info("Iteration start time {0}".format(iter_start_time))
    wfs_processed = 0
    read_unprocessed_workflows()
    iter_end_time = time.time()
    logger.info("Iteration end time {0}".format(iter_end_time))
    logger.info("Iteration duration {0}".format((iter_end_time - iter_start_time)))
    logger.info("Processing Elastic workflows end for iteration {0}".format(index + 1))
    handle_kerberos_error()
    handle_redis_error()


if __name__== "__main__":
    index = 0 
    initialize_app()
    while True:
        try:
            run_application(index)
            logger.info("Sleeping for {0} seconds".format(sleep_time_in_secs["elasticWorkFlows"]))
            time.sleep(sleep_time_in_secs["elasticWorkFlows"])
        except Exception:
            logger.exception("Received Exception")
        finally:
            index = index + 1

