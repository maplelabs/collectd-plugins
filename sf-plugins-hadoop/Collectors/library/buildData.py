from utilities import *
import time
import json
from configuration import *
import logging

logger = logging.getLogger(__name__)

def prepare_workflow(workflow):
    result = {
        "wfName": workflow['appName'] if 'appName' in workflow else workflow['wfName'],
        "wfId": workflow['id'] if 'id' in workflow else workflow['wfId'] ,
        "id": workflow['id'] if 'id' in workflow else workflow['wfId'] ,
        "lastModTime": get_unix_timestamp(workflow['lastModTime']) if workflow['lastModTime'] is not None else None,
        "submitTime": get_unix_timestamp(workflow['createdTime']) if workflow['createdTime'] is not None else None,
        "createdTime": get_unix_timestamp(workflow['createdTime']) if workflow['createdTime'] is not None else None,
        "startTime": get_unix_timestamp(workflow['startTime']) if workflow['startTime'] is not None else None,
        "endTime": get_unix_timestamp(workflow['endTime']) if workflow['endTime'] is not None else None,
        "status": workflow['status'],
        "time": int(math.floor(time.time())),
        "_plugin": plugin_name['oozie'],
        "_documentType": "oozieWorkflows",
        "_tag_appName": tag_app_name['oozie'],
        "wfSchedulingDelay":  workflow['wfSchedulingDelay'] if "wfSchedulingDelay" in workflow else None,
        "jobSchedulingDelay": workflow['jobSchedulingDelay'] if "jobSchedulingDelay" in workflow else None,
        "hdfsBytesReadTotal": workflow['hdfsBytesReadTotal'] if "hdfsBytesReadTotal" in workflow else None,
        "waitTimeTotal":      workflow['waitTimeTotal'] if "waitTimeTotal" in workflow else None,
        "cpuMillisecondsTotal":  workflow['cpuMillisecondsTotal'] if "cpuMillisecondsTotal" in workflow else None,
        "mapsTotal":         workflow['mapsTotal'] if "mapsTotal" in workflow else None,
        "reducesTotal":      workflow['reducesTotal'] if "reducesTotal" in workflow else None,
        "weightedAvgMapTime":workflow['weightedAvgMapTime'] if "weightedAvgMapTime" in workflow else None,
        "weightedAvgReduceTime": workflow['weightedAvgReduceTime'] if "weightedAvgReduceTime" in workflow else None,
        "weightedAvgShuffleTime": workflow['weightedAvgShuffleTime'] if "weightedAvgShuffleTime" in workflow else None,
        "weightedAvgMergeTime": workflow['weightedAvgMergeTime'] if "weightedAvgMergeTime" in workflow else None,
        "killedReduceAttempts" : workflow['killedReduceAttempts'] if "killedReduceAttempts" in workflow else None,
        "killedMapAttempts": workflow['killedMapAttempts'] if "killedMapAttempts" in workflow else None,
        "offSwitchHdfsBytesReadTotal": workflow['offSwitchHdfsBytesReadTotal'] if "offSwitchHdfsBytesReadTotal" in workflow else None,
        "rackLocalHdfsBytesReadTotal": workflow['rackLocalHdfsBytesReadTotal'] if "rackLocalHdfsBytesReadTotal" in workflow else None,
        "nodeLocalHdfsBytesReadTotal": workflow['nodeLocalHdfsBytesReadTotal'] if "nodeLocalHdfsBytesReadTotal" in workflow else None,
        "workflowMonitorStatus": "init"
    }

    result['elapsedTime'] = result['endTime'] - result['submitTime'] if result['endTime'] and result['submitTime'] else None
    result['runTime'] = result['endTime'] - result['startTime'] if result['endTime'] and result['startTime'] else None
    result['schedulingDelay'] = result['startTime'] - result['submitTime'] if result['startTime'] and result['submitTime'] else None

    return result

def build_spark_apps_status(last_processed_app_id,
                            last_processed_end_time,
                            pending_apps):
    if last_processed_end_time:
        return {
            "lastProcessedApplicationId": last_processed_app_id,
            "lastProcessedEndTime": last_processed_end_time,
            "pendingApps": json.dumps(pending_apps) if pending_apps else "",
            "time": int(math.floor(time.time())),
            "type": "sparkStatus"
        }
    else:
        return {
            "pendingApps": json.dumps(pending_apps) if pending_apps else "",
            "time": int(math.floor(time.time())),
        }

def prepare_workflow_monitor(success, document=None):
    data = None
    if success and document is not None:
        logger.debug("Status Document : {0}".format(document))
        if 'createdTime' in document:
            data = {
                "lastProcessedCreatedTime": get_unix_timestamp(document['createdTime']),
                "lastProcessWorkflowId": document['id'],
                "runStatus": "success",
                "erroString": " ",
                "lastRunTime": int(math.floor(time.time())),
                "type": "oozieStatus"
            }
        else:
            document["lastRunTime"] = int(math.floor(time.time()))
            data = document
    return data

def generate_bulk_data(document_list, type):
    document_str = ""
    for document in document_list:
        if type == "workflow":
            document_str = document_str + '{"index": {}}\n' + json.dumps(prepare_workflow(document)) + '\n'
        else:
            document_str = document_str + '{"index": {}}\n' + json.dumps(document) + '\n'

    return document_str

def build_bulk_spark_attempt_data(attempt, stages, executors, jobs):
    result = ""
    if attempt:
        result += generate_bulk_data(attempt, "spark")
    if stages:
        result += generate_bulk_data(stages, "spark")
    if executors:
        result += generate_bulk_data(executors, "spark")
    if jobs:
        result += generate_bulk_data(jobs, "spark")
    return result

def build_spark_task_data(tasks):
    result = ""
    if tasks:
        result += generate_bulk_data(tasks, "spark")
    return result

def build_spark_taskpoints(taskpoints):
    result = ""
    if taskpoints:
        result += generate_bulk_data(taskpoints, "spark")
    return result

def prepare_workflow_action_data(action, workflowId, workflowName):
    result =  {
        "wfId": workflowId,
        "id": action['id'],
        "wfName": workflowName,
        "wfaName": action['name'],
        "wfaId": action['id'],
        "submitDelay": action['submitDelay'] if 'submitDelay' in action else None,
        "jobDelay": action['jobDelay'] if 'jobDelay' in action else None,
        "submitTime": get_unix_timestamp(action['startTime']) if action['startTime'] is not None else -1,
        "startTime": get_unix_timestamp(action['startTime']) if action['startTime'] is not None else -1,
        "endTime": get_unix_timestamp(action['endTime']) if action['endTime'] is not None else -1,
        "externalId": action['externalId'],
        "externalChildID": action['externalChildIDs'],
        "status": action['status'],
        "externalStatus": action['externalStatus'],
        "errorCode": action["errorCode"],
        "type": action['type'],
        "time": int(math.floor(time.time())),
        "retries": action['retries'],
        "waitTimeTotal": action['waitTimeTotal'] if "waitTimeTotal" in action else None,
        "cpuMillisecondsTotal": action['cpuMillisecondsTotal'] if "cpuMillisecondsTotal" in action else None,
        "mapsTotal": action['mapsTotal'] if "mapsTotal" in action else None,
        "reducesTotal": action['reducesTotal'] if "reducesTotal" in action else None,
        "weightedAvgMapTime": action['weightedAvgMapTime'] if "weightedAvgMapTime" in action else None,
        "weightedAvgReduceTime": action['weightedAvgReduceTime'] if "weightedAvgReduceTime" in action else None,
        "weightedAvgShuffleTime": action['weightedAvgShuffleTime'] if "weightedAvgShuffleTime" in action else None,
        "weightedAvgMergeTime": action['weightedAvgMergeTime'] if "weightedAvgMergeTime" in action else None,
        "killedReduceAttempts": action['killedReduceAttempts'] if "killedReduceAttempts" in action else None,
        "killedMapAttempts": action['killedMapAttempts'] if "killedMapAttempts" in action else None,
        "offSwitchHdfsBytesReadTotal": action['offSwitchHdfsBytesReadTotal'] if "offSwitchHdfsBytesReadTotal" in action else None,
        "rackLocalHdfsBytesReadTotal": action['rackLocalHdfsBytesReadTotal'] if "rackLocalHdfsBytesReadTotal" in action else None,
        "nodeLocalHdfsBytesReadTotal": action['nodeLocalHdfsBytesReadTotal'] if "nodeLocalHdfsBytesReadTotal" in action else None,
        "_plugin": plugin_name['oozie'],
        "_documentType": "oozieWorkflowActions",
        "_tag_appName": tag_app_name['oozie']
    }

    result['elapsedTime'] = result['endTime'] - result['submitTime'] if result['endTime'] > 0 and result['submitTime'] > 0 else None
    result['runTime'] = result['endTime'] - result['startTime'] if result['endTime'] > 0 and result['startTime'] > 0 else None
    result['schedulingDelay'] = result['startTime'] - result['submitTime'] if result['startTime'] > 0 and result['submitTime']  > 0 else None

    return result

def build_bulk_data_for_workflow(workflowData, post_data):
    document_str = ""
    #logger.debug("Post Data %s" % post_data)
    if workflowData is not None:
        workflowUpdatedInfo = prepare_workflow(workflowData)
        workflowUpdatedInfo['workflowMonitorStatus'] = "processed"
        if post_data['workflow'] is not None and post_data['workflow']['_source'] is not None:
            document_str = document_str + '{"update": {"_id": "' + post_data["workflow"]["_id"] + '"}}\n' + \
                           json.dumps({"doc": workflowUpdatedInfo}) + '\n'

            for workflowAction in post_data['workflowActions']:
                document_str = document_str + '{"index": {}}\n' + json.dumps(workflowAction['action']) + '\n'
                for yarnJobInfo in workflowAction['yarnJobs']:
                    if yarnJobInfo['job'] is not None:
                        document_str = document_str + '{"index": {}}\n' + json.dumps(yarnJobInfo['job']) + '\n'

                    if yarnJobInfo['appInfo'] is not None:
                        document_str = document_str + '{"index": {}}\n' + json.dumps(yarnJobInfo['appInfo']) + '\n'

            return document_str

def prepare_task_stats_by_timepoint(tp_start, tp_end, map_count, reduce_count, job_id, wfaId, wfId, wfName, wfaName):
    return {
        "wfId": wfId,
        "wfaId": wfaId,
        "wfName": wfName,
        "wfaName": wfaName,
        "time": int(math.floor(time.time())),
        "jobId": job_id,
        'timePeriodStart': tp_start,
        'timePeriodEnd': tp_end,
        "mapTaskCount": map_count,
        "reduceTaskCount": reduce_count,
        'duration': tp_end - tp_start,
        "_plugin": plugin_name['oozie'],
        "_documentType": "taskCounts",
        "_tag_appName": tag_app_name['oozie']
    }

def prepare_spark_task_stats_by_timepoint(tp_start, tp_end, task_count, appId, app_attempt_id,  stageId,
                                          stageAttemptId, appName):
    return {
        "appId": appId,
        "appAttemptId": app_attempt_id,
        "stageId": stageId,
        "stageAttemptId": stageAttemptId,
        "appName": appName,
        "time": int(math.floor(time.time())),
        'timePeriodStart': tp_start,
        'timePeriodEnd': tp_end,
        "taskCount": task_count,
        'duration': tp_end - tp_start,
        "_plugin": plugin_name['spark'],
        "_documentType": "sparkTaskCounts",
        "_tag_appName": tag_app_name['spark']
    }
