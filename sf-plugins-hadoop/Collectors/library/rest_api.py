"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""

from library.http_request import *
from configuration import *
from library.utilities import *
import traceback


def get_job_info(job_id, wfName, wfId, wfaId, wfaName):
    location = job_history_server['host']
    port = job_history_server['port']
    path = "/ws/v1/history/mapreduce/jobs/{0}".format(job_id)
    json_resp = http_request(location, port, path, scheme=job_history_server['scheme'])
    if json_resp is None:
        return None
    job_info = json_resp['job']

    job_info['wfName'] = wfName
    job_info['wfId'] = wfId
    job_info['wfaId'] = wfaId
    job_info['wfaName'] = wfaName
    job_info['_plugin'] = plugin_name['oozie']
    job_info['_documentType'] = "jobStats"
    job_info['_tag_appName'] = tag_app_name['oozie']
    job_info['time'] = int(time.time())
    job_info['startTime'] = int(job_info['startTime'] / 1000) if job_info['startTime'] else -1
    job_info['finishTime'] = int(job_info['finishTime'] / 1000) if job_info['finishTime'] else -1
    job_info['endTime'] = job_info['finishTime']
    job_info['submitTime'] = int(job_info['submitTime'] / 1000) if job_info['submitTime'] else -1
    job_info['avgMapTime'] = int(job_info['avgMapTime'] / 1000)
    job_info['avgReduceTime'] = int(job_info['avgReduceTime'] / 1000)
    job_info['avgShuffleTime'] = int(job_info['avgShuffleTime'] / 1000)
    job_info['avgMergeTime'] = int(job_info['avgMergeTime'] / 1000)
    job_info['avgReduceTimeMs'] = (job_info['avgReduceTime'])
    job_info['avgShuffleTimeMs'] = (job_info['avgShuffleTime'])
    job_info['avgMergeTimeMs'] = (job_info['avgMergeTime'])

    job_info['elapsedTime'] = job_info['endTime'] - job_info['submitTime'] if job_info['endTime'] > 0 and job_info['submitTime'] > 0 else None
    job_info['runTime'] = job_info['endTime'] - job_info['startTime'] if job_info['endTime'] > 0 and job_info['startTime'] > 0 else None
    job_info['schedulingDelay'] = job_info['startTime'] - job_info['submitTime'] if job_info['startTime'] > 0 and job_info['submitTime']  > 0 else None

    job_info['jobId'] = job_info['id']

    path_counters = "/ws/v1/history/mapreduce/jobs/{0}/counters".format(job_id)

    json_resp_counters = http_request(location, port, path_counters, scheme=job_history_server['scheme'])
    if json_resp_counters is None:
        return None

    if json_resp_counters['jobCounters'].get('counterGroup') is not None:
        job_counters = json_resp_counters['jobCounters']['counterGroup']
        for jc in job_counters:
            counter_list = jc["counter"]
            for counter in counter_list:
                job_info[convert_camelcase(counter["name"], "_") + "Total"] = counter["totalCounterValue"]
                job_info[convert_camelcase(counter["name"], "_") + "Reduce"] = counter["reduceCounterValue"]
                job_info[convert_camelcase(counter["name"], "_") + "Map"] = counter["mapCounterValue"]

    return job_info


def get_job_counters(job_id):
    location = job_history_server['ip']
    port = job_history_server['port']
    path = "/ws/v1/history/mapreduce/jobs/{0}/counters".format(job_id)
    json_resp = http_request(location, port, path, scheme=job_history_server['scheme'])
    if json_resp is None:
        return None
    job_counters = json_resp['jobCounters']['counterGroup']

    job_counters_json = {}
    job_counters_json['_plugin'] = plugin_name['oozie']
    job_counters_json['_documentType'] = "jobStats"
    job_counters_json['_tag_appName'] = tag_app_name['oozie']
    job_counters_json['time'] = int(time.time())

    for jc in job_counters:
        counter_list = jc["counter"]
        for counter in counter_list:
            job_counters_json[convert_camelcase(counter["name"], "_") + "Total"] = counter["totalCounterValue"]
            job_counters_json[convert_camelcase(counter["name"], "_") + "Reduce"] = counter["reduceCounterValue"]
            job_counters_json[convert_camelcase(counter["name"], "_") + "Map"] = counter["mapCounterValue"]

    return job_counters_json


def get_task_info(job_id, wfName, wfId, wfaId, wfaName):
    location = job_history_server['host']
    port = job_history_server['port']
    path = '/ws/v1/history/mapreduce/jobs/{0}'.format(job_id)
    json_resp_ts = http_request(location, port, path, scheme=job_history_server['scheme'])
    if json_resp_ts is None:
        return None

    path = '/ws/v1/history/mapreduce/jobs/{0}/tasks'.format(job_id)
    json_resp_tasks = http_request(location, port, path, scheme=job_history_server['scheme'])
    if json_resp_tasks is None:
        return None
    task_list = json_resp_tasks['tasks']['task']
    task_document_list = []
    for task in task_list:
        task['wfName'] = wfName
        task['wfId'] = wfId
        task['wfaId'] = wfaId
        task['wfaName'] = wfaName
        task['_plugin'] = plugin_name['oozie']
        task['_documentType'] = "taskStats"
        task['jobId'] = job_id
        task['name'] = json_resp_ts['job']['name']
        task['_tag_appName'] = tag_app_name['oozie']
        task['taskId'] = task['id']
        task['time'] = int(time.time())
        task['submitTime'] = int(task['startTime'] / 1000)
        task['startTime'] = int(task['startTime'] / 1000)
        task['finishTime'] = int(task['finishTime'] / 1000)
        task['endTime'] = int(task['finishTime'] / 1000)
        task['elapsedTime'] = int(task['elapsedTime'] / 1000)
        task['runTime'] = task['endTime'] - task['startTime'] if task['endTime'] and task['startTime'] else None
        task['schedulingDelay'] = task['startTime'] - task['submitTime'] if task['startTime'] and task['submitTime'] else None
        task_document_list.append(task)

    return task_document_list


def get_app_info(app_id, wfName, wfId, wfaId, wfaName):
    location = timeline_server['host']
    port = timeline_server['port']
    path = '/ws/v1/applicationhistory/apps/{0}/'.format(app_id)
    app_info = http_request(location, port, path, scheme=timeline_server['scheme'])
    if app_info is None:
        return None

    app_info['wfName'] = wfName
    app_info['wfId'] = wfId
    app_info['wfaId'] = wfaId
    app_info['wfaName'] = wfaName
    app_info['_plugin'] = plugin_name['oozie']
    app_info['_documentType'] = "appStats"
    app_info['_tag_appName'] = tag_app_name['oozie']
    app_info['time'] = int(time.time())
    # Convert times to epoch seconds from ms
    app_info['startedTime'] = int(app_info['startedTime'] / 1000)
    app_info['finishedTime'] = int(app_info['finishedTime'] / 1000)
    app_info['elapsedTime'] = int(app_info['elapsedTime'] / 1000)
    app_info['submittedTime'] = int(app_info['submittedTime'] / 1000)

    return app_info



def get_task_ids_by_job(job_id):
    location = job_history_server['host']
    port = job_history_server['port']
    path = '/ws/v1/history/mapreduce/jobs/{0}/tasks'.format(job_id)
    json_resp_tasks = http_request(location, port, path, scheme=job_history_server['scheme'])
    if json_resp_tasks is None:
        return None
    task_list = json_resp_tasks['tasks']['task']
    task_id_list = []
    for task in task_list:
        task_id_list.append(task['id'])

    return task_id_list


def get_container_info(job_id):

    app_id = job_id.replace("job", "application")
    location = timeline_server['host']
    port = timeline_server['port']
    path = '/ws/v1/applicationhistory/apps/{0}/'.format(app_id)
    json_resp_ts = http_request(location, port, path, scheme=timeline_server['scheme'])
    if json_resp_ts is None:
        return None

    app_attempt = json_resp_ts['currentAppAttemptId']
    path = '/ws/v1/applicationhistory/apps/{0}/appattempts/'.format(app_id)
    app_attempts = http_request(location, port, path, scheme=timeline_server['scheme'])
    container_attempt = []
    for attempt in app_attempts['appAttempt']:
        path = '/ws/v1/applicationhistory/apps/{0}/appattempts/{1}/containers'.format(app_id, attempt['appAttemptId'])
        json_resp_containers = http_request(location, port, path, scheme=timeline_server['scheme'])
        if json_resp_containers is None:
            return None
        container_list = json_resp_containers['container']
        container_attempt.append(container_list)

    containers_flat = [l for k in container_attempt for l in k]

    return containers_flat


def get_taskattempt_container_info(job_id, task_ids, wfName, wfId, wfaId, wfaName):

    try:
        if task_ids is None:
            return None
        location = job_history_server['host']
        port = job_history_server['port']

        #containers_list = get_container_info(job_id)
        containers_list = []
        task_attempt_document_job = []
        for task in task_ids:
            path = '/ws/v1/history/mapreduce/jobs/{0}/tasks/{1}/attempts'.format(job_id, task)
            json_resp_tasks = http_request(location, port, path, scheme=job_history_server['scheme'])
            if json_resp_tasks is None:
                return None
            task_attempt_list = json_resp_tasks['taskAttempts']['taskAttempt']
            task_attempt_document = []
            for task_attempt in task_attempt_list:
                task_attempt['wfName'] = wfName
                task_attempt['wfId'] = wfId
                task_attempt['wfaId'] = wfaId
                task_attempt['wfaName'] = wfaName
                task_attempt['_plugin'] = plugin_name['oozie']
                task_attempt['_documentType'] = 'taskAttemptStat'
                task_attempt['_tag_appName'] = tag_app_name['oozie']
                task_attempt['jobId'] = job_id
                task_attempt['taskId'] = task

                task_attempt['time'] = int(time.time())
                task_attempt['submitTime'] = int(task_attempt['startTime'] / 1000) if task_attempt['startTime'] else -1
                task_attempt['startTime'] = int(task_attempt['startTime'] / 1000) if task_attempt['startTime'] else -1
                task_attempt['finishTime'] = int(task_attempt['finishTime'] / 1000) if task_attempt['finishTime'] else -1
                task_attempt['endTime'] = task_attempt['finishTime']
                task_attempt['elapsedTime'] = int(task_attempt['elapsedTime'] / 1000) if task_attempt['elapsedTime'] else -1
                task_attempt['runTime'] = task_attempt['endTime'] - task_attempt['startTime'] if task_attempt['endTime'] > 0 and task_attempt['startTime'] > 0 else -1
                task_attempt['schedulingDelay'] = task_attempt['startTime'] - task_attempt['submitTime'] if task_attempt['submitTime'] > 0 and \
                                                                                                 task_attempt['startTime'] > 0 else -1
                task_attempt['taskAttemptId'] = task_attempt['id']
                task_attempt['containerId'] = task_attempt.pop('assignedContainerId')
                if 'shuffleFinishTime' in task_attempt:
                    task_attempt['shuffleFinishTime'] = int(task_attempt['shuffleFinishTime'] / 1000)
                if 'mergeFinishTime' in task_attempt:
                    task_attempt['mergeFinishTime'] = int(task_attempt['mergeFinishTime'] / 1000)
                if 'elapsedShuffleTime' in task_attempt:
                    task_attempt['elapsedShuffleTime'] = int(task_attempt['elapsedShuffleTime'] / 1000)
                if 'elapsedMergeTime' in task_attempt:
                    task_attempt['elapsedMergeTime'] = int(task_attempt['elapsedMergeTime'] / 1000)
                if 'elapsedReduceTime' in task_attempt:
                    task_attempt['elapsedReduceTime'] = int(task_attempt['elapsedReduceTime'] / 1000)

                # Find the container from container app list and merge
                for container in containers_list:
                    if container['containerId'] == task_attempt['containerId']:
                        task_attempt['allocatedMB'] = container['allocatedMB']
                        task_attempt['allocatedVCores'] = container['allocatedVCores']
                        break

                # Merge the counters document

                path = '/ws/v1/history/mapreduce/jobs/{0}/tasks/{1}/attempts/{2}/counters'.format(job_id, task,
                                                                                            task_attempt['taskAttemptId'])
                json_resp_tasks1 = http_request(location, port, path, scheme=job_history_server['scheme'])
                task_attempt_counter_group = json_resp_tasks1['jobTaskAttemptCounters']["taskAttemptCounterGroup"]
                task_attempt_counter = {}
                for group in task_attempt_counter_group:
                    counter_list = group["counter"]
                    for counter in counter_list:
                        task_attempt_counter[convert_camelcase(counter["name"], "_")] = counter["value"]

                task_attempt.update(task_attempt_counter)
                if task_attempt['nodeHttpAddress']:
                    task_attempt['nodeHttpAddress'] = task_attempt['nodeHttpAddress'].split(":")[0]

                task_attempt_document.append(task_attempt)

            task_attempt_document_job.append(task_attempt_document)

        return task_attempt_document_job
    except Exception as e:
        logger.debug('Unable to get task details => ' + traceback.format_exc().splitlines()[-1])
        return None


def get_num_node_to_tasks(job_id, task_list):

    node_to_tasks = {"jobId": job_id, "tasksTotal": len(task_list)}
    for task in task_list:
        job_type = task["type"]
        node = task["nodeHttpAddress"]
        # node_to_tasks[node] = {"maps":0, "reduces":0}
        if job_type == "MAP":
            if node not in node_to_tasks:
                node_to_tasks[node] = {'maps': 1, "reduces": 0}
            else:
                node_to_tasks[node]['maps'] += 1
        elif job_type == "REDUCE":
            if node not in node_to_tasks:
                node_to_tasks[node] = {'maps': 0, "reduces": 1}
            else:
                node_to_tasks[node]['reduces'] += 1

    return node_to_tasks


def group_tasks_by_start_time(job_id, task_list):

    tasks_by_start_time = {"jobId": job_id}

    for task in task_list:
        job_type = task["type"]
        if job_type == "MAP":
            if task["startTime"] not in tasks_by_start_time:
                tasks_by_start_time[task["startTime"]] = {"maps": 1, "reduces": 0}
            else:
                tasks_by_start_time[task["startTime"]]["maps"] += 1
        elif job_type == "REDUCE":
            if task["startTime"] not in tasks_by_start_time:
                tasks_by_start_time[task["startTime"]] = {"maps": 0, "reduces": 1}
            else:
                tasks_by_start_time[task["startTime"]]["reduces"] += 1

    return tasks_by_start_time


# SPARK APIs

def get_app_details(app_details):

    #location = spark2_history_server['host']
    #port = spark2_history_server['port']
    #scheme = spark2_history_server['scheme']

    #path = '/api/v1/applications/{}'.format(app)
    #app_details = http_request(location, port, path, scheme=scheme)
    #if app_details is None:
    #    return None

    app_doc = []
    for attempt in app_details['attempts']:
        attempt['appId'] = app_details['id']
        attempt['appName'] = app_details['name']
        if 'attemptId' not in attempt:
            attempt['appAttemptId'] = 0
        else:
            attempt['appAttemptId'] = attempt.pop('attemptId')
        attempt['startTime'] = int(attempt['startTimeEpoch'])/1000
        attempt['submitTime'] = attempt['startTime']
        attempt['endTime'] = int(attempt['endTimeEpoch'])/ 1000
        attempt['elapsedTime'] = attempt['endTime'] - attempt['submitTime']
        attempt['runTime'] = attempt['endTime'] - attempt['startTime']
        attempt['schedulingDelay'] = attempt['startTime'] - attempt['submitTime']
        attempt['lastUpdated'] = int(attempt['lastUpdatedEpoch']) / 1000
        attempt.pop('startTimeEpoch')
        attempt.pop('lastUpdatedEpoch')
        attempt['duration'] = attempt['duration']
        attempt['_documentType'] = 'sparkApp'
        attempt['_tag_appName'] = tag_app_name['spark']
        attempt['_plugin'] = plugin_name['spark']
        attempt['time'] = int(time.time())
        app_doc.append(attempt)

    return app_doc


def get_job_details(app, name, attempt_id):
    location = spark2_history_server.get('host')
    port = spark2_history_server.get('port')

    if attempt_id == 0:
        path = '/api/v1/applications/{}/jobs'.format(app)
    else:
        path = '/api/v1/applications/{}/{}/jobs'.format(app, attempt_id)
    job_details = http_request(location, port, path, scheme=spark2_history_server.get('scheme'))
    if job_details is None:
        return None

    for job in job_details:
        job['appId'] = app
        job['appAttemptId'] = attempt_id
        job['appName'] = name
        job['jobName'] = job.pop('name')
        job['_documentType'] = 'sparkJobs'
        job['_tag_appName'] = tag_app_name['spark']
        job['_plugin'] = plugin_name['spark']
        job['submitTime'] = convert_to_epoch(job['submissionTime'])
        job['startTime'] = convert_to_epoch(job['submissionTime'])
        job['endTime'] = convert_to_epoch(job['completionTime'])
        job['elapsedTime'] = job['endTime'] - job['submitTime']
        job['runTime'] = job['endTime'] - job['startTime']
        job['schedulingDelay'] = job['startTime'] - job['submitTime']
        job['time'] = int(time.time())

    return job_details


def get_executors(app, name, attempt_id):
    location = spark2_history_server['host']
    port = spark2_history_server['port']
    scheme = spark2_history_server['scheme']

    if attempt_id == 0:
        path = '/api/v1/applications/{}/allexecutors'.format(app)
    else:
        path = '/api/v1/applications/{}/{}/allexecutors'.format(app, attempt_id)

    executors = http_request(location, port, path, scheme=scheme)

    if executors is None:
        return None

    def update(e):
        #logger.debug("The value of e: {0}".format(e))
        if 'memoryMetrics' in e:
            for m in e['memoryMetrics']:
                e[m] = e['memoryMetrics'][m]
            e.pop('memoryMetrics')
        for m in e['executorLogs']:
            new_key = 'executorLogs' + m.capitalize()
            e[new_key] = e['executorLogs'][m]
        e.pop('executorLogs')
        e['appId'] = app
        e['appAttemptId'] = attempt_id
        e['executorId'] = e.pop('id')
        e['_documentType'] = 'sparkExecutors'
        e['_tag_appName'] = tag_app_name['spark']
        e['_plugin'] = plugin_name['spark']
        if 'addTime' in e:
            e['addTime'] = convert_to_epoch(e['addTime'])
        e['time'] = int(time.time())
        e['appName'] = name
        e['totalDuration'] = e['totalDuration']
        e['totalGCTime'] = e['totalGCTime']

    [update(e) for e in executors]
    return executors


def get_stages(app, name, attempt_id):

    location = spark2_history_server['host']
    port = spark2_history_server['port']
    scheme = spark2_history_server['scheme']
    if attempt_id == 0:
        path = '/api/v1/applications/{}/stages'.format(app)
    else:
        path = '/api/v1/applications/{}/{}/stages'.format(app, attempt_id)

    all_stages_json = http_request(location, port, path, scheme=scheme)
    if all_stages_json is None:
        return None
    #logger.debug("All stages: {0}".format(all_stages_json))

    def updates(d):
        d['appId'] = app
        d['appName'] = name
        d['appAttemptId'] = attempt_id
        d['stageAttemptId'] = d.pop('attemptId')
        d['_documentType'] = 'sparkStages'
        d['_tag_appName'] = tag_app_name['spark']
        d['_plugin'] = plugin_name['spark']
        d['time'] = int(time.time())
        metrics = d['accumulatorUpdates']
        for m in metrics:
            old_name = m['name']
            #logger.debug(metrics)
            if '.' in old_name:
                new_name = old_name.split(".")[2] + "".join(x[0].capitalize()+x[1::] for x in old_name.split(".")[3::])
                if isInt(m['value']):
                    d[new_name] = int(m['value'])
                elif isFloat(m['value']):
                    d[new_name] = float(m['value'])
                else:
                    d[new_name] = m['value']
            else: #TODO The metrics name does not contain . and is repeated multiple times 
                continue
        d.pop('accumulatorUpdates', None)
        if d['status'] == "COMPLETE" or d['status'] == "FAILED":
            d['submissionTime'] = convert_to_epoch(d['submissionTime']) if 'submissionTime' in d else convert_to_epoch(d['completionTime'])
            d['firstTaskLaunchedTime'] = convert_to_epoch(d['firstTaskLaunchedTime']) if 'firstTaskLaunchedTime' in d else convert_to_epoch(d['completionTime'])
            d['completionTime'] = convert_to_epoch(d['completionTime'])
            d['runTime'] = d['completionTime'] - d['firstTaskLaunchedTime']
            d['elapsedTime'] = d['completionTime'] - d['submissionTime']
            d['schedulingDelay'] = d['firstTaskLaunchedTime'] - d['submissionTime']
            d['executorRunTime'] = int(d['executorRunTime']) if 'executorRunTime' in d else 0
        d['stageName'] = d.pop('name')


    [updates(d) for d in all_stages_json]

    return all_stages_json


def get_stage_attempt_ids(app, app_attempt):

    location = spark2_history_server['host']
    port = spark2_history_server['port']
    scheme = spark2_history_server['scheme']
    if app_attempt == 0:
        path = '/api/v1/applications/{}/stages'.format(app)
    else:
        path = '/api/v1/applications/{}/{}/stages'.format(app, app_attempt)

    all_stages_json = http_request(location, port, path, scheme=scheme)
    if all_stages_json is None:
        return None
    stage_ids = [{'stageId': s['stageId'], 'stageAttemptId': s['attemptId'], 'numTasks' : s['numTasks'] if 'numTasks' in s else s['numCompleteTasks'] + s['numFailedTasks'] } for s in all_stages_json if 'status' in s and s['status'] == "COMPLETE" ]
    return stage_ids


def get_tasks_per_stage(app, name, attempt_id):

    location = spark2_history_server['host']
    port = spark2_history_server['port']
    scheme = spark2_history_server['scheme']

    result = []

    stage = get_stage_attempt_ids(app, attempt_id)
    if stage is None:
        return None
    for s in stage:
        if s['numTasks'] > 0:
            if attempt_id == 0:
                path = '/api/v1/applications/{}/stages/{}/{}/taskList?offset=0&length={}'.format(app, s['stageId'], s['stageAttemptId'], s['numTasks'])
            else:
                path = '/api/v1/applications/{}/{}/stages/{}/{}/taskList?offset=0&length={}'.format(app, attempt_id, s['stageId'], s['stageAttemptId'], s['numTasks'])
        else:
            continue

        tasks_json = http_request(location, port, path, scheme=scheme)
        if tasks_json is None:
            return None

        def updates(task):
            task['appId'] = app
            task['appAttemptId'] = attempt_id
            task['appName'] = name
            task['stageAttemptId'] = s['stageAttemptId']
            task['stageId'] = s['stageId']
            task['sparkTaskId'] = int(task.pop('taskId'))
            if 'taskMetrics' in task:
                metrics = task['taskMetrics']
                for m in metrics:
                    if isinstance(metrics[m], dict):
                        #flatten metrics
                        for k in metrics[m]:
                            new_key = m.replace("Metrics", "") + k[0].capitalize()+k[1::]
                            task[new_key] = metrics[m][k]
                    else:
                        task[m] = metrics[m]
                task.pop('taskMetrics')
            task.pop('accumulatorUpdates')
            task['_documentType'] = 'sparkTasks'
            task['_tag_appName'] = tag_app_name['spark']
            task['_plugin'] = plugin_name['spark']
            task['launchTime'] = convert_to_epoch(task['launchTime'])

            #if 'duration' in task:
            #    task['endTime'] = task['launchTime'] + int(task['duration']/1000)

            if 'executorRunTime' in task:
                task['executorRunTime'] = task['executorRunTime']
            else:
                task['executorRunTime'] = 0
            task['endTime'] = task['launchTime'] + int(task['executorRunTime'] / 1000)
            task['runTime'] = task['endTime'] - task['launchTime']
            task['elapsedTime'] = task['endTime'] - task['launchTime']
            task['schedulingDelay'] = 0
            task['time'] = int(time.time())

        [updates(t) for t in tasks_json]
        result += tasks_json
    return result

# YARN, Name Node apis


def find_diff(doc_current, fields_for_diff, file):

    def set_value_to_zero(doc, doc_type):
        for dt in doc_type:
            if fields_for_diff.get(dt) != None:
                for field in fields_for_diff[dt]:
                    doc[field] = 0

    def set_diff_values(doc_c, doc_p, doc_type):
        if fields_for_diff.get(doc_type) != None:
                for field in fields_for_diff.get(doc_type):
                    doc_c[field] = doc_c[field] - doc_p[field]

    def find_doc_of_type(doc, doc_type):
        for d in doc:
            if d['_documentType'] == doc_type:
                return d

    if os.path.isfile(os.path.abspath(file)):
        with open(file) as afd:
            doc_previous_file = afd.readlines()
    else:
        with open(os.path.abspath(file), "w") as pfd:
           for doc in doc_current:
              pfd.write(json.dumps(doc))
              pfd.write("\n")
        for doc_c in doc_current:
            set_value_to_zero(doc_c, fields_for_diff.keys())
        return

    doc_previous = []
    for doc in doc_previous_file:
         doc_previous.append(json.loads(doc))

    with open(os.path.abspath(file), "w") as pfd:
        for doc in doc_current:
           pfd.write(json.dumps(doc))
           pfd.write("\n")

    for doc_c in doc_current:
        doc_type = doc_c['_documentType']
        doc_p = find_doc_of_type(doc_previous, doc_type)
        if doc_p == None:
           return None
        time_diff = doc_c['time'] - doc_p['time']
        if time_diff > (yarn_stats_time_interval * 3):
            set_value_to_zero(doc_c, fields_for_diff.keys())
        else:
            set_diff_values(doc_c, doc_p, doc_type)


def get_active_nn(name_node_list):

    for nn in name_node_list:
        location = nn
        port = name_node['port']
        path = "/jmx?qry=Hadoop:service=NameNode,name={}".format('NameNodeStatus')
        json_doc = http_request(location, port, path, scheme=name_node['scheme'])

        try:
           if json_doc == None:
               continue
           if json_doc.get('beans') == []:
               continue
        except KeyError as e:
           continue

        if json_doc['beans'][0]['State'] == 'active':
            return nn

    return None


def get_active_resource_manager(resource_manager_list):

    for nn in resource_manager_list:
        location = nn
        port = resource_manager['port']
        path = "/ws/v1/cluster/info"
        res = http_request(location, port, path, scheme=name_node['scheme'])

        try:
           if res == None:
               continue
           if res['clusterInfo']['haState'] == 'ACTIVE':
               return nn
        except KeyError as e:
           continue

    return None




