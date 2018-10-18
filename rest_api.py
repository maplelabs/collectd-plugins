from http_request import *
import time
from utilities import *
import traceback


def get_job_info(host, port, job_id, wfName, wfId, wfaId, wfaName):

    location = host
    port = port
    path = "/ws/v1/history/mapreduce/jobs/{0}".format(job_id)
    json_resp = http_request(location, port, path, scheme='http')
    if json_resp is None:
        return None
    job_info = json_resp['job']

    job_info['wfName'] = wfName
    job_info['wfId'] = wfId
    job_info['wfaId'] = wfaId
    job_info['wfaName'] = wfaName
    job_info['_plugin'] = "oozie"
    job_info['_documentType'] = "jobStats"
    job_info['_tag_appName'] = "oozie"
    job_info['time'] = int(time.time())
    job_info['startTime'] = int(job_info['startTime'] / 1000)
    job_info['finishTime'] = int(job_info['finishTime'] / 1000)
    job_info['submitTime'] = int(job_info['submitTime'] / 1000)
    job_info['avgMapTime'] = int(job_info['avgMapTime'] / 1000)
    job_info['avgReduceTime'] = int(job_info['avgReduceTime'] / 1000)
    job_info['avgShuffleTime'] = int(job_info['avgShuffleTime'] / 1000)
    job_info['avgMergeTime'] = int(job_info['avgMergeTime'] / 1000)

    job_info['jobId'] = job_info.pop('id')

    path_counters = "/ws/v1/history/mapreduce/jobs/{0}/counters".format(job_id)

    json_resp_counters = http_request(location, port, path_counters, scheme='http')
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


def get_job_counters(host, port, job_id):
    location = host
    port = port
    path = "/ws/v1/history/mapreduce/jobs/{0}/counters".format(job_id)
    json_resp = http_request(location, port, path, scheme='http')
    if json_resp is None:
        return None
    job_counters = json_resp['jobCounters']['counterGroup']

    job_counters_json = {}
    job_counters_json['_plugin'] = "oozie"
    job_counters_json['_documentType'] = "jobStats"
    job_counters_json['_tag_appName'] = "oozie"
    job_counters_json['time'] = int(time.time())

    for jc in job_counters:
        counter_list = jc["counter"]
        for counter in counter_list:
            job_counters_json[convert_camelcase(counter["name"], "_") + "Total"] = counter["totalCounterValue"]
            job_counters_json[convert_camelcase(counter["name"], "_") + "Reduce"] = counter["reduceCounterValue"]
            job_counters_json[convert_camelcase(counter["name"], "_") + "Map"] = counter["mapCounterValue"]

    return job_counters_json


def get_task_info(host, port, job_id, wfName, wfId, wfaId, wfaName):
    location = host
    port = port
    path = '/ws/v1/history/mapreduce/jobs/{0}'.format(job_id)
    json_resp_ts = http_request(location, port, path, scheme='http')
    if json_resp_ts is None:
        return None

    path = '/ws/v1/history/mapreduce/jobs/{0}/tasks'.format(job_id)
    json_resp_tasks = http_request(location, port, path, scheme='http')
    if json_resp_tasks is None:
        return None
    task_list = json_resp_tasks['tasks']['task']
    task_document_list = []
    for task in task_list:
        task['wfName'] = wfName
        task['wfId'] = wfId
        task['wfaId'] = wfaId
        task['wfaName'] = wfaName
        task['_plugin'] = "oozie"
        task['_documentType'] = "taskStats"
        task['jobId'] = job_id
        task['name'] = json_resp_ts['job']['name']
        task['_tag_appName'] = "oozie"
        task['taskId'] = task.pop('id')
        task['time'] = int(time.time())
        task['startTime'] = int(task['startTime'] / 1000)
        task['finishTime'] = int(task['finishTime'] / 1000)
        task['elapsedTime'] = int(task['elapsedTime'] / 1000)
        task_document_list.append(task)

    return task_document_list


def get_app_info(host, port,app_id, wfName, wfId, wfaId, wfaName):
    location = host
    port = port
    path = '/ws/v1/applicationhistory/apps/{0}/'.format(app_id)
    app_info = http_request(location, port, path, scheme='http')
    if app_info is None:
        return None

    app_info['wfName'] = wfName
    app_info['wfId'] = wfId
    app_info['wfaId'] = wfaId
    app_info['wfaName'] = wfaName
    app_info['_plugin'] = "oozie"
    app_info['_documentType'] = "appStats"
    app_info['_tag_appName'] = "oozie"
    app_info['time'] = int(time.time())
    # Convert times to epoch seconds from ms
    app_info['startedTime'] = int(app_info['startedTime'] / 1000)
    app_info['finishedTime'] = int(app_info['finishedTime'] / 1000)
    app_info['elapsedTime'] = int(app_info['elapsedTime'] / 1000)
    app_info['submittedTime'] = int(app_info['submittedTime'] / 1000)

    return app_info



def get_task_ids_by_job(host, port, job_id):
    location = host
    port = port
    path = '/ws/v1/history/mapreduce/jobs/{0}/tasks'.format(job_id)
    json_resp_tasks = http_request(location, port, path, scheme='http')
    if json_resp_tasks is None:
        return None
    task_list = json_resp_tasks['tasks']['task']
    task_id_list = []
    for task in task_list:
        task_id_list.append(task['id'])

    return task_id_list


def get_container_info(host, port, job_id):

    app_id = job_id.replace("job", "application")
    location = host
    port = port
    path = '/ws/v1/applicationhistory/apps/{0}/'.format(app_id)
    json_resp_ts = http_request(location, port, path, scheme='http')
    if json_resp_ts is None:
        return None

    app_attempt = json_resp_ts['currentAppAttemptId']
    path = '/ws/v1/applicationhistory/apps/{0}/appattempts/'.format(app_id)
    app_attempts = http_request(location, port, path, scheme='http')
    container_attempt = []
    for attempt in app_attempts['appAttempt']:
        path = '/ws/v1/applicationhistory/apps/{0}/appattempts/{1}/containers'.format(app_id, attempt['appAttemptId'])
        json_resp_containers = http_request(location, port, path, scheme='http')
        if json_resp_containers is None:
            return None
        container_list = json_resp_containers['container']
        container_attempt.append(container_list)

    containers_flat = [l for k in container_attempt for l in k]

    return containers_flat


def get_taskattempt_container_info(host, port, job_id, task_ids, wfName, wfId, wfaId, wfaName):

    try:
        if task_ids is None:
            return None
        location = host
        port = port

        #containers_list = get_container_info(job_id)
        containers_list = []
        task_attempt_document_job = []
        for task in task_ids:
            path = '/ws/v1/history/mapreduce/jobs/{0}/tasks/{1}/attempts'.format(job_id, task)
            json_resp_tasks = http_request(location, port, path, scheme='http')
            if json_resp_tasks is None:
                return None
            task_attempt_list = json_resp_tasks['taskAttempts']['taskAttempt']
            task_attempt_document = []
            for task_attempt in task_attempt_list:
                task_attempt['wfName'] = wfName
                task_attempt['wfId'] = wfId
                task_attempt['wfaId'] = wfaId
                task_attempt['wfaName'] = wfaName
                task_attempt['_plugin'] = "oozie"
                task_attempt['_documentType'] = 'taskAttemptStat'
                task_attempt['_tag_appName'] = "oozie"
                task_attempt['jobId'] = job_id
                task_attempt['taskId'] = task

                task_attempt['time'] = int(time.time())
                task_attempt['startTime'] = int(task_attempt['startTime'] / 1000)
                task_attempt['finishTime'] = int(task_attempt['finishTime'] / 1000)
                task_attempt['elapsedTime'] = int(task_attempt['elapsedTime'] / 1000)
                task_attempt['taskAttemptId'] = task_attempt.pop('id')
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
                json_resp_tasks1 = http_request(location, port, path, scheme='http')
                task_attempt_counter_group = json_resp_tasks1['jobTaskAttemptCounters']["taskAttemptCounterGroup"]
                task_attempt_counter = {}
                for group in task_attempt_counter_group:
                    counter_list = group["counter"]
                    for counter in counter_list:
                        task_attempt_counter[convert_camelcase(counter["name"], "_")] = counter["value"]

                task_attempt.update(task_attempt_counter)

                task_attempt_document.append(task_attempt)

            task_attempt_document_job.append(task_attempt_document)

        return task_attempt_document_job
    except Exception as e:
        logger.debug('Unable to get task details => ' + traceback.format_exc().splitlines()[-1])
        return None


def get_containers_node(host, port, app_id):

    location = host
    port = port
    path = '/ws/v1/cluster/nodes'.format(app_id)
    nodes_json = http_request(location, port, path, scheme='http')
    if nodes_json is None:
        return None

    nodes_list = nodes_json["nodes"]["node"]
    return nodes_list


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
