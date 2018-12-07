import json
import time
import logging
import collectd



def convert_camelcase(str_to_convert, separator):
    c = ''.join(x for x in str_to_convert.title() if not x == separator)
    c = c[0].lower() + c[1::]
    return c

def initialize_job_stats():
    return {
"slotsMillisMapsReduce": 0,
"reduceInputRecordsReduce": 0,
"slotsMillisMapsMap": 0,
"fileLargeReadOpsTotal": 0,
"hdfsReadOpsMap": 0,
"mapOutputBytesTotal": 0,
"reduceShuffleBytesMap": 0,
"uberized": False,
"committedHeapBytesMap": 0,
"hdfsLargeReadOpsMap": 0,
"shuffledMapsTotal": 0,
"wfName": "",
"fileWriteOpsMap": 0,
"fileBytesReadMap": 0,
"physicalMemoryBytesReduce": 0,
"mapOutputRecordsTotal": 0,
"vcoresMillisMapsTotal": 0,
"reduceInputRecordsTotal": 0,
"committedHeapBytesReduce": 0,
"reduceInputGroupsMap": 0,
"fileWriteOpsReduce": 0,
"vcoresMillisReducesReduce": 0,
"hdfsBytesReadReduce": 0,
"reduceInputGroupsTotal": 0,
"fileBytesWrittenTotal": 0,
"slotsMillisMapsTotal": 0,
"fileWriteOpsTotal": 0,
"name": "",
"hdfsBytesReadTotal": 0,
"physicalMemoryBytesTotal": 0,
"ioErrorMap": 0,
"killedReduceAttempts": 0,
"fileLargeReadOpsReduce": 0,
"vcoresMillisReducesTotal": 0,
"millisMapsMap": 0,
"hdfsReadOpsReduce": 0,
"hdfsReadOpsTotal": 0,
"failedShuffleReduce": 0,
"badIdMap": 0,
"connectionReduce": 0,
"fileBytesReadTotal": 0,
"mapOutputMaterializedBytesReduce": 0,
"millisReducesTotal": 0,
"wrongReduceTotal": 0,
"combineInputRecordsReduce": 0,
"hdfsWriteOpsTotal": 0,
"totalLaunchedReducesMap": 0,
"spilledRecordsReduce": 0,
"hdfsBytesWrittenTotal": 0,
"physicalMemoryBytesMap": 0,
"fileReadOpsTotal": 0,
"state": "",
"hdfsBytesReadMap": 0,
"badIdTotal": 0,
"mergedMapOutputsTotal": 0,
"virtualMemoryBytesTotal": 0,
"killedMapAttempts": 0,
"wrongLengthMap": 0,
"vcoresMillisMapsReduce": 0,
"wrongReduceReduce": 0,
"failedShuffleTotal": 0,
"mapsTotal": 0,
"failedShuffleMap": 0,
"finishTime": -1,
"combineOutputRecordsMap": 0,
"submitTime": 0,
"shuffledMapsMap": 0,
"hdfsWriteOpsMap": 0,
"fileReadOpsReduce": 0,
"queue": "",
"mbMillisMapsTotal": 0,
"fileBytesReadReduce": 0,
"fileBytesWrittenReduce": 0,
"slotsMillisReducesTotal": 0,
"mbMillisReducesMap": 0,
"combineOutputRecordsTotal": 0,
"virtualMemoryBytesReduce": 0,
"connectionTotal": 0,
"avgShuffleTime": 0,
"wfaName": "",
"jobId": "",
"avgReduceTime": 0,
"mapOutputMaterializedBytesTotal": 0,
"millisMapsTotal": 0,
"combineInputRecordsTotal": 0,
"mapInputRecordsTotal": 0,
"connectionMap": 0,
"splitRawBytesMap": 0,
"avgMergeTime": 0,
"totalLaunchedMapsMap": 0,
"successfulMapAttempts": 0,
"reduceOutputRecordsMap": 0,
"cpuMillisecondsTotal": 0,
"wrongLengthTotal": 0,
"ioErrorTotal": 0,
"hdfsLargeReadOpsTotal": 0,
"bytesReadTotal": 0,
"mapsCompleted": 0,
"wrongMapTotal": 0,
"vcoresMillisReducesMap": 0,
"hdfsBytesWrittenMap": 0,
"_documentType": "",
"bytesReadReduce": 0,
"mbMillisReducesTotal": 0,
"fileBytesWrittenMap": 0,
"reduceOutputRecordsReduce": 0,
"slotsMillisReducesReduce": 0,
"mapOutputRecordsReduce": 0,
"millisReducesReduce": 0,
"gcTimeMillisTotal": 0,
"reduceInputRecordsMap": 0,
"slotsMillisReducesMap": 0,
"mapOutputMaterializedBytesMap": 0,
"mergedMapOutputsMap": 0,
"committedHeapBytesTotal": 0,
"badIdReduce": 0,
"dataLocalMapsTotal": 0,
"mbMillisReducesReduce": 0,
"hdfsBytesWrittenReduce": 0,
"mapInputRecordsMap": 0,
"reduceOutputRecordsTotal": 0,
"failedReduceAttempts": 0,
"mapOutputBytesMap": 0,
"wrongMapReduce": 0,
"reducesCompleted": 0,
"cpuMillisecondsReduce": 0,
"avgMapTime": 0,
"totalLaunchedMapsReduce": 0,
"bytesWrittenTotal": 0,
"hdfsWriteOpsReduce": 0,
"virtualMemoryBytesMap": 0,
"wrongReduceMap": 0,
"splitRawBytesReduce": 0,
"bytesReadMap": 0,
"millisReducesMap": 0,
"successfulReduceAttempts": 0,
"reduceInputGroupsReduce": 0,
"wfId": "",
"waitTime": 0,
"dataLocalMapsMap": 0,
"mbMillisMapsReduce": 0,
"wrongMapMap": 0,
"fileReadOpsMap": 0,
"gcTimeMillisMap": 0,
"reducesTotal": 0,
"spilledRecordsMap": 0,
"combineOutputRecordsReduce": 0,
"splitRawBytesTotal": 0,
"bytesWrittenReduce": 0,
"gcTimeMillisReduce": 0,
"totalLaunchedReducesReduce": 0,
"millisMapsReduce": 0,
"mapInputRecordsReduce": 0,
"time": 0,
"totalLaunchedMapsTotal": 0,
"failedMapAttempts": 0,
"vcoresMillisMapsMap": 0,
"mapOutputBytesReduce": 0,
"user": "",
"startTime": -1,
"mergedMapOutputsReduce": 0,
"ioErrorReduce": 0,
"bytesWrittenMap": 0,
"spilledRecordsTotal": 0,
"mbMillisMapsMap": 0,
"totalLaunchedReducesTotal": 0,
"dataLocalMapsReduce": 0,
"shuffledMapsReduce": 0,
"reduceShuffleBytesReduce": 0,
"wfaId": "",
"combineInputRecordsMap": 0,
"cpuMillisecondsMap": 0,
"hdfsLargeReadOpsReduce": 0,
"diagnostics": "" ,
"reduceShuffleBytesTotal": 0,
"wrongLengthReduce": 0,
"mapOutputRecordsMap": 0,
"fileLargeReadOpsMap": 0
}


def parse_job_event(line, job_json, event_type):
    event = next(iter(line['event'].values()))
    job_id = event['jobid']
    try:
        if event_type == 'submit':
            sd = initialize_job_stats()
            sd['jobId'] = job_id
            sd['name'] = event['jobName']
            sd['user'] = event['userName']
            sd['submitTime'] = event['submitTime']
            sd['queue'] = event['jobQueueName']
            job_json[job_id] = sd
        elif event_type == 'init':
            job_json[job_id]['startTime'] = event['launchTime']
            job_json[job_id]["mapsTotal"]  = event['totalMaps']
            job_json[job_id]["reducesTotal"] =  event['totalReduces']
            job_json[job_id]["uberized"] = event['uberized']
        elif event_type == 'finish':
            job_json[job_id]['finishTime'] = event['finishTime']
            job_json[job_id]['mapsCompleted'] = event['finishedMaps']
            job_json[job_id]['reducesCompleted'] = event['finishedReduces']
            job_json[job_id]['state'] = "SUCCEEDED"

            for ct in ['total', 'map', 'reduce']:
                # counters = event[ct+'Counters']['groups']['counts']
                if event[ct+'Counters']['groups']:
                    for group in event[ct+'Counters']['groups']:
                        counters = group['counts']
                        for c in counters:
                            job_json[job_id][convert_camelcase(c["name"], "_")+ct.title()] = c['value']
        elif event_type == 'info_changed':
            job_json[job_id]['submitTime'] = event['submitTime']
            job_json[job_id]['startTime'] = event['launchTime']
        elif event_type == "queue_changed":
            job_json['queue'] = event['jobQueueName']
        elif event_type == "unsuccessful": # handles killed or error as well
            job_json[job_id]['finishTime'] = event['finishTime']
            job_json[job_id]['mapsCompleted'] = event['finishedMaps']
            job_json[job_id]['reducesCompleted'] = event['finishedReduces']
            job_json[job_id]['state'] = event['jobStatus']
            job_json[job_id]['diagnostics'] = event['diagnostics']
    except KeyError as error:
        return error


def parse_task_events(line, tasks_json, event_type):

    event = next(iter(line['event'].values()))
    try:
        if event_type == "start":
            sd = {}
            taskId = event['taskid']
            sd['taskId'] = taskId
            sd['id'] = taskId
            sd['type'] = event['taskType']
            sd['submitTime'] = int(event['startTime'] / 1000)
            sd['startTime'] = int(event['startTime']/1000)
            tasks_json[sd['taskId']] = sd
        elif event_type == "finish":
            taskId = event['taskid']
            tasks_json[taskId]['state'] = event['status']
            tasks_json[taskId]['finishTime'] = int(event['finishTime']/1000)
            tasks_json[taskId]['endTime'] = int(event['finishTime'] / 1000)
            tasks_json[taskId]['successfulAttempt'] = event['successfulAttemptId']['string']
            counters = event['counters']['groups']
            for group in counters:
                counter_list = group["counts"]
                for counter in counter_list:
                    tasks_json[taskId][convert_camelcase(counter["name"], "_")] = counter["value"]
        elif event_type == "failed":
            taskId = event['taskid']
            tasks_json[taskId]['state'] = event['status']
            tasks_json[taskId]['finishTime'] = int(event['finishTime']/1000)
            tasks_json[taskId]['endTime'] = int(event['finishTime'] / 1000)
            tasks_json[taskId]['error'] = event['error']
            tasks_json[taskId]['failedDueToAttempt'] = event['failedDueToAttempt']
            counters = event['counters']['groups']
            for group in counters:
                counter_list = group["counts"]
                for counter in counter_list:
                    tasks_json[taskId][convert_camelcase(counter["name"], "_")] = counter["value"]
    except KeyError as error:
        return error


def parse_task_attempt_events(line, attempts_json, event_type):

    event = next(iter(line['event'].values()))
    try:
        taskAttemptId = event['attemptId']
        if event_type == "start":
            sd = {}
            sd['taskId'] = event['taskid']
            sd['taskAttemptId'] = taskAttemptId
            sd['type'] = event['taskType']
            sd['submitTime'] = int(event['startTime'] / 1000)
            sd['id'] = taskAttemptId
            sd['startTime'] = int(event['startTime'] / 1000)
            sd['finishTime'] = -1
            sd['elapsedTime'] = -1
            sd['containerId'] = event['containerId']
            if 'locality' in event:
                sd['locality'] = event['locality']['string']
            attempts_json[taskAttemptId] = sd
        elif event_type == "finish":
            attempts_json[taskAttemptId]['state'] = event['taskStatus']
            attempts_json[taskAttemptId]['finishTime'] = int(event['finishTime'] / 1000)
            attempts_json[taskAttemptId]['endTime'] = int(event['finishTime'] / 1000)
            attempts_json[taskAttemptId]['nodeHttpAddress'] = event['hostname']
            attempts_json[taskAttemptId]['rackname'] = event['rackname']
            attempts_json[taskAttemptId]['elapsedTime'] = attempts_json[taskAttemptId]['finishTime'] - \
                                                          attempts_json[taskAttemptId]['startTime']

            counters = event['counters']['groups']
            if 'mapFinishTime' in event:
                attempts_json[taskAttemptId]['mapFinishTime'] = int(event['mapFinishTime'] / 1000)
            if 'shuffleFinishTime' in event:
                attempts_json[taskAttemptId]['shuffleFinishTime'] = int(event['shuffleFinishTime'] / 1000)
                attempts_json[taskAttemptId]['elapsedShuffleTime'] = attempts_json[taskAttemptId]['shuffleFinishTime'] - \
                                                          attempts_json[taskAttemptId]['startTime']
                if 'sortFinishTime' in event:
                    attempts_json[taskAttemptId]['mergeFinishTime'] = int(event['sortFinishTime'] / 1000)
                    attempts_json[taskAttemptId]['elapsedMergeTime'] = attempts_json[taskAttemptId]['mergeFinishTime'] - \
                                                          attempts_json[taskAttemptId]['shuffleFinishTime']
                    attempts_json[taskAttemptId]['elapsedReduceTime'] = attempts_json[taskAttemptId]['elapsedTime'] - \
                       attempts_json[taskAttemptId]['elapsedShuffleTime'] - attempts_json[taskAttemptId]['elapsedMergeTime']

            if 'state' in event:
                attempts_json[taskAttemptId]['status'] = event['state']
            if 'diagnostics' in event:
                attempts_json[taskAttemptId]['diagnostics'] = event['diagnostics']
            else:
                attempts_json[taskAttemptId]['diagnostics'] = ''

            attempts_json[taskAttemptId]['progress'] = 100
            for group in counters:
                counter_list = group["counts"]
                for counter in counter_list:
                    attempts_json[taskAttemptId][convert_camelcase(counter["name"], "_")] = counter["value"]
        elif event_type == "failed":
            if taskAttemptId not in attempts_json:
                attempts_json[taskAttemptId] = {}
                attempts_json[taskAttemptId]['startTime'] = 0
            attempts_json[taskAttemptId]['taskId'] = event['taskid']
            attempts_json[taskAttemptId]['taskAttemptId'] = taskAttemptId
            attempts_json[taskAttemptId]['type'] = event['taskType']
            attempts_json[taskAttemptId]['error'] = event['error']
            attempts_json[taskAttemptId]['finishTime'] = int(event['finishTime'] / 1000)
            attempts_json[taskAttemptId]['endTime'] = int(event['finishTime'] / 1000)
            attempts_json[taskAttemptId]['elapsedTime'] = attempts_json[taskAttemptId]['finishTime'] - \
                                                          attempts_json[taskAttemptId]['startTime']
            attempts_json[taskAttemptId]['rackname'] = event['rackname']
            attempts_json[taskAttemptId]['state'] = event['status']
            attempts_json[taskAttemptId]['nodeHttpAddress'] = event['hostname']
            attempts_json[taskAttemptId]['rackname'] = event['rackname']
            counters = event['counters']['groups']
            for group in counters:
                counter_list = group["counts"]
                for counter in counter_list:
                    attempts_json[taskAttemptId][convert_camelcase(counter["name"], "_")] = counter["value"]
    except KeyError as error:
        return error

def process_job_stats_from_attempts(map_attempts, reduce_attempts, job, tasks):
    successfulMapAttempts = 0
    failedMapAttempts = 0
    killedMapAttempts = 0
    successfulReduceAttempts = 0
    failedReduceAttempts = 0
    killedReduceAttempts = 0
    numOfMaps = 0
    numOfReduces = 0
    avgMapTime = 0
    avgReduceTime = 0
    avgShuffleTime = 0
    avgMergeTime = 0
    map_attempts_list = map_attempts.values()
    reduce_attempts_list = reduce_attempts.values()
    for task in tasks.values():
        if task['type'] == 'MAP':
            map_attempts_for_task = [ x for x in map_attempts_list if x['taskId'] == task['taskId']]
            for task_attempt in map_attempts_for_task:
                #print task_attempt
                if task_attempt['state'] == "SUCCEEDED":
                    successfulMapAttempts += 1
                    numOfMaps += 1
                    avgMapTime += task_attempt['elapsedTime']
                elif task_attempt['state'] == "FAILED":
                    failedMapAttempts += 1
                elif task_attempt['state'] == "KILLED":
                    killedMapAttempts += 1
        elif task['type'] == 'REDUCE':
            reduce_attempts_for_task = [x for x in reduce_attempts_list if x['taskId'] == task['taskId']]
            for task_attempt in reduce_attempts_for_task:
                if task_attempt['state'] == "SUCCEEDED":
                    successfulReduceAttempts += 1
                    numOfReduces += 1
                    avgShuffleTime += task_attempt['elapsedShuffleTime']
                    avgMergeTime += task_attempt['elapsedMergeTime']
                    avgReduceTime +=  task_attempt['elapsedReduceTime']
                elif task_attempt['state'] == "FAILED":
                    failedReduceAttempts += 1
                elif task_attempt['state'] == "KILLED":
                    killedReduceAttempts += 1

    if numOfMaps > 0:
        avgMapTime = int(avgMapTime/numOfMaps)

    if numOfReduces > 0:
        avgShuffleTime = int(avgShuffleTime/numOfReduces)
        avgMergeTime = int(avgMergeTime/numOfReduces)
        avgReduceTime = int(avgReduceTime/numOfReduces)

    job['failedReduceAttempts'] = failedReduceAttempts
    job['killedReduceAttempts'] = killedReduceAttempts
    job['successfulReduceAttempts'] = successfulReduceAttempts
    job['failedMapAttempts'] = failedMapAttempts
    job['killedMapAttempts'] = killedMapAttempts
    job['successfulMapAttempts'] = successfulMapAttempts
    job['avgMapTime'] = avgMapTime
    job['avgShuffleTime'] = avgShuffleTime
    job['avgMergeTime'] = avgMergeTime
    job['avgReduceTime'] = avgReduceTime



def add_additional_fields(original, to_add):

    def merge_two_dicts(x, y):
        z = x.copy()
        z.update(y)
        return z

    for (key, value) in original.items():
        v = merge_two_dicts(value, to_add)
        original[key] = v


def process_jhist(fl, job_id, wfId, wfName, wfaId, wfaName):
    with open(fl, "r") as fin:
        tasks_list = {}
        job_json = {}
        tasks_json = {}
        map_attempt_json = {}
        reduce_attempt_json = {}

        for line in fin:
            if line[0] == '{':
                line_json = json.loads(line)
                type = line_json['type']
                # print(line_json)
                # print(line_json['type'])

                if type == 'MAP_ATTEMPT_STARTED':
                    parse_task_attempt_events(line_json, map_attempt_json, "start")
                elif type == 'MAP_ATTEMPT_FINISHED':
                    parse_task_attempt_events(line_json, map_attempt_json, "finish")
                elif type == 'MAP_ATTEMPT_FAILED' or type == 'MAP_ATTEMPT_KILLED':
                    parse_task_attempt_events(line_json, map_attempt_json, "failed")
                elif type == 'REDUCE_ATTEMPT_STARTED':
                    parse_task_attempt_events(line_json, reduce_attempt_json, "start")
                elif type == 'REDUCE_ATTEMPT_FINISHED':
                    parse_task_attempt_events(line_json, reduce_attempt_json, "finish")
                elif type == 'REDUCE_ATTEMPT_FAILED' or type == 'REDUCE_ATTEMPT_KILLED':
                    parse_task_attempt_events(line_json, reduce_attempt_json, "failed")
                elif type == 'TASK_STARTED':
                    parse_task_events(line_json, tasks_json, "start")
                elif type == 'TASK_FINISHED':
                    parse_task_events(line_json, tasks_json, "finish")
                elif type == 'TASK_FAILED':
                    parse_task_events(line_json, tasks_json, "failed")
                elif type == 'JOB_FINISHED':
                    parse_job_event(line_json, job_json, "finish")
                elif type == 'JOB_SUBMITTED':
                    parse_job_event(line_json, job_json, "submit")
                elif type == 'JOB_INITED':
                    parse_job_event(line_json, job_json, "init")
                elif type == 'JOB_INFO_CHANGED':
                    parse_job_event(line_json, job_json, "info_changed")
                elif type == "JOB_FAILED" or type == "JOB_KILLED" or type == "JOB_ERROR":
                    parse_job_event(line_json, job_json, "unsuccessful")
                else:
                    continue

        if job_json:
            process_job_stats_from_attempts(map_attempt_json, reduce_attempt_json, job_json[job_id], tasks_json)

    add_to_task_attempt = {
            '_documentType' : 'taskAttemptStat',
            'jobId' : job_id,
            'time' : int(time.time()),
            'wfId' : wfId,
            'wfName': wfName,
            'wfaId': wfaId,
            'wfaName': wfaName
    }

    add_to_task = {
            '_documentType': 'taskStats',
            'jobId': job_id,
            'time': int(time.time()),
            'wfId': wfId,
            'wfName': wfName,
            'wfaId': wfaId,
            'wfaName': wfaName
    }

    add_to_job = {
                  '_documentType': 'jobStats',
                  'jobId': job_id,
                  'time': int(time.time()),
                  'wfId': wfId,
                  'wfName': wfName,
                  'wfaId': wfaId,
                  'wfaName': wfaName
                  }
    add_additional_fields(tasks_json, add_to_task)
    add_additional_fields(job_json, add_to_job)
    add_additional_fields(map_attempt_json, add_to_task_attempt)
    add_additional_fields(reduce_attempt_json, add_to_task_attempt)

    return {
        'taskInfo': tasks_json,
        'tasksMap' : map_attempt_json,
        'tasksReduce': reduce_attempt_json,
        'jobInfo': job_json
    }
