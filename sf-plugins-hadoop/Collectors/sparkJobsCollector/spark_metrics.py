import logging
import math
import time
import sys
from configuration import tasks_by_time, plugin_name, tag_app_name
from library.buildData import prepare_spark_task_stats_by_timepoint
from bisect import bisect

logger = logging.getLogger(__name__)


def aggregate_tasks_metrics(stage_json, tasks_json, executors):

    metrics_for_aggregation = ['jvmGcTime', 'shuffleReadRemoteBlocksFetched', 'shuffleReadLocalBlocksFetched',
                               'shuffleReadFetchWaitTime', 'shuffleReadRemoteBytesRead',
                               'shuffleReadRemoteBytesReadToDisk', 'shuffleReadLocalBytesRead',
                               'shuffleReadRecordsRead', 'shuffleWriteWriteTime', 'shuffleWriteRecordsWritten',
                               'executorDeserializeTime', 'executorDeserializeCpuTime', 'resultSerializationTime',
                               'resultSize', 'isStraggler', ]

    metrics_to_find_max = []

    def find_stage(s, sa):
        for stg in stage_json:
            if int(stg['stageId']) == int(s) and int(stg['stageAttemptId']) == int(sa):
                return stg

    for stage in stage_json:
        for metric in metrics_for_aggregation:
            stage[metric] = 0

        for metric in metrics_to_find_max:
            stage['max' + metric] = 0


    for task in tasks_json:
        stage_id = task['stageId']
        stage_attempt_id = task['stageAttemptId']
        stage = find_stage(stage_id, stage_attempt_id)

        for metric in metrics_for_aggregation:
            stage[metric] += task.get(metric, 0)

        for metric in metrics_to_find_max:
            if int(task.get(metric, 0)) > stage['max' + metric]:
                stage['max' + metric] = int(task.get(metric, 0))

    totalCores = 0
    for executor in executors:
        totalCores += executor.get('totalCores', 0)


    for stage in stage_json:
        stage['totalCores'] = totalCores
        if 'numTasks' not in stage:
            stage['numTasks'] = stage['numCompleteTasks'] + stage['numFailedTasks']




def aggregate_stages_metrics(app_json, stages_json):

    metrics_from_stages = ['numTasks', 'numFailedTasks', 'numKilledTasks', 'inputBytes', 'outputBytes',
                               'inputRecords', 'outputRecords', 'shuffleReadBytes', 'shuffleWriteBytes',
                               'memoryBytesSpilled', 'executorRunTime', 'executorCpuTime', 'diskBytesSpilled',
                               'resultSerializationTime', 'executorDeserializeTime', 'executorDeserializeCpuTime',
                               'taskSchedulingDelay', 'resultSize', 'isStraggler', ]

    metrics_from_tasks = ['jvmGcTime', 'shuffleReadRemoteBlocksFetched', 'shuffleReadLocalBlocksFetched',
                               'shuffleReadFetchWaitTime', 'shuffleReadRemoteBytesRead',
                               'shuffleReadRemoteBytesReadToDisk', 'shuffleReadLocalBytesRead',
                               'shuffleReadRecordsRead', 'shuffleWriteWriteTime', 'shuffleWriteRecordsWritten']

    metrics_for_aggregation = metrics_from_tasks + metrics_from_stages

    metrics_to_find_max = ['maxParallelTasks']
    metrics_to_find_min = ['minParallelTasks']


    for metric in metrics_for_aggregation:
        app_json[metric] = 0

    for metric in metrics_to_find_max:
        if metric.startswith('max'):
            app_json[metric] = 0
        else:
            app_json['max' + metric] = 0

    for metric in metrics_to_find_min:
        if stages_json:
            if metric.startswith('min'):
                app_json[metric] = sys.maxint
            else:
                app_json['min' + metric] = sys.maxint
        else:
            if metric.startswith('min'):
                app_json[metric] = 0
            else:
                app_json['min' + metric] = 0


    stage_ids = set()
    totalCores = 0
    for stage in stages_json:
        stage_ids.add(stage['stageId'])
        totalCores = stage['totalCores']
        for metric in metrics_for_aggregation:
            app_json[metric] += int(stage.get(metric, 0))

        for metric in metrics_to_find_max:
                if metric.startswith('max'):
                    if int(stage.get(metric, 0)) > app_json[metric]:
                        app_json[metric] = int(stage.get(metric, 0))
                else:
                    if int(stage.get(metric, 0)) > app_json['max' + metric]:
                        app_json['max' + metric] = int(stage.get(metric, 0))

        for metric in metrics_to_find_min:
                if metric.startswith('min'):
                    if int(stage.get(metric, 0)) < app_json[metric]:
                        app_json[metric] = int(stage.get(metric, 0))
                else:
                    if int(stage.get(metric, 0)) < app_json['min' + metric]:
                        app_json['min' + metric] = int(stage.get(metric, 0))


    app_json['numStages'] = len(stage_ids)
    app_json['totalCores'] = totalCores

    app_json['dataProc'] = (app_json.get('inputBytes', 0) +
                            app_json.get('outputBytes', 0) +
                            app_json.get('shuffleWriteBytes', 0) +
                            app_json.get('shuffleReadBytes', 0) +
                            app_json.get('resultSize', 0))/ 1048576


def get_critical_path(jobs_list, startTimeKey, endTimeKey):
    critical_path = []
    sorted_job_list = None
    if jobs_list:
        sorted_job_list = sorted(jobs_list, key=lambda k: k[startTimeKey])
    #logger.debug("sorted_job_list: {0}".format(sorted_job_list))
    if sorted_job_list and len(sorted_job_list) > 1:
        index = 0
        for job in sorted_job_list:
            if job[startTimeKey] and job[endTimeKey] and job[startTimeKey] > 0 and job[endTimeKey] > 0:
                if index == 0:
                    iterationStartTime = job[startTimeKey]
                    iterationEndTime = job[endTimeKey]
                #logger.debug("IterationStartTime:{0}, IterationEndTime:{1}".format(iterationStartTime, iterationEndTime))
                #logger.debug("JobStartTime:{0}, JobEndTime:{1}".format(job[startTimeKey], job[endTimeKey]))
                if job[startTimeKey] >= iterationStartTime and job[startTimeKey] <= iterationEndTime:
                    if job[endTimeKey] > iterationEndTime:
                        iterationEndTime = job[endTimeKey]
                elif job[startTimeKey] > iterationEndTime:
                    critical_path.append({startTimeKey : iterationStartTime, endTimeKey: iterationEndTime})
                    #logger.debug("Added to critical path iterationStartTime:{0} iterationEndTime:{1}".format(iterationStartTime,
                    #                                                                                iterationEndTime))
                    iterationStartTime = job[startTimeKey]
                    iterationEndTime = job[endTimeKey]
                index = index + 1
                if index == len(sorted_job_list):
                    critical_path.append({startTimeKey: iterationStartTime, endTimeKey: iterationEndTime})
            else:
                return None
    else:
        return None

    return critical_path

def get_app_stage_scheduling_delay(app, stages):
    valid_stages = [ stage for stage in stages if 'submissionTime' in stage and 'completionTime' in stage ]

    app['stageSchedulingDelay'] = 0

    if valid_stages:
        if 'endTime' in app and 'startTime' in app:
            app_runtime = app['endTime'] - app['startTime']
            cumulative_stage_runtime = 0
            result = get_critical_path(valid_stages, 'submissionTime', 'completionTime')
            if result:
                for stage in result:
                    cumulative_stage_runtime += stage['completionTime'] - stage['submissionTime']
            if app_runtime > 0 and cumulative_stage_runtime > 0:
                app['stageSchedulingDelay'] = app_runtime - cumulative_stage_runtime
            logger.debug("appId:{0} app_runtime:{1}, cumulative_stage_runtime:{2}, stageSchedulingDelay:{3}".format( app['appId'],
                 app_runtime, cumulative_stage_runtime, app['stageSchedulingDelay']))


def get_stage_task_scheduling_delay(stage, tasks):
    stage['taskSchedulingDelay'] = 0
    if stage and tasks:
        valid_tasks = [ task for task in tasks if 'launchTime' in task and 'endTime' in task and
                        stage['stageId'] == task['stageId'] and stage['stageAttemptId'] == task['stageAttemptId']]

        if valid_tasks:
            cumulative_task_runtime = 0
            if 'completionTime' in stage and 'submissionTime' in stage:
                stage_run_time = stage['completionTime'] - stage['submissionTime']
                result = get_critical_path(valid_tasks, 'launchTime', 'endTime')
                if result:
                    for task in result:
                        cumulative_task_runtime += task['endTime'] - task['launchTime']

                if stage_run_time > 0 and cumulative_task_runtime > 0:
                    stage['taskSchedulingDelay'] = stage_run_time - cumulative_task_runtime

                logger.debug("StageId:{0} StageAttemptId:{1} stage_runtime:{2}, cumulative_task_runtime:{3}, taskSchedulingDelay:{4} tasksinStage:{5} totalTasks:{6}".format(
                    stage['stageId'], stage['stageAttemptId'], stage_run_time, cumulative_task_runtime, stage['taskSchedulingDelay'], len(valid_tasks), len(tasks)))


def calculate_taskcount_by_time_points(stage, task_list, appId, app_attempt_id, stageId, stageAttemptId,  appName):
    if task_list:
        stage_attempt_task_list = [ task for task in task_list if task['stageId'] == stageId
                                    and task['stageAttemptId'] == stageAttemptId]
        if not stage_attempt_task_list:
            return
    else:
        return
    stage_submit_time = stage['firstTaskLaunchedTime']
    stage_finish_time = stage['completionTime'] + 1
    #interval_to_plot = int(math.ceil((stage_finish_time - stage_submit_time) / tasks_by_time['numOfDataPoints']))
    #interval_to_plot = interval_to_plot if interval_to_plot > tasks_by_time['minimumInterval'] else tasks_by_time['minimumInterval']
    interval_to_plot = tasks_by_time['minimumInterval']
    sorted_task_list = sorted(stage_attempt_task_list, key=lambda x: x['launchTime'])
    tpTaskStats = []
    for x in range(stage_submit_time, stage_finish_time, interval_to_plot ):
        time_interval_start = x
        time_interval_end = x + interval_to_plot if x + interval_to_plot < stage_finish_time else stage_finish_time
        task_count = 0
        for task in sorted_task_list:
            if task['launchTime'] and task['endTime']:
                if (task['launchTime'] >= time_interval_start and task['launchTime'] < time_interval_end) or \
                    (task['endTime'] > time_interval_start and task['endTime'] <= time_interval_end) or \
                        (task['launchTime'] < time_interval_start and task['endTime'] > time_interval_end):
                        task_count += 1
        stat = prepare_spark_task_stats_by_timepoint(time_interval_start, time_interval_end, task_count, appId,
                                                     app_attempt_id, stageId, stageAttemptId, appName)
        tpTaskStats.append(stat)
    maxParallelTasks = 0
    minParallelTasks = sys.maxint
    sumTasks = 0
    avgParallelTasks = 0
    if tpTaskStats:
        for stat in tpTaskStats:
            if stat['taskCount'] > maxParallelTasks:
                maxParallelTasks = stat['taskCount']
            if stat['taskCount'] < minParallelTasks:
                minParallelTasks = stat['taskCount']
            sumTasks += stat['taskCount']
        avgParallelTasks = int(sumTasks/len(tpTaskStats))
    else:
        maxParallelTasks = 0
        minParallelTasks = 0
        avgParallelTasks = 0
    stage['minParallelTasks'] = minParallelTasks
    stage['maxParallelTasks'] = maxParallelTasks
    stage['avgParallelTasks'] = avgParallelTasks

    logger.debug("The number of task count points collected for stageId:{0}, stageAttemptId:{1} is {2}".format(
                                                                             stageId, stageAttemptId, len(tpTaskStats)))

    return tpTaskStats if tpTaskStats else None


def find_stragglers_runtime(tasks_list):

    def percentile(N, percent, key=lambda x: x):
        if not N:
            return None
        k = (len(N) - 1) * percent
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return key(N[int(k)])
        d0 = key(N[int(f)]) * (c - k)
        d1 = key(N[int(c)]) * (k - f)
        return d0 + d1

    def find_outliers(nums):
        q75 = percentile(nums, 0.75)
        q25 = percentile(nums, 0.25)

        iqr = q75 - q25
        iqr15 = iqr * 1.5
        lower = q25 - iqr15
        upper = q75 + iqr15
        return upper

    tasks_sorted_elapsed_time = sorted(tasks_list, key=lambda k: k['duration'])
    elapsed_time_list = []

    for e in tasks_sorted_elapsed_time:
        elapsed_time_list.append(e['duration'])

    upper = find_outliers(elapsed_time_list)
    straggler_index = (bisect(elapsed_time_list, upper))
    for idx, task in enumerate(tasks_list):
        if idx < straggler_index:
            task["isStraggler"] = 0
        else:
            task["isStraggler"] = 1


