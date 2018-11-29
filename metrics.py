from bisect import *
from utilities import *
#from buildData import prepare_task_stats_by_timepoint

logger = logging.getLogger(__name__)

reduce_start = 70.0

tasks_by_time = {
    "numOfDataPoints": 25,
    "minimumInterval": 5  # seconds
}

def prepare_task_stats_by_timepoint(tp_start, tp_end, map_count, reduce_count, job_id, wfaId, wfId, wfName, wfaName):
    return {
        "wfId": wfId,
        "wfaId": wfaId,
        "wfName": wfName,
        "wfaName": wfaName,
        "jobId": job_id,
        'timePeriodStart': tp_start,
        'timePeriodEnd': tp_end,
        "mapTaskCount": map_count,
        "reduceTaskCount": reduce_count,
        'duration': tp_end - tp_start,
        "_plugin": "hadoop",
        "_documentType": "taskCounts",
        "_tag_appName": "hadoop"
    }

def get_wait_time(job_json, tasks_reduce, tasks_map):
    wait_time_total = 0
    if tasks_map:
        tasks_map_sorted = sorted(tasks_map, key=lambda k: k['finishTime'])
        max_finish_time_map = max(t['finishTime'] for t in tasks_map)
        max_map_elapsed_time = max(t['elapsedTime'] for t in tasks_map)
        schedule_time = job_json["submitTime"]
        wait_time_map = max_finish_time_map - schedule_time - max_map_elapsed_time
        wait_time_reduce = 0
        if tasks_reduce:
            map_before_reduce = int(math.ceil(len(tasks_map_sorted) * reduce_start / 100))
            if map_before_reduce < len(tasks_map_sorted):
                max_finish_time_reduce = max(t['finishTime'] for t in tasks_reduce)
                max_reduce_elapsed_time = max(t['elapsedTime'] for t in tasks_reduce)
                last_map_before_reduce_finishtime = tasks_map_sorted[map_before_reduce-1]['finishTime']
                wait_time_reduce = max_finish_time_reduce - last_map_before_reduce_finishtime - max_reduce_elapsed_time
                if wait_time_reduce < 0:
                    wait_time_reduce = 0

        wait_time_total = wait_time_map + wait_time_reduce

    return wait_time_total


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


def find_stragglers_runtime(tasks_list):
    tasks_sorted_elapsed_time = sorted(tasks_list, key=lambda k: k['elapsedTime'])
    elapsed_time_list = []
    for e in tasks_sorted_elapsed_time:
        elapsed_time_list.append(e['elapsedTime'])
    upper = find_outliers(elapsed_time_list)
    straggler_index = (bisect(elapsed_time_list, upper))
    for idx, task in enumerate(tasks_list):
        if idx < straggler_index:
            task["isStraggler"] = 0
        else:
            task["isStraggler"] = 1


def calculate_scheduling_delays(workflow, wfa_list):
    #logger.debug("workflow {0}".format(workflow))
    #logger.debug("wfa_list {0}".format((wfa_list)))
    wf_runtime = get_unix_timestamp(workflow['endTime']) - get_unix_timestamp(workflow['startTime'])
    sigma_wfa_runtime = 0
    sigma_job_runtime = 0
    workflow['wfSchedulingDelay'] = 0
    workflow['jobSchedulingDelay'] = 0
    if wfa_list:
        for wfadict in wfa_list:
            wfa = wfadict['action']
            if wfa['endTime'] and wfa['startTime']:
                sigma_job_runtime_wfa_level = 0
                wfa['submitDelay'] = 0
                wfa['jobDelay'] = 0
                sigma_wfa_runtime += wfa['endTime'] - wfa['startTime']
                first_job = None
                index = 0
                for job_info in wfadict['yarnJobs']:
                    if index == 0 and wfa['externalChildID'] and wfa['externalChildID'] != '-': # Launcher job duration encapsulates its children
                        index += 1
                        continue
                    if not first_job:
                        first_job = job_info
                    sigma_job_runtime += job_info['job']['finishTime'] - job_info['job']['startTime']
                    sigma_job_runtime_wfa_level += job_info['job']['finishTime'] - job_info['job']['startTime']
                    index += 1
                if first_job:
                    wfa['submitDelay'] = first_job['job']['submitTime'] - wfa['startTime']
                    wfa['jobDelay'] = (wfa['endTime'] - wfa['startTime']) - sigma_job_runtime_wfa_level
        if sigma_wfa_runtime > 0:
            workflow['wfSchedulingDelay'] = wf_runtime - sigma_wfa_runtime
        if sigma_job_runtime > 0:
            workflow['jobSchedulingDelay'] = wf_runtime - sigma_job_runtime


def find_mapper_spill(tasks_list):

    spilled_records_from_tasks = 0
    output_records_from_tasks = 0
    for task in tasks_list:
        spilled_records_from_tasks += task["spilledRecords"]
        output_records_from_tasks += task["mapOutputRecords"]

    if output_records_from_tasks == 0:
        return 0
    else:
        mapper_spill = spilled_records_from_tasks / output_records_from_tasks

    return mapper_spill


def find_shuffle_ratio(tasks_list):

    elapsed_shuffle_time = 0
    elapsed_time = 0
    elapsed_merge_time = 0
    for task in tasks_list:
        elapsed_shuffle_time += task["elapsedShuffleTime"]
        elapsed_merge_time += task["elapsedMergeTime"]
        elapsed_time += task["elapsedTime"]
 
    if (elapsed_time - elapsed_shuffle_time - elapsed_merge_time) == 0:
        return 0
    else:
        shuffle_ratio = (elapsed_shuffle_time * 2) / (elapsed_time - elapsed_shuffle_time - elapsed_merge_time)

    return shuffle_ratio


def find_map_speed(tasks_list):

   hdfs_bytes_read = 0
   finish_time = 0
   start_time = 0

   for task in tasks_list:
       hdfs_bytes_read += task["hdfsBytesRead"]
       finish_time += task["finishTime"]
       start_time += task["startTime"]
    
   if (finish_time - start_time) == 0:
       return 0
   else:
       map_speed = hdfs_bytes_read / (finish_time - start_time)

   return map_speed


def find_sort_ratio(tasks_list):

   elapsed_shuffle_time = 0
   elapsed_time = 0
   elapsed_merge_time = 0
   for task in tasks_list:
       elapsed_shuffle_time += task["elapsedShuffleTime"]
       elapsed_merge_time += task["elapsedMergeTime"]
       elapsed_time += task["elapsedTime"]

   if (elapsed_time - elapsed_shuffle_time - elapsed_merge_time) == 0:
        return 0
   else:
        sort_ratio = (elapsed_merge_time * 2) / (elapsed_time - elapsed_shuffle_time - elapsed_merge_time)

   return sort_ratio

def calculate_taskcount_by_time_points(job_info, task_list, wfaId, wfId, wfName, wfaName):
    job_start_time = job_info['startTime']
    job_finish_time = job_info['finishTime'] + 1
    interval_to_plot = int(math.ceil((job_finish_time - job_start_time) / tasks_by_time['numOfDataPoints']))
    interval_to_plot = interval_to_plot if interval_to_plot > tasks_by_time['minimumInterval'] else tasks_by_time['minimumInterval']
    sorted_task_list = sorted(task_list, key=lambda x: x['startTime'])
    tpTaskStats = []
    for x in range(job_start_time, job_finish_time, interval_to_plot ):
        time_interval_start = x
        time_interval_end = x + interval_to_plot if x + interval_to_plot <  job_finish_time else job_finish_time
        map_count = 0
        reduce_count = 0
        for task in sorted_task_list:
            if task['startTime'] and task['finishTime']:
                if (task['startTime'] >= time_interval_start and task['startTime'] < time_interval_end) or \
                    (task['finishTime'] > time_interval_start and task['finishTime'] <= time_interval_end) or \
                        (task['startTime'] < time_interval_start and task['finishTime'] > time_interval_end):
                        if task['type'] == 'MAP':
                            map_count += 1
                        else:
                            reduce_count += 1
        stat = prepare_task_stats_by_timepoint(time_interval_start, time_interval_end, map_count, reduce_count,
                                                  job_info['jobId'], wfaId, wfId, wfName, wfaName)
        tpTaskStats.append(stat)
        logger.debug("tpStats {0}".format(tpTaskStats))

    return tpTaskStats if tpTaskStats else None
