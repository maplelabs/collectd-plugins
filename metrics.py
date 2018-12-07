from bisect import * # pylint: disable=unused-import
from utilities import * # pylint: disable=unused-import
import collectd
#from buildData import prepare_task_stats_by_timepoint


reduce_start = 70.0

tasks_by_time = {
    "numOfDataPoints": 25,
    "minimumInterval": 5  # seconds
} # pylint: disable=unused-import

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
        "_plugin": "oozie",
        "_documentType": "taskCounts",
        "_tag_appName": "oozie"
    }


def get_wait_time(job_json, param_tasks_reduce, param_tasks_map):
#    logger.debug("get_wait_time for job {0} with submitTime: {1}".format(job_json['jobId'], job_json["submitTime"]))
    wait_time_total = 0
    wait_time_map = 0
    wait_time_reduce = 0
    tasks_maps_sorted = []
    job_submit_time = job_json["submitTime"]
#    for task in param_tasks_map:
#        collectd.debug("get_wait_time MAP Task: {0} with startTime: {1}, finishTime: {2} , elaspedTime: {3}".format(task['taskId'], task['startTime'], task['finishTime'], task['elapsedTime'] ))
#    for task in param_tasks_reduce:
#        collectd.debug("get_wait_time REDUCE Task: {0} with startTime: {1}, finishTime: {2} , elaspedTime: {3}".format(task['taskId'], task['startTime'], task['finishTime'], task['elapsedTime'] ))
    tasks_map = [ task for task in param_tasks_map if task['startTime'] > 0 if task['finishTime'] > 0 if task['elapsedTime'] > 0 ]
    tasks_reduce = [ task for task in param_tasks_reduce if task['startTime'] > 0 if task['finishTime'] > 0 if task['elapsedTime'] > 0 ]
    if tasks_map and len(tasks_map) > 0:
        max_finish_time_map = max(t['finishTime'] for t in tasks_map)
        max_map_elapsed_time = max(t['elapsedTime'] for t in tasks_map)
        wait_time_map = max_finish_time_map - job_submit_time - max_map_elapsed_time
    if tasks_reduce and len(tasks_reduce) > 0:
       max_finish_time_reduce = max(t['finishTime'] for t in tasks_reduce)
       max_reduce_elapsed_time = max(t['elapsedTime'] for t in tasks_reduce)
       if tasks_map and len(tasks_map) > 0:
           tasks_map_sorted = sorted(tasks_map, key=lambda k: k['finishTime'])
           map_before_reduce = int(math.ceil(len(tasks_map_sorted) * reduce_start / 100))
           if map_before_reduce < len(tasks_map_sorted):
               last_map_before_reduce_finishtime = tasks_map_sorted[map_before_reduce-1]['finishTime']
               wait_time_reduce = max_finish_time_reduce - last_map_before_reduce_finishtime - max_reduce_elapsed_time
       elif job_json['mapsTotal'] > 0: # Maps are there but there is no map tasks data . Can't calculate
           wait_time_reduce = 0
       else: # No Maps
           wait_time_reduce = max_finish_time_reduce -  job_submit_time - max_reduce_elapsed_time
           
    wait_time_total = wait_time_map + wait_time_reduce
#    collectd.debug("get_wait_time Total_wait_time:{0}, Map_wait_time:{1}, Reduce_wait_time:{2}".format(wait_time_total,wait_time_map, wait_time_reduce))

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
    if not tasks_list:
        return
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


def find_mapper_spill(tasks_list): # pylint: disable=unused-import

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
        collectd.debug("tpStats {0}".format(tpTaskStats))

    return tpTaskStats if tpTaskStats else None


def calculate_wf_metrics(workflow, wfa_list):
#    logger.debug("calculate_wf_metrics wfId: {0} startTime: {1}, endTime:{2}".format(workflow['id'], workflow['startTime'], workflow['endTime']))
    #logger.debug("wfa_list {0}".format((wfa_list)))
    sigma_wfa_runtime = 0
    sigma_hdfs_read_bytes = 0
    sigma_hdfs_read_bytes_node_local = 0
    sigma_hdfs_read_bytes_rack_local = 0
    sigma_hdfs_read_bytes_off_switch = 0
    sigma_wait_time = 0
    sigma_cpu_ms = 0
    sigma_maps_total = 0
    sigma_reduces_total = 0
    sigma_killed_reduce_attempts = 0
    sigma_killed_map_attempts = 0
    wSumMapTime = 0
    wSumReduceTime = 0
    wSumShuffleTime = 0
    wSumMergeTime = 0
    collectd.info("<=============== IN metrics function %s %s =============>" %(workflow['startTime'], workflow['endTime']))
    if not workflow['startTime'] or not workflow['endTime']:
#        logger.error("calculate_wf_metrics cannot calculate WF runtime with these details, skipping WF metrics calculation")
        return
    #wf_runtime = get_unix_timestamp(workflow['endTime']) - get_unix_timestamp(workflow['startTime'])
    collectd.info("<=============== IN metrics function start inter =============>")
    if wfa_list:
        collectd.info("<=============== IN metrics function condtion 1 =============>")
        for wfadict in wfa_list:
            wfa = wfadict
#            collectd.info("<=============== IN metrics function condtion iteration %s =============>" %wfa)
#            logger.debug("calculate_wf_metrics WFAId: {0} , startTime: {1} , endTime: {2}".format(wfa['wfaId'], wfa['startTime'], wfa['endTime']))
            if wfa['startTime'] > 0:
                sigma_job_runtime_wfa_level = 0
                sigma_hdfs_read_bytes_wfa_level = 0
                sigma_hdfs_read_bytes_node_local_wfa_level = 0
                sigma_hdfs_read_bytes_rack_local_wfa_level = 0
                sigma_hdfs_read_bytes_off_switch_wfa_level = 0
                sigma_wait_time_wfa_level = 0
                sigma_cpu_ms_wfa_level = 0
                sigma_maps_total_wfa_level = 0
                sigma_reduces_total_wfa_level = 0
                sigma_killed_reduce_attempts_wfa_level = 0
                sigma_killed_map_attempts_wfa_level = 0
                wSumMapTime_wfa_level = 0
                wSumReduceTime_wfa_level = 0
                wSumShuffleTime_wfa_level = 0
                wSumMergeTime_wfa_level = 0
                wfa['submitDelay'] = 0
                wfa['jobDelay'] = 0
                sigma_wfa_runtime += wfa['endTime'] - wfa['startTime']
                first_job = None
                index = 0
                job_info = wfadict
                #for job_info in wfadict:
#                    logger.debug(job_info)
#                    logger.debug("calculate_wf_metrics WFAId: {0} JobId: {1}, startTime: {2} , finishTime: {3}".format(job_info['job']['wfaId'], job_info['job']['jobId'], job_info['job']['startTime'], job_info['job']['finishTime']))
                collectd.info("<=============== IN metrics function condtion 2 =============>")
                if 'taskAttemptStat' in job_info:
#                        for task in job_info['taskAttemptsCounters']:
                    if 'locality' in task and 'hdfsBytesRead' in task:
                        if task['locality'] == "OFF_SWITCH":
                            sigma_hdfs_read_bytes_off_switch += task['hdfsBytesRead']
                            sigma_hdfs_read_bytes_off_switch_wfa_level += task['hdfsBytesRead']
                        elif task['locality'] == "NODE_LOCAL":
                            sigma_hdfs_read_bytes_node_local += task['hdfsBytesRead']
                            sigma_hdfs_read_bytes_node_local_wfa_level += task['hdfsBytesRead']
                        elif task['locality'] == "RACK_LOCAL":
                            sigma_hdfs_read_bytes_rack_local += task['hdfsBytesRead']
                            sigma_hdfs_read_bytes_rack_local_wfa_level += task['hdfsBytesRead']
                collectd.info("<=============== IN metrics function condtion 3 =============>")
                if job_info['_documentType'] == "jobStats" and job_info['finishTime'] > 0 and job_info['startTime'] > 0:
                    collectd.info("<=============== IN metrics function condtion 4 =============>")
                    if 'hdfsBytesReadTotal' in job_info:
                        sigma_hdfs_read_bytes_wfa_level += job_info['hdfsBytesReadTotal']
                        sigma_hdfs_read_bytes += job_info['hdfsBytesReadTotal']
                    if job_info['waitTime']:
                        sigma_wait_time += job_info['waitTime']
                        sigma_wait_time_wfa_level += job_info['waitTime']
                    if 'cpuMillisecondsTotal' in job_info:
                        sigma_cpu_ms += job_info['cpuMillisecondsTotal']
                        sigma_cpu_ms_wfa_level += job_info['cpuMillisecondsTotal']
                    if 'mapsTotal' in  job_info:
                        sigma_maps_total += job_info['mapsTotal']
                        sigma_maps_total_wfa_level += job_info['mapsTotal']
                        if job_info['avgMapTime']:
                            wSumMapTime += job_info['avgMapTime'] * job_info['mapsTotal']
                            wSumMapTime_wfa_level += job_info['avgMapTime'] * job_info['mapsTotal']
                    if job_info['reducesTotal']:
                        sigma_reduces_total += job_info['reducesTotal']
                        sigma_reduces_total_wfa_level += job_info['reducesTotal']
                        if job_info['job']['avgReduceTimeMs']:
                            wSumReduceTime_wfa_level += int((job_info['avgReduceTimeMs'] * job_info['reducesTotal']) / 1000)
                            wSumMergeTime_wfa_level += int((job_info['avgMergeTimeMs'] * job_info['reducesTotal']) / 1000)
                            wSumShuffleTime_wfa_level += int(job_info['avgShuffleTimeMs'] * job_info['reducesTotal'] / 1000)
                            wSumReduceTime += int((job_info['avgReduceTimeMs'] * job_info['reducesTotal']) / 1000)
                            wSumMergeTime += int((job_info['avgMergeTimeMs'] * job_info['reducesTotal']) / 1000)
                            wSumShuffleTime += int(job_info['avgShuffleTimeMs'] * job_info['reducesTotal'] / 1000)
#                               del job_info['job']['avgReduceTimeMs']
#                                del job_info['job']['avgMergeTimeMs']
#                                del job_info['job']['avgShuffleTimeMs']
                    collectd.info("<=============== IN metrics function condtion 5 =============>")
                    if job_info['killedReduceAttempts']:
                        sigma_killed_reduce_attempts += job_info['killedReduceAttempts']
                        sigma_killed_reduce_attempts_wfa_level += job_info['killedReduceAttempts']
                    if job_info['killedMapAttempts']:
                        sigma_killed_map_attempts += job_info['killedMapAttempts']
                        sigma_killed_map_attempts_wfa_level += job_info['killedMapAttempts']
                    collectd.info("<=============== IN metrics function condtion 6 =============>")
                    if index == 0 and wfa['_documentType'] == "oozieWorkflowActions" and wfa['externalChildID'] and wfa['externalChildID'] != '-':  # Launcher job duration encapsulates its children
                        pass
                    elif job_info['_documentType'] == 'jobStats':
                        collectd.info("<=============== IN metrics function condtion 7 =============>")
                        sigma_job_runtime_wfa_level += job_info['finishTime'] - job_info['startTime']
                        if not first_job:
                            first_job = job_info
                    collectd.info("<=============== IN metrics function condtion 8 =============>")
                    index += 1
            collectd.info("<=============== IN metrics function condtion 10 =============>")
            if first_job and wfa['_documentType'] == "oozieWorkflowActions":
                collectd.info("<=============== IN metrics function condtion import =============>")
                wfa['submitDelay'] = first_job['submitTime'] - wfa['startTime']
                if wfa['endTime'] > 0:
                    wfa['jobDelay'] = (wfa['endTime'] - wfa['startTime']) - sigma_job_runtime_wfa_level
                wfa['hdfsBytesReadTotal'] = sigma_hdfs_read_bytes_wfa_level
                wfa['waitTimeTotal'] = sigma_wait_time_wfa_level
                wfa['cpuMillisecondsTotal'] = sigma_cpu_ms_wfa_level
                wfa['mapsTotal'] = sigma_maps_total_wfa_level
                wfa['reducesTotal'] = sigma_reduces_total_wfa_level
                wfa['killedReduceAttempts'] = sigma_killed_reduce_attempts_wfa_level
                wfa['killedMapAttempts'] = sigma_killed_map_attempts_wfa_level
                wfa['offSwitchHdfsBytesReadTotal'] = sigma_hdfs_read_bytes_off_switch_wfa_level
                wfa['rackLocalHdfsBytesReadTotal'] = sigma_hdfs_read_bytes_rack_local_wfa_level
                wfa['nodeLocalHdfsBytesReadTotal'] = sigma_hdfs_read_bytes_node_local_wfa_level
                if sigma_maps_total_wfa_level > 0:
                    wfa['weightedAvgMapTime'] = int(wSumMapTime_wfa_level / sigma_maps_total_wfa_level)
                if sigma_reduces_total_wfa_level > 0:
                    wfa['weightedAvgReduceTime'] = int(wSumReduceTime_wfa_level / sigma_reduces_total_wfa_level)
                    wfa['weightedAvgShuffleTime'] = int(wSumShuffleTime_wfa_level / sigma_reduces_total_wfa_level)
                    wfa['weightedAvgMergeTime'] = int(wSumMergeTime_wfa_level / sigma_reduces_total_wfa_level)
            #        logger.debug("calculate_wf_metrics wfId:{0} wfaId:{1} submitDelay:{2} jobDelay:{3}".format(wfa['wfId'], wfa['wfaId'],  wfa['submitDelay'],  wfa['jobDelay']))
        collectd.info("<=============== IN metrics function condtion 9 =============>")
        workflow['hdfsBytesReadTotal'] = sigma_hdfs_read_bytes
        workflow['waitTimeTotal'] = sigma_wait_time
        workflow['cpuMillisecondsTotal'] = sigma_cpu_ms
        workflow['mapsTotal'] = sigma_maps_total
        workflow['reducesTotal'] = sigma_reduces_total
        workflow['killedReduceAttempts'] = sigma_killed_reduce_attempts
        workflow['killedMapAttempts'] = sigma_killed_map_attempts
        workflow['offSwitchHdfsBytesReadTotal'] = sigma_hdfs_read_bytes_off_switch
        workflow['rackLocalHdfsBytesReadTotal'] = sigma_hdfs_read_bytes_rack_local
        workflow['nodeLocalHdfsBytesReadTotal'] = sigma_hdfs_read_bytes_node_local
        if sigma_maps_total > 0:
            workflow['weightedAvgMapTime'] = int(wSumMapTime / sigma_maps_total)
        if sigma_reduces_total > 0:
            workflow['weightedAvgReduceTime'] = int(wSumReduceTime / sigma_reduces_total)
            workflow['weightedAvgShuffleTime'] = int(wSumShuffleTime /sigma_reduces_total)
            workflow['weightedAvgMergeTime'] = int(wSumMergeTime / sigma_reduces_total)
        collectd.info("<=============== End of metrics fun =============>")
