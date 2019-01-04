from library.rest_api import *
from library.elastic import *
from library.kerberos_utils import *
from library.log import configure_logger
import time
from library import graceful_exit
from metrics import *
from library.redis_utils import *

logger = logging.getLogger(__name__)

def get_app_list(last_processed_end_time, pending_apps):

    host = spark2_history_server['host']
    port = spark2_history_server['port']

    path = '/api/v1/applications?status=completed'

    app_details = http_request(host, port, path, scheme=spark2_history_server['scheme'])
    if app_details is None:
        return None

    app_list = []
    for app in app_details:
        if "attempts" in app and len(app["attempts"]) > 0:
            if app['attempts'][0]['endTimeEpoch'] <= last_processed_end_time:
                logger.debug("Found app: {0} whose endTime is less than lastProcessedEndTime: {1} , Slicing the app list to this point".format(app['id'], last_processed_end_time))
                break
        if pending_apps and app["id"] in pending_apps:
            app['retry_attempt_id'] = pending_apps[app['id']]['retry_attempt_id']
            app['number_of_failures'] = pending_apps[app['id']]['number_of_failures']
            logger.debug("A pending app: {0} received a new attempt. Removing it from pending app list ")
            pending_apps.pop(app['id'])
        else:
            app['retry_attempt_id'] = None
            app['number_of_failures'] = 0
        app_list.insert(0, app)


    if pending_apps:
        logger.debug("Pending apps length:{0}, pending_apps:{1}".format(len(pending_apps), pending_apps))
        app_list = app_list + pending_apps.values()
    logger.debug("The applications to process {0}".format(app_list))

    return app_list

def get_spark_apps_processing_status():
    if app_status['use_redis']:
        return read_from_redis("sparkStatus")
    else:
        return get_processing_status("sparkStatus")

def initialize_app():
    log_config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "loggingspark.conf") 
    configure_logger(log_config_file, logging_config['sparkJobs'])

    if kerberos["enabled"]:
        if kinit_tgt_for_user():
            kerberos_initialize()
        else:
            logging.error("Failed to kinit with kerberos enabled. Exiting")
            exit(1)

def run_application(index):
    logger.debug("Iteration {0} Start Time is :{1} ".format(index, time.time()))
    iteration_start_time = time.time()

    collector_status = get_spark_apps_processing_status()
    if collector_status:
        pending_apps = json.loads(collector_status['pendingApps']) if collector_status['pendingApps'] else {}
        last_processed_end_time = collector_status['lastProcessedEndTime']
    elif collector_status is not None:
        pending_apps = {}
        last_processed_end_time = int(time.time() * 1000)
    else:
        index += 1
        logger.error("Unable to get status from elastic . Will retry after {0} seconds".format(index * 30))
        logger.debug("Iteration {0} end Time is :{1} ".format(index, time.time()))
        time.sleep(index * 30)
        return

    logger.debug("The last processing status {0}".format(collector_status))

    app_list = get_app_list(last_processed_end_time, pending_apps)
    if app_list:
        logger.debug("Number of spark applications to process {0}".format(len(app_list)))
        for app in app_list:
            try:
                app_id = app['id']
                retry_attempt_id = app['retry_attempt_id']
                logger.debug("Processing spark application with app_id {0}, app:{1}".format(app_id, app))
                if 'attempts' in app and len(app['attempts']) > 0:
                    if 'startTimeEpoch' in app['attempts'][0]:
                        app_details = get_app_details(app)
                    else:
                        app_details = app['attempts']
                else:
                    logger.error("No Attempts in app:{0} . Skipping it".format(app_id))
                    continue
                logger.debug("App Details for app_id: {0} is :{1}".format(app_id, app_details))
                app_attempt_id = None
                if app_details:
                    for app_attempt in reversed(app_details):  # app attempt are listed in descending order of endtime
                        app_attempt_processing_status = False
                        app_attempts = []
                        executors = []
                        stages = []
                        completed_stages = []
                        tasks = []
                        spark_task_counts_list = []
                        app_attempt_id = app_attempt['appAttemptId']
                        logger.debug("Processing spark application with app_id {0}, app_attempt_id:{1}".format(app_id,
                                                                                                               app_attempt_id))
                        if (retry_attempt_id is None and app_attempt['endTimeEpoch'] < last_processed_end_time) or (
                                retry_attempt_id is not None and int(app_attempt_id) < int(retry_attempt_id)):
                            logger.debug(
                                "Skipping app_attempt_id:{0} of app-id:{1} since endTime:{2} less than lastProcessedEndTime:{3} or attemptId less than retry_attempt_id:{4}".format(
                                    app_attempt['appAttemptId'], app_id, app_attempt['endTimeEpoch'],
                                    last_processed_end_time, retry_attempt_id
                                ))
                            continue

                        app_name = app_attempt['appName']
                        app_attempt_end_time_in_ms = app_attempt['endTimeEpoch']
                        app_attempts.append(app_attempt)
                        executor = get_executors(app_id, app_name, app_attempt_id)
                        if executor is not None:
                            executors.extend(executor)
                        stage = get_stages(app_id, app_name, app_attempt_id)
                        if stage is not None:
                            stages.extend(stage)
                        task = get_tasks_per_stage(app_id, app_name, app_attempt_id)
                        if task is not None:
                            tasks.extend(task)
                        if tasks and stages:
                            completed_stages = [s for s in stages if 'status' in s and s['status'] == "COMPLETE"]
                            for stage in completed_stages:
                                stats = calculate_taskcount_by_time_points(stage, tasks, app_id, app_attempt_id,
                                                                           stage['stageId'],
                                                                           stage['stageAttemptId'], app_name)
                                spark_task_counts_list.append(stats)

                        if app_attempts:
                            for stage in completed_stages:
                                get_stage_task_scheduling_delay(stage, tasks)
                            for a in app_attempts:
                                # find_stragglers_runtime(tasks)
                                aggregate_tasks_metrics(completed_stages, tasks, executors)
                                aggregate_stages_metrics(a, completed_stages)
                                get_app_stage_scheduling_delay(a, completed_stages)

                            attempt_post_data = build_bulk_spark_attempt_data(app_attempts, stages, executors)
                            logger.debug("POST Data length:{0}".format(len(attempt_post_data)))
                            post_status = send_bulk_docs_to_elastic(attempt_post_data, indices['spark'])
                        if post_status and len(tasks) > 0:
                            app_attempt_processing_status = build_and_send_bulk_docs_to_elastic(tasks, "sparkTasks",
                                                                                                indices['spark'])
                        elif post_status and len(tasks) == 0:
                            app_attempt_processing_status = True
                        else:
                            app_attempt_processing_status = False

                        if not app_attempt_processing_status:
                            handle_app_processing_failure(app_id, app_attempt_id, pending_apps, app)
                            status_app_id = None
                            status_app_attempt_end_time_in_ms = None
                            update_status(collector_status, status_app_id, status_app_attempt_end_time_in_ms,
                                          pending_apps, app_attempt_id)
                            break
                        else:
                            status_app_id = app_id
                            status_app_attempt_end_time_in_ms = app_attempt_end_time_in_ms
                            send_sparkTaskCounts_for_stages(spark_task_counts_list, app_id)
                            # All attempts processed
                            if app_attempt_id == app_details[0]['appAttemptId']:
                                if app_id in pending_apps:
                                    pending_apps.pop(app_id)
                                    logger.debug("Removed app_id:{0} from pending items".format(app_id))
                                    status_app_id = None
                                    status_app_attempt_end_time_in_ms = None
                                update_status(collector_status, status_app_id, status_app_attempt_end_time_in_ms,
                                              pending_apps, app_attempt_id)
                else:
                    logger.debug("App {0} contains no attempts. Skipping it".format(app_id))
            except Exception as e:
                logger.exception("Failed to process Application with app-id:{0}".format(app_id))
                handle_app_processing_failure(app_id, app_attempt_id, pending_apps, app)
                update_status(collector_status, '0', None, pending_apps, app_attempt_id)
    elif not collector_status:
        update_status(collector_status, '0', last_processed_end_time, None, 'NA')
    else:
        logger.debug("No new applications to process")
    logger.debug("Iteration {0} end Time is :{1} ".format(index, time.time()))
    iteration_end_time = time.time()
    logger.debug("Time Taken for iteration is {0} seconds".format(iteration_end_time - iteration_start_time))

def main():
    initialize_app()
    index = 0
    while True:
        try:
            run_application(index)
        except Exception as e:
            logger.exception("Iteration Failed")
        finally:
            index = index + 1
            time.sleep(30)


def update_status(collector_status, status_app_id, status_app_attempt_end_time_in_ms, pending_apps, app_attempt_id):
    if app_status['use_redis']: # redis replaces the value. so we cannot just update some of it
        lastUpdatedStatus = read_from_redis('sparkStatus')
        if not status_app_attempt_end_time_in_ms:
            status_app_attempt_end_time_in_ms = lastUpdatedStatus['lastProcessedEndTime']

    run_status = build_spark_apps_status(status_app_id, status_app_attempt_end_time_in_ms,
                                         pending_apps)

    if app_status['use_redis']:
        result = write_to_redis("sparkStatus", json.dumps(run_status))
    else:
        if collector_status:
            result = update_document_in_elastic(data={"doc": run_status},
                                                documentId=collector_status['id'],
                                                index=indices['workflowmonitor'])
        else:
            result = write_document_to_elastic(run_status,
                                               index=indices['workflowmonitor'])
    if result:
        logger.debug(
            "Successfully pushed app attempt:{0} for application:{1} to elastic".format(
                app_attempt_id, status_app_id))
    else:
        logger.error(
            "Failed to push app attempt:{0} for application:{1} to elastic".format(
                app_attempt_id, status_app_id))

def send_sparkTaskCounts_for_stages(task_points_list,  app_id):
    if task_points_list:
        post_data_taskpoints = ""
        stat_count = 0
        index = 0
        for task_points in task_points_list:
            if task_points:
                post_data_taskpoints += build_spark_taskpoints(task_points)
                stat_count += len(task_points)
            if stat_count / tasks_by_time['numOfDataPoints'] == 20 or index == len(task_points_list) - 1:
                stat_post_status = send_bulk_docs_to_elastic(post_data_taskpoints,
                                                             indices['spark'])
                if stat_post_status:
                    logger.debug(
                        "Successfully posted sparkTaskCounts for application:{0}",
                        app_id)
                else:
                    logger.error(
                        "Failed to post sparkTaskCounts for application:{0}", app_id)
                stat_count = 0
                post_data_taskpoints = ""
            index = index + 1


def handle_app_processing_failure(app_id, app_attempt_id, pending_apps, app):
    app["retry_attempt_id"] = app_attempt_id
    app["number_of_failures"] += 1

    if app['number_of_failures'] <= 3:
        pending_apps[app_id] = app
    else:
        pending_apps.pop(app_id, None)

    logger.debug(
        "handle_app_processing_failure app_id:{0}, app_attempt_id:{1} pending_apps:{2} num_of_fails:{3}".format(app_id, app_attempt_id,
                                                                                               pending_apps, app['number_of_failures']))




if __name__ == '__main__':
    main()
