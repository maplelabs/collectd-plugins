"""Python plugin for collectd to fetch oozie workflow statistics information."""

#!/usr/bin/python
import operator
import signal # pylint: disable=unused-import
import time
from datetime import datetime # pylint: disable=W
import json
from copy import deepcopy
from multiprocessing.dummy import Pool as ThreadPool
import requests
import collectd
import redis
from metrics import * # pylint: disable=W
from rest_api import * # pylint: disable=W
from utils import * # pylint: disable=W
from buildData import * # pylint: disable=W
from constants import * # pylint: disable=W
from utilities import * # pylint: disable=W

class Oozie:
    """Plugin object will be created only once and collects oozie statistics info every interval."""
    def __init__(self):
        """Initializes interval, oozie server, Job history and Timeline server details"""
        self.ooziehost = None
        self.oozieport = None
        self.timeline_server = None
        self.timeline_port = None
        self.job_history_server = None
        self.job_history_port = None
        self.resource_manager = None
        self.resource_manager_port = None
        self.interval = None
        self.with_threading = True
        self.workflows_processed = 0
        self.wfs_processed = 0
        self.pool = None
        self.thread_count = 15
        self.url = None
        self.results = []


    def read_config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            elif children.key == OOZIEHOST:
                self.ooziehost = children.values[0]
            elif children.key == OOZIEPORT:
                self.oozieport = children.values[0]
            elif children.key == JOB_HISTORY_SERVER:
                self.job_history_server = children.values[0]
            elif children.key == JOB_HISTORY_PORT:
                self.job_history_port = children.values[0]
            elif children.key == RESOURCE_MANAGER:
                self.resource_manager = children.values[0]
            elif children.key == RESOURCE_MANAGER_PORT:
                self.resource_manager_port = children.values[0]
            elif children.key == TIMELINE_SERVER:
                self.timeline_server = children.values[0]
            elif children.key == TIMELINE_PORT:
                self.timeline_port = children.values[0]

    def get_redis_conn(self):
        """Function to connect redis database"""
        try:
            redis_obj = redis.Redis(host='localhost', port=6379, password=None)
            return redis_obj
        except:
            collectd.error("Plugin Oozie: Unable to connect redis") # pylint: disable=no-member
            return None

    def read_from_redis(self):
        """Function to read data from redis database"""
        redis_obj = self.get_redis_conn()
        if not redis_obj:
            return None
        if not redis_obj.get("workflows"):
            redis_obj.set("workflows", json.dumps({"workflows": []}))
        return json.loads(redis_obj.get("workflows"))

    def write_to_redis(self, workflow, index=-1):
        """Function to write data into redis"""
        workflows_data = self.read_from_redis()
        if not workflows_data:
            return
        if index == -1:
            workflows_data["workflows"].append(workflow)
        else:
            workflows_data["workflows"][index] = workflow
        redis_obj = self.get_redis_conn()
        redis_obj.set("workflows", json.dumps(workflows_data))


    def prepare_workflow(self, workflow):
        """Function to convert workflow details """
        return {
            "wfName": workflow['appName'] if 'appName' in workflow else workflow['wfName'],
            "wfId": workflow['id'] if 'id' in workflow else workflow['wfId'],
            "lastModTime": get_unix_timestamp(workflow['lastModTime']) \
                           if workflow['lastModTime'] is not None else None,
            "createdTime": get_unix_timestamp(workflow['createdTime']) \
                           if workflow['createdTime'] is not None else None,
            "startTime": get_unix_timestamp(workflow['startTime']) \
                         if workflow['startTime'] is not None else 0,
            "endTime": get_unix_timestamp(workflow['endTime']) \
                       if workflow['endTime'] is not None else 0,
            "status": workflow['status'],
            "time": int(math.floor(time.time())),
            "_plugin": "oozie",
            "_documentType": "oozieWorkflows",
            "_tag_appName": "oozie",
            "wfSchedulingDelay":  workflow['wfSchedulingDelay'] \
                                  if "wfSchedulingDelay" in workflow else None,
            "jobSchedulingDelay": workflow['jobSchedulingDelay'] \
                                  if "jobSchedulingDelay" in workflow else None,
            "workflowMonitorStatus": "init"
        }

    def prepare_workflow_action_data(self, action, yarnjobid, workflowId, workflowName):
        """Function to convert workflow action details"""
        return {
            "wfId": workflowId,
            "wfName": workflowName,
            "wfaName": action['name'],
            "wfaId": action['id'],
            "startTime": get_unix_timestamp(action['startTime']) \
                         if action['startTime'] is not None else 0,
            "endTime": get_unix_timestamp(action['endTime']) \
                       if action['endTime'] is not None else 0,
            "externalId": action['externalId'],
            "externalChildID": yarnjobid,
            "status": action['status'],
            "externalStatus": action['externalStatus'],
            "errorCode": action["errorCode"],
            "type": action['type'],
            "time": int(math.floor(time.time())),
            "retries": action['retries'],
            "_plugin": "oozie",
            "_documentType": "oozieWorkflowActions",
            "_tag_appName": "oozie"
        }

    def prepare_metrics(self, metrics):
        """Function to truncate jobStats document"""
        metrics_update = {}
        if metrics["_documentType"] == 'jobStats':
            metrics_update = jobstats
        for metric in metrics:
            if metric in metrics_update:
                metrics_update[metric] = metrics[metric]
        metrics = metrics_update
        return metrics


    def is_latest_oozie_job(self, lastjobdetails, latestjobdetails):
        """Function to check the Jobi details is already in flatMap file"""
        for workflow in lastjobdetails['workflows']:
            if workflow['wfId'] == latestjobdetails['wfId']:
                return False
        return True

    def change_workflow_status(self, workflow):
        """Function to change status of workflow in json file"""
        workflows = self.read_from_redis()
        if not workflows:
            return
        index = -1
        for i in range(0, len(workflows['workflows'])):
            if workflows['workflows'][i]['wfId'] == workflow['wfId']:
                index = i
                break
        if index != -1:
            self.write_to_redis(workflow, index)


    def processyarnjob(self, yarnjobid, oozieworkflowid, oozieworkflowname, \
                       oozieworkflowactionid, oozieworkflowactionname):
        """Function to get Job details fro yarnjobid"""
        collectd.debug("Plugin Oozie: Processing yarnjobid %s of workflow %s workflowId: %s ActionId:%s ActionName:%s" \
                       %(yarnjobid, oozieworkflowname, oozieworkflowid, oozieworkflowactionid, \
                         oozieworkflowactionname))
        dic_host = {}
        dic_host['timeline_server'] = self.timeline_server
        dic_host['timeline_port'] = self.timeline_port
        dic_host['job_history'] = self.job_history_server
        dic_host['job_port'] = self.job_history_port

        job = yarnjobid
        tpTaskStats = None
        app = yarnjobid.replace("job_", "application_")
        app_info = get_app_info(self.timeline_server, self.timeline_port, app, \
                   oozieworkflowname, oozieworkflowid, oozieworkflowactionid, \
                   oozieworkflowactionname)
        if not app_info:
            return None
        if ('type' not in app_info) or (app_info['type'] != 'MAPREDUCE'):
            return [app_info]
        job_info = get_job_info(self.job_history_server, self.job_history_port, job, \
                   oozieworkflowname, oozieworkflowid, oozieworkflowactionid, \
                   oozieworkflowactionname)
        task_info = get_task_info(self.job_history_server, self.job_history_port, job, \
                    oozieworkflowname, oozieworkflowid, oozieworkflowactionid, \
                    oozieworkflowactionname)
        task_ids = get_task_ids_by_job(self.job_history_server, self.job_history_port, job)
        taskattempt_container_info = get_taskattempt_container_info(dic_host, job, task_ids, \
                                     oozieworkflowname, oozieworkflowid, \
                                     oozieworkflowactionid, oozieworkflowactionname)


        if taskattempt_container_info:
            tasks_map = []
            tasks_reduce = []
            for task in taskattempt_container_info:
                for task_attempt in task:
                    if task_attempt['type'] == 'MAP':
                        tasks_map.append(task_attempt)
                    elif task_attempt['type'] == 'REDUCE':
                        tasks_reduce.append(task_attempt)

            job_info["waitTime"] = get_wait_time(job_info, tasks_reduce, tasks_map)
            find_stragglers_runtime(tasks_map)
            find_stragglers_runtime(tasks_reduce)
            hueristics = {}
            hueristics["tasks_by_start_time"] = group_tasks_by_start_time(job, \
                                                tasks_map + tasks_reduce)
            hueristics["time"] = int(time.time())
            hueristics["jobId"] = job
            hueristics_info = {"hueristics_" + job : hueristics}
            tpTaskStats = calculate_taskcount_by_time_points(job_info, tasks_map + tasks_reduce, \
                                                             wfId=oozieworkflowid, \
                                                             wfaId=oozieworkflowactionid, \
                                                             wfName=oozieworkflowname, \
                                                             wfaName=oozieworkflowactionname)

        yarnJobInfo = []
        if job_info is not None:
            yarnJobInfo.append(job_info)
        if app_info is not None:
            yarnJobInfo.append(app_info)
        if task_info is not None:
            yarnJobInfo.extend(task_info)
        if taskattempt_container_info is not None:
            yarnJobInfo.extend(reduce(operator.concat, taskattempt_container_info))
        if tpTaskStats is not None:
            yarnJobInfo.extend(tpTaskStats)
        return yarnJobInfo


    def process_workflow(self, workflow):
        """Function to get list of oozie workflows"""
        res_json = requests.get(self.url+'/job/%s' %workflow['wfId'])
        if not res_json.ok:
            collectd.error("Unable to get oozie jobs from %s server and status is %s" \
                           %(self.ooziehost, res_json.status_code))
            return
        else:
            res_data = res_json.json()
        collectd.debug("action data is %s" %res_data) # pylint: disable=E1101
        if res_data['status'] == 'SUCCEEDED' or res_data['status'] == 'KILLED':
            for action in res_data['actions']:
                actiondata = {"action": None,
                              "yarnJobs": []}
                if action['externalChildIDs']:
                    childindex = 0
                    for externalChildID in action['externalChildIDs'].split(','):
                        if childindex == 0:
                            workflowActionData = self.prepare_workflow_action_data(action, \
                                                 externalChildID, workflow['wfId'], \
                                                 workflow['wfName'])
                            actiondata['action'] = workflowActionData
                            childindex = childindex + 1
                        if res_data['status'] == "SUCCEEDED":
                            yarnJobInfo = self.processyarnjob(externalChildID, workflow['wfId'], \
                                          workflow['wfName'], action['id'], action['name'])
                            if yarnJobInfo:
                     #       self.results.extend(yarnJobInfo)
                                actiondata['yarnJobs'].append(yarnJobInfo)
                            else:
                                collectd.error("Don't have all info for wfaId %s" % action['id']) # pylint: disable=E1101
                else:
                    workflowActionData = self.prepare_workflow_action_data(action, \
                                         action['externalId'], workflow['wfId'], workflow['wfName'])
                    actiondata['action'] = workflowActionData
                    if action['externalId'] and action['externalId'] != '-' and res_data['status'] == "SUCCEEDED":
                        yarnJobInfo = self.processyarnjob(action['externalId'], workflow['wfId'], \
                                      workflow['wfName'], action['id'], action['name'])
                        if yarnJobInfo:
                            actiondata['yarnJobs'].append(yarnJobInfo)
                    #        self.results.extend(yarnJobInfo)
                        else:
                            collectd.error("Don't have all info for wfaId %s" % action['id']) # pylint: disable=E1101
                if actiondata['action']:
                    calculate_scheduling_delays(res_data, [actiondata])
                    self.results.append(actiondata['action'])
                    if actiondata["yarnJobs"]:
                        self.results.extend(reduce(operator.concat, actiondata["yarnJobs"]))
            res_data = self.prepare_workflow(res_data)
            res_data['workflowMonitorStatus'] = "processed"
            self.change_workflow_status(res_data)
            self.results.append(res_data)
        elif res_data['status'] != "RUNNING" and res_data['status'] != "SUSPENDED":
            wf_data_to_send = self.prepare_workflow(res_data)
            wf_data_to_send['workflowMonitorStatus'] = "processed"
            self.change_workflow_status(wf_data_to_send)
            self.results.append(wf_data_to_send)
        else:
            res_data = self.prepare_workflow(res_data)
            self.results.append(res_data)

    def read_workflows(self):
        """Function to process unprocessed workflows"""
        res_json = requests.get(self.url+'/jobs')
        if not res_json.ok:
            collectd.error("Unable to get oozie jobs from %s server and status is %s" \
                           %(self.ooziehost, res_json.status_code))
            return
        else:
            res_json = res_json.json()
        if not res_json['workflows']:
            return
        data = self.read_from_redis()
        if not data:
            return
        for workflow in res_json['workflows']:
            worklow_data = self.prepare_workflow(workflow)
            if not self.is_latest_oozie_job(data, worklow_data):
                continue
            self.write_to_redis(worklow_data)
        res_data = self.read_from_redis()
        if not res_data:
            return
        res_data = [workflow for workflow in res_data['workflows'] \
                    if workflow['workflowMonitorStatus'] != 'processed']
        if not res_data:
            return
        if self.with_threading:
            thread_pool = self.pool.map_async(self.process_workflow, res_data)
            thread_pool.wait()
        else:
            for workflow in res_data:
                self.process_workflow(workflow)


    @staticmethod
    def add_common_params(oozie_dict, doc_type):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        hostname = gethostname()
        timestamp = int(round(time.time()))

        oozie_dict[HOSTNAME] = hostname
        oozie_dict[TIMESTAMP] = timestamp
        oozie_dict[PLUGIN] = 'oozie'
        oozie_dict[ACTUALPLUGINTYPE] = 'oozie'
        oozie_dict[PLUGINTYPE] = doc_type

    @staticmethod
    def dispatch_data(oozie_dict):
        """Dispatches dictionary to collectd."""
        collectd.info("Plugin Oozie: Values: %s" %(oozie_dict)) # pylint: disable=E1101
        dispatch(oozie_dict)

    def collect_data(self):
        """Collects all data."""
        if self.with_threading:
            self.pool = ThreadPool(self.thread_count)
        self.url = 'http://%s:%s/oozie/v1' %(self.ooziehost, self.oozieport)
        self.results = []
        self.read_workflows()
        for oozie_dict in self.results:
            if oozie_dict['_documentType'] == 'jobStats':
                oozie_dict = self.prepare_metrics(oozie_dict)
            self.add_common_params(oozie_dict, oozie_dict['_documentType'])
            self.dispatch_data(deepcopy(oozie_dict))

    def read(self):
        self.collect_data()

    def read_temp(self):
        """
        Collectd first calls register_read. At that time default interval is taken,
        hence temporary function is made to call, the read callback is unregistered
        and read() is called again with interval obtained from conf by register_config callback.
        """
        collectd.unregister_read(self.read_temp) # pylint: disable=E1101
        collectd.register_read(self.read, interval=int(self.interval)) # pylint: disable=E1101

oozieinstance = Oozie()
collectd.register_config(oozieinstance.read_config) # pylint: disable=E1101
collectd.register_read(oozieinstance.read_temp) # pylint: disable=E1101
