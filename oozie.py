import collectd
import operator
import signal
import time
from datetime import datetime
import json
import requests
from multiprocessing.dummy import Pool as ThreadPool
from metrics import *
from rest_api import *
from utils import *
from copy import deepcopy

class Oozie:
    def __init__(self):
        self.ooziehost = None
        self.oozieport = None
        self.timeline_server = None
        self.timeline_port = None
        self.job_history_server = None
        self.job_history_port = None
        self.resource_manager = None
        self.resource_manager_port = None
        self.interval = None
        self.with_threading = False
        self.workflows_processed = 0
        self.wfs_processed = 0
        self.pool = None
        self.THREAD_COUNT = 15
        self.url = None       
        self.out_file = '/var/log/hueristics/hadoop-plugin-test/test.json'


    def read_config(self, cfg):
        for children in cfg.children:
            if children.key == 'interval':
                self.interval = children.values[0]
            elif children.key == 'ooziehost':
                self.ooziehost = children.values[0]
            elif children.key == 'oozieport':
                self.oozieport = children.values[0]
            elif children.key == 'job_history_server':
                self.job_history_server = children.values[0]
            elif children.key == 'job_history_port':
                self.job_history_port = children.values[0]
            elif children.key == 'resource_manager':
                self.resource_manager = children.values[0]
            elif children.key == 'resource_manager_port':
                self.resource_manager_port = children.values[0]
            elif children.key == 'timeline_server':
                self.timeline_server = children.values[0]
            elif children.key == 'timeline_port':
                self.timeline_port = children.values[0]

    def get_unix_timestamp(self, date_text):
        dateInput = date_text.rsplit(' ', 1)
        utc = timezone(dateInput[1])
        datetimedata = datetime.strptime(dateInput[0], "%a, %d %b %Y %H:%M:%S")
        utctimestamp = utc.localize(datetimedata)
        #collectd.debug(utctimestamp.tzinfo)
        return int(time.mktime(utctimestamp.timetuple()))

    def prepare_workflow(self, workflow):
        return {
            "wfName": workflow['appName'] if 'appName' in workflow else workflow['wfName'],
            "wfId": workflow['id'] if 'id' in workflow else workflow['wfId'] ,
            "lastModTime": self.get_unix_timestamp(workflow['lastModTime']) if workflow['lastModTime'] is not None else None,
            "createdTime": self.get_unix_timestamp(workflow['createdTime']) if workflow['createdTime'] is not None else None,
            "startTime": self.get_unix_timestamp(workflow['startTime']) if workflow['startTime'] is not None else 0,
            "endTime": self.get_unix_timestamp(workflow['endTime']) if workflow['endTime'] is not None else 0,
            "status": workflow['status'],
            "time": int(math.floor(time.time())),
            "_plugin": "oozie",
            "_documentType": "oozieWorkflows",
            "_tag_appName": "oozie",
            "wfSchedulingDelay":  workflow['wfSchedulingDelay'] if "wfSchedulingDelay" in workflow else None,
            "jobSchedulingDelay": workflow['jobSchedulingDelay'] if "jobSchedulingDelay" in workflow else None,
            "workflowMonitorStatus": "init"
        }

    def prepare_workflow_action_data(self, action, yarnJobId, workflowId, workflowName):
        return {
            "wfId": workflowId,
            "wfName": workflowName,
            "wfaName": action['name'],
            "wfaId": action['id'],
            "startTime": self.get_unix_timestamp(action['startTime']) if action['startTime'] is not None else 0,
            "endTime": self.get_unix_timestamp(action['endTime']) if action['endTime'] is not None else 0,
            "externalId": action['externalId'],
            "externalChildID": yarnJobId,
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


    def is_latest_oozie_job(self, lastjobdetails, latestjobdetails):        
        for workflow in lastjobdetails['workflows']:
            if workflow['wfId'] == latestjobdetails['wfId']:
                return False
        return True

    def read_json(self):
        with open("/opt/collectd/plugins/oozieworkflows.json", "r") as fp:
            data = json.loads(fp.read())
        if 'workflows' not in data:
            data['workflows'] = []
        fp.close()
        return data

    def write_json(self, workflow):
        data = self.read_json()
        with open("/opt/collectd/plugins/oozieworkflows.json", "w") as fp:
            data['workflows'].append(workflow)
            json.dump(data, fp)
        fp.close()

    def change_workflow_status(self, workflow):
        workflows = self.read_json()
        index = -1
        for i in range(0, len(workflows['workflows'])):
            if workflows['workflows'][i]['wfId'] == workflow['wfId']:
                index = i
                break
        if index != -1:
            workflows['workflows'][i] = workflow
            with open("/opt/collectd/plugins/oozieworkflows.json", "w") as fp:
                json.dump(workflows, fp)
            fp.close()


    def processYarnJob(self, yarnJobId, oozieWorkflowId, oozieWorkflowName, oozieWorkflowActionId, oozieWorkflowActionName, post_data):
        collectd.debug("Processing yarnJobId %s of workflow %s workflowId: %s ActionId:%s ActionName:%s" %(yarnJobId, oozieWorkflowName, oozieWorkflowId ,
                                                                 oozieWorkflowActionId, oozieWorkflowActionName ))

        job = yarnJobId
        tpTaskStats = None
        app = yarnJobId.replace("job_", "application_")   
        app_info = get_app_info(self.timeline_server, self.timeline_port,app,oozieWorkflowName, oozieWorkflowId, oozieWorkflowActionId, oozieWorkflowActionName)
        if app_info['type'] != 'MAPREDUCE':
            return app_info
        job_info = get_job_info(self.job_history_server, self.job_history_port, job,oozieWorkflowName, oozieWorkflowId, oozieWorkflowActionId, oozieWorkflowActionName)
        task_info = get_task_info(self.job_history_server, self.job_history_port, job, oozieWorkflowName, oozieWorkflowId, oozieWorkflowActionId, oozieWorkflowActionName)
        task_ids = get_task_ids_by_job(self.job_history_server, self.job_history_port, job)
        taskattempt_container_info = get_taskattempt_container_info(self.job_history_server, self.job_history_port, job, task_ids, oozieWorkflowName, oozieWorkflowId, oozieWorkflowActionId, oozieWorkflowActionName)


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
            #find_stragglers_runtime(tasks_map)
            #find_stragglers_runtime(tasks_reduce)
            hueristics = {}
            #hueristics["mapper_spill"] = find_mapper_spill(tasks_map)
            #hueristics["shuffle_ratio"] = find_shuffle_ratio(tasks_reduce)
            #hueristics["sort_ratio"] = find_sort_ratio(tasks_reduce)
            #hueristics["map_speed"] = find_map_speed(tasks_map)
            hueristics["tasks_by_start_time"] = group_tasks_by_start_time(job, tasks_map + tasks_reduce)
            hueristics["time"] = int(time.time())
            hueristics["jobId"] = job
            hueristics_info = {"hueristics_" + job : hueristics}
#            write_json_to_file(hueristics_info, self.out_file)
            tpTaskStats = calculate_taskcount_by_time_points(job_info, tasks_map + tasks_reduce,wfId=oozieWorkflowId,
                                                             wfaId=oozieWorkflowActionId, wfName=oozieWorkflowName,
                                                             wfaName=oozieWorkflowActionName)

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
        res_json = requests.get(self.url+'/job/%s' %workflow['wfId'])
        if not res_json.ok:
            collectd.error("Unable to get oozie jobs from %s server and status is %s" %(self.ooziehost, res_json.status_code))
        else:
            res_data = res_json.json()
        collectd.debug("action data is %s" %res_data)
        if res_data['status'] == 'SUCCEEDED' or res_data['status'] == 'KILLED':
            post_data = {"workflow" : workflow,
                         "workflowActions": []}
            processed_data = []
            for action in res_data['actions']:
                actionData = {"action": None,
                              "yarnJobs": []}
                if (action['type'] == ':START:') or (action['type'] == ':END:'):
                    continue
                if action['externalChildIDs']:
                    childIndex = 0
                    for externalChildID in action['externalChildIDs'].split(','):
                        if childIndex == 0:
                            workflowActionData = self.prepare_workflow_action_data(action, externalChildID,
                                                                              workflow['wfId'], workflow['wfName'])
                            actionData['action'] = workflowActionData
                            childIndex = childIndex + 1
                        yarnJobInfo = self.processYarnJob(externalChildID, workflow['wfId'], workflow['wfName'],action['id'], action['name'], post_data)
                        if yarnJobInfo:
                            processed_data.extend(yarnJobInfo)
                            #actionData['yarnJobs'].append(yarnJobInfo)
                        else:
                            collectd.error("Don't have all info for wfaId %s" % action['id'])
                else:
                    workflowActionData = self.prepare_workflow_action_data(action, action['externalId'], workflow['wfId'], workflow['wfName'])
                    actionData['action'] = workflowActionData
                    if action['externalId'] and action['externalId'] != '-':
                        yarnJobInfo = self.processYarnJob(action['externalId'], workflow['wfId'], workflow['wfName'], action['id'], action['name'], post_data)
                        if yarnJobInfo:
#                            actionData['yarnJobs'].append(yarnJobInfo)
                            processed_data.extend(yarnJobInfo)
                        else:
                            collectd.error("Don't have all info for wfaId %s" % action['id'])
                if actionData['action']:
                    processed_data.append(actionData['action'])
#            if post_data["workflowActions"]:
#                res_data['workflowMonitorStatus'] = "processed"
    #            res_data = self.prepare_workflow(res_data)
#                self.change_workflow_status(workflow)
#                post_data["workflowActions"].extend()
#                collectd.info("======== Processed bulk data is %s" %post_data)
#                return post_data
            if processed_data:
                workflow['workflowMonitorStatus'] = "processed"
                self.change_workflow_status(workflow)
                processed_data.append(workflow)
                return processed_data
            elif res_data['status'] == 'SUCCEEDED':
                res_data = self.prepare_workflow(res_data)
                res_data['workflowMonitorStatus'] = "processed"
                self.change_workflow_status(res_data)
                return [res_data]
        elif res_data['status'] != "RUNNING" and res_data['status'] != "SUSPENDED":
            wf_data_to_send = self.prepare_workflow(res_data)
            wf_data_to_send['workflowMonitorStatus'] = "processed"
            self.change_workflow_status(wf_data_to_send)
            return [wf_data_to_send]

    def read_workflows(self):
        res_json = requests.get(self.url+'/jobs')
        if not res_json.ok:
            collectd.error("Unable to get oozie jobs from %s server and status is %s" %(self.ooziehost, res_json.status_code))
        else:
            res_json = res_json.json()
        if len(res_json['workflows']) == 0:
            return None
        data = self.read_json()            
       
        result = []
        for workflow in res_json['workflows']:
            worklow_data = self.prepare_workflow(workflow)
            if not self.is_latest_oozie_job(data, worklow_data):
                continue
            self.write_json(worklow_data)
    
        res_data = self.read_json()
        if self.with_threading:
            r = self.pool.map_async(self.process_workflow,res_data['workflows'], callback=result) 
            r.wait()
            result = r.get()
            result = [r for r in result if r]
            result = reduce(operator.concat, result)
        else:
            for workflow in res_data['workflows']:
                if workflow['workflowMonitorStatus'] == 'processed':
                    continue
                status_data = self.process_workflow(workflow)
                if status_data:
                    result.extend(status_data)
        return result


    @staticmethod
    def add_common_params(oozie_dict, doc_type):
        hostname = gethostname()
        timestamp = int(round(time.time()))

        oozie_dict[HOSTNAME] = hostname
        oozie_dict[TIMESTAMP] = timestamp
        oozie_dict[PLUGIN] = 'oozie'
        oozie_dict[ACTUALPLUGINTYPE] = 'oozie'
        oozie_dict[PLUGINTYPE] = doc_type
        #oozie_dict[PLUGIN_INS] = doc_type

    @staticmethod
    def dispatch_data(oozie_dict):
        collectd.info("Plugin Oozie: Values: " + json.dumps(oozie_dict))
        dispatch(oozie_dict)

    def collect_data(self):
        if self.with_threading:
            self.pool = ThreadPool(self.THREAD_COUNT)
        self.url = 'http://%s:%s/oozie/v1' %(self.ooziehost, self.oozieport)
        oozie_dicts = self.read_workflows()
        for oozie_dict in oozie_dicts:
            collectd.info("Collectd info is %s" %oozie_dict)
            self.add_common_params(oozie_dict, oozie_dict['_documentType'])
            self.dispatch_data(deepcopy(oozie_dict))

    def read(self):
        self.collect_data()

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))

oozieinstance = Oozie()
collectd.register_config(oozieinstance.read_config)
collectd.register_read(oozieinstance.read_temp)
