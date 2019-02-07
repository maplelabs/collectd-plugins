"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""

import requests
import time
import json
import subprocess
from pywebhdfs.webhdfs import PyWebHdfsClient
from multiprocessing.dummy import Pool as ThreadPool
from tornado.websocket import WebSocketClosedError

# Constants
LOG_ROOT_DIR = "/log_query_results"

class SparkHandler(object):

    def __init__(self):
        pass

    def executeJob(self, queryData, queryId):

        appId = ""
        outputDir = "%s/%s" % (LOG_ROOT_DIR, queryId)
        startTime = queryData["start_time"]
        endTime = queryData["end_time"]
        logLevel = queryData["log_level"]

        url = 'http://10.81.1.242:8998/batches'
        headers = {'content-type': 'application/json'}
        payload = {"file": "/apple_log_management_jars/kafka-spark-query_2.11-1.0.jar","className":"apple.org.hadoop.KafkaQuery","numExecutors":3,"executorCores":3,"executorMemory":"4G","args":[startTime,endTime,logLevel,outputDir], "name": queryId}
            
        requests.post(url, data=json.dumps(payload), headers=headers)

    def getRunningAppInfo(self, jobName):
        url = 'http://%s:%s%s' % ('10.81.1.160', '8088', '/ws/v1/cluster/apps?state=RUNNING')
        appInfo = None
        yarn_list = requests.get(url)
        yarn_list = json.loads(yarn_list.text)
        if yarn_list['apps'] != None:
            for app in yarn_list['apps']['app']:
                if jobName == app['name']:
                    appInfo = app

        return appInfo

    def getStatusCount(self, statusData):
        totalTasks = 0
        completedTasks = 0
        for job in statusData:
            totalTasks = job['numTasks']
            completedTasks = job['numCompletedTasks']
            
        return totalTasks, completedTasks

    def closeSocket(self, socketHandler, messages):
        try:
            for msg in messages:
                socketHandler.write_message(msg)
            socketHandler.close()
        except Exception as ex:
            print(str(ex))

    def checkInHistoryServer(self, id):
        ret_val = False
        url = '%s/%s' % ('http://10.81.1.215:18081/api/v1/applications', id)
        resp = requests.get(url).json()
        for attempt in resp["attempts"]:
            if attempt['completed'] == True:
                ret_val = True

        return ret_val

    def getQueryAppInfo(self, socketHandler, jobName):
        queryAppInfo = None
        (timeOutSecs, timeOutCount) = (10,0)
        while True:
            queryAppInfo = self.getRunningAppInfo(jobName)
            if queryAppInfo is not None:
                break
            if timeOutSecs == timeOutCount:
                self.closeSocket(socketHandler, ['0', 'Could not find a running job'])
                queryAppInfo = None
                break
            timeOutCount +=1
            time.sleep(1)
        return queryAppInfo

    def sendProgress(self, socketHandler, jobName):
        queryAppInfo = None
        queryAppInfo = self.getQueryAppInfo(socketHandler, jobName)

        if queryAppInfo:
            trackingUrl = queryAppInfo['trackingUrl']
            id = queryAppInfo['id']
            sparkJobDetailsUrl = '%s%s/%s/%s' % (trackingUrl, 'api/v1/applications', id, 'jobs')
            lastProgressPercentage = 0

            while True:
                try:
                    sparkJobDetailsResp = requests.get(sparkJobDetailsUrl)
                    sparkJobDetails = json.loads(sparkJobDetailsResp.text)
                    if sparkJobDetails:
                        totalTasks, completedTasks = self.getStatusCount(sparkJobDetails)
                        progressPercentage = int(completedTasks/(totalTasks/100))
                        if progressPercentage > lastProgressPercentage:
                            print(progressPercentage)
                            socketHandler.write_message(str(progressPercentage))
                            if progressPercentage ==  100:
                                self.closeSocket(socketHandler, ['Job finished execution'])
                                break
                            lastProgressPercentage = progressPercentage
                except Exception as ex:
                    ex_msg = str(ex)
                    if 'line 1 column 1 (char 0)' in ex_msg:
                        if self.checkInHistoryServer(id):
                            self.closeSocket(socketHandler, ['100', 'Job completed successfully'])
                        else:
                            self.closeSocket(socketHandler, ['0', 'Job failed unexpectedly'])
                        break
                    print("Exception: %s" % ex_msg)
                time.sleep(0.1)

class HdfsHandler(object):
    def __init__(self):
        self._HDFS = PyWebHdfsClient(host='10.81.1.160',port='50070', user_name='hdfs')

    def readFile(self, file):
        dirToRead = "%s/%s" % (LOG_ROOT_DIR, file)
        dataOut = self._HDFS.list_dir(dirToRead)
        fileToRead = "%s/%s" % (dirToRead, dataOut['FileStatuses']['FileStatus'][1]['pathSuffix'])
        return self._HDFS.read_file(fileToRead)
