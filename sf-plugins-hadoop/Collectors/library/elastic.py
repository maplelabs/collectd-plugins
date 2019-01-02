from buildData import *
import json
import traceback
from http_request import *
import logging

logger = logging.getLogger(__name__)


def read_runstatus_from_elastic():
    url = "http://%s:%s/%s/%s"
    res_json = None
    try:
        es_url = url % (elastic['host'], elastic['port'], indices['workflowmonitor'], '_search')
        logger.debug("Reading from Elastic Search %s" % es_url)
        res = requests.get(es_url)
        if res.status_code >= 200 and res.status_code <=300:
            logger.debug("Reading from Elastic Search %s is successful" % elastic['host'])
            res_json = res.json()
            #logger.debug(res_json)
        else:
            logger.debug("Reading from Elastic Search %s failed " % elastic['host'])
    except Exception as e:
        res = 'Error Writting to ES => ' + traceback.format_exc().splitlines()[-1]
        logger.error("Exception in the write elastic due to %s" % res)
    return res_json

def write_document_to_elastic(document, index=indices['workflow']):
    # To send the data to Elasticsearch
    url = "http://%s:%s/%s/%s"
    headers = {'content-type': 'application/json'}
    timeout = 30
    result = None
    try:
        es_url = url % (elastic['host'], elastic['port'], index, '_doc')
        logger.debug("Writing to Elastic Search %s" % elastic['host'])
        res = requests.post(es_url, data=json.dumps(document), headers=headers, timeout=timeout)
        if res.status_code >= 200 and res.status_code <=300:
            logger.debug("Writing to Elastic Search %s is successful" % elastic['host'])
            result = res.json()
            #logger.debug(result)
        else:
            logger.debug("Writing to Elastic Search %s failed " % elastic['host'])
    except Exception as e:
        logger.exception("Exception in the write elastic due to ")

    return result

def update_document_in_elastic(data, documentId, index=indices['workflow']):
    # To send the data to Elasticsearch
    url = "http://%s:%s/%s/_doc/%s/_update"
    headers = {'content-type': 'application/json'}
    timeout = 30
    result = False
    try:
        es_url = url % (elastic['host'], elastic['port'], index, documentId)
        logger.debug("Updating status to Elastic Search %s" % elastic['host'])
        res = requests.post(es_url, data=json.dumps(data), headers=headers, timeout=timeout)
        if res.status_code >= 200 and res.status_code <=300:
            logger.debug("Updating status to Elastic Search %s is successful" % elastic['host'])
            #logger.debug(res.json())
            result = True
        else:
            logger.debug("Updating to Elastic Search {0} failed statuscode:{1} res:{2}".format(elastic['host'], res.status_code, res.content))
    except Exception as e:
        logger.error("Exception in the write elastic due to %s" % e)

    return result

def read_document_to_elastic(document_id, index=indices['workflow']):
    # To send the data to Elasticsearch
    url = "http://%s:%s/%s/%s/%s"
    timeout = 30
    res_json = None
    try:
        es_url = url % (elastic['host'], elastic['port'], index, '_doc', document_id)
        logger.debug("Reading from Elastic Search %s" % elastic['host'])
        res = requests.get(es_url, timeout=timeout)
        if res.status_code >= 200 and res.status_code <=300:
            logger.debug("Reading from Elastic Search %s is successful" % elastic['host'])
            logger.debug(res.json())
            res_json = res.json()
        else:
            logger.debug("Reading from Elastic Search %s failed " % elastic['host'])
    except Exception as e:
        logger.debug("Exception in the write elastic due to %s" % e)
        res = 'Error Reading from ES => ' + traceback.format_exc().splitlines()[-1]
    return res_json


def bulk_write_to_elastic(document_list, index = indices['workflow']):
    bulk_data = generate_bulk_data(document_list, type="workflow")
    url = "http://%s:%s/%s/%s/%s"
    headers = {'content-type': 'application/json'}
    timeout = 30
    result = False
    try:
        es_url = url % (elastic['host'], elastic['port'], index, '_doc', '_bulk')
        logger.debug("Bulk Writing to Elastic Search %s" % elastic['host'])
        res = requests.post(es_url, data=bulk_data, headers=headers, timeout=timeout)
        if res.status_code >= 200 and res.status_code <= 300:
            logger.debug("Bulk Writing to Elastic Search %s is successful" % elastic['host'])
            logger.debug(res.json())
            result = True
        else:
            logger.debug("Bulk Writing to Elastic Search %s failed " % elastic['host'])
    except Exception as e:
        logger.error("Exception in the write elastic due to %s" % e)


    return result

def send_bulk_docs_to_elastic(data, index):
    location = "/{0}/_doc/_bulk".format(index)
    headers = {'content-type': 'application/json'}
    return http_post(elastic['host'], elastic['port'], data=data, path=location, headers=headers,
                    scheme=elastic['scheme'])


def build_and_send_bulk_docs_to_elastic(docs, type, index):
    task_index = 0
    increments = 500
    end_slice = 0
    all_posted = True
    try:
        logger.debug("end slice {0}, number of docs:{1}".format(end_slice, len(docs)))
        while end_slice < len(docs):
            start_slice = task_index * increments
            end_slice = (task_index + 1) * increments if ((task_index + 1) * increments) < len(docs) else len(docs)
            task_index += 1
            bulk_task_post_data = generate_bulk_data(docs[start_slice:end_slice], type)
            task_post_status = send_bulk_docs_to_elastic(bulk_task_post_data, index)
            if task_post_status:
                logger.debug("Successfully posted tasks {0} to {1}".format(start_slice, end_slice))
            else:
                logger.error("Failed to post tasks {0} to {1}",
                             start_slice, end_slice)
                all_posted = False
                break
    except Exception as e:
        logger.exception(e)
        all_posted = False

    if all_posted:
        logger.debug("All documents of type:{0} posted successfully".format(type))

    return all_posted

def send_bulk_workflow_to_elastic(workflowData, post_data, index = indices['workflow']):
    bulk_data = build_bulk_data_for_workflow(workflowData, post_data)
    #logger.info("Workflow info being written to elastic {0}".format(bulk_data))
    url = "%s://%s:%s/%s/%s/%s"
    headers = {'content-type': 'application/json'}
    timeout = 30
    result = False
    try:
        es_url = url % (elastic['scheme'], elastic['host'], elastic['port'], index, '_doc', '_bulk')
        logger.debug("Bulk Writing to Elastic Search %s" % elastic['host'])
        res = requests.post(es_url, data=bulk_data, headers=headers, timeout=timeout)
        if res.status_code >= 200 and res.status_code <= 300:
            logger.debug("Bulk Writing to Elastic Search %s is successful" % elastic['host'])
            #logger.debug(res.json())
            result = True
        else:
            logger.debug("Bulk Writing to Elastic Search %s failed " % elastic['host'])
    except Exception as e:
        logger.exception("Exception in the write elastic")
    return result


def search_workflows_in_elastic(index=indices['workflow']):
    url = "http://%s:%s/%s/_doc/%s"
    params = {"q" : "workflowMonitorStatus:init",
              "sort": "createdTime:asc",
              "from": 0,
              "size": 500}
    timeout = 30
    res_json = None
    try:
        es_url = url % (elastic['host'], elastic['port'], index, '_search')
        logger.debug("Reading from Elastic Search %s" % elastic['host'])
        res = requests.get(es_url, params=params, timeout=timeout)
        if res.status_code >= 200 and res.status_code <= 300:
            logger.debug("Reading from Elastic Search %s is successful" % elastic['host'])
            res_json = res.json()
        else:
            logger.debug("Reading from Elastic Search %s failed " % elastic['host'])
    except Exception as e:
        logger.error("Exception in the write elastic due to %s" % e)
    return res_json




def get_processing_status(type):
    location = "/{0}/_search".format(indices['workflowmonitor'])
    headers = {'content-type': 'application/json'}
    data = {
        "query": {
            "bool": {
                "must": [{
                        "match": {
                            "type": type
                        }
                    }
                ]
            }
        }
    }
    res = http_post(elastic['host'], elastic['port'], data=json.dumps(data), path=location,
                    scheme=elastic['scheme'] , headers=headers)

    if res:
        logger.debug(res)
        if res['hits']['total'] == 1:
            res['hits']['hits'][0]['_source']["id"] = res['hits']['hits'][0]['_id']
            return res['hits']['hits'][0]['_source']
        elif res['hits']['total'] == 0:
            return {}

    return res

def send_doc_list_to_es(doc_list, indice):

    for doc in doc_list:
        send_to_elasticsearch(json.dumps(doc), indice)

def send_to_elasticsearch(data, indice):

    location = elastic['host']
    port = elastic['port']
    path = "/{0}/_doc".format(indice)
    headers = {'Content-Type': 'application/json'}
    res = None

    if isinstance(data, list):
        for doc in data:
            res = http_post(location, port, path, doc, headers, scheme=elastic['scheme'])
    else:
        res = http_post(location, port, path, data, headers, scheme=elastic['scheme'])

    #logger.debug("Response for post to elasticsearch: {0}".format(res))
    return res





