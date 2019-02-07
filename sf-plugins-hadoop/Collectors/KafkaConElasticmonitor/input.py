"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""

confluent_kafka_rest_server = {
    "scheme": "http",
    "hosts": ["datanode-0", "datanode-1", "datanode-2"],
    "port": "8083"
  }

elastic =  {
    "scheme": "http",
    "hosts": ["10.81.1.212"],
    "port": "9200"
  }

kafka_sink_name = 'elasticsearch-sink'

logging_config = {
   "monitorkafkaelk" : "./monitorkafkaelk.log"
}

status_file = "lastProcessedStatus"
