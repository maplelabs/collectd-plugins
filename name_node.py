import collectd
from http_request import *
import json
from constants import *
import time
from utils import *
from copy import deepcopy

class Namenode:
    def __init__(self):
        self.namenode = None
        self.port = None
        self.interval = 0

    def read_config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            elif children.key == NAMENODE:
                self.namenode = children.values[0]
            elif children.key == NAMENODE_PORT:
                self.port = children.values[0]

    def remove_dot(self, doc, field):
        new_field = '_' + field.split('.')[0] + '_' + field.split('.')[1].lower()
        doc[new_field] = doc.pop(field)


    def get_name_node_stats(self):

        location = self.namenode
        port = self.port
        path = "/jmx?qry=Hadoop:service=NameNode,name={}".format('JvmMetrics')
        json_name_node = http_request(location, port, path, scheme='http')
        if json_name_node is not None:
            json_name_node = json_name_node['beans']
            json_name_node[0]['_documentType'] = "nameNodeStats" + 'JvmMetrics'
        else:
            return None
        hostname = json_name_node[0]['tag.Hostname']

        for a in ['FSNamesystemState', 'FSNamesystem', 'RpcActivityForPort8020']:
            path = "/jmx?qry=Hadoop:service=NameNode,name={}".format(a)
            json_doc = http_request(location, port, path, scheme='http')
            try:
                if json_doc['beans'] == []:
                    continue
                doc = json_doc['beans'][0]
            except KeyError as e:
                collectd.error("Plugin Name_node: Error ", e)
                return None
            if 'TopUserOpCounts' in doc:
                doc.pop('TopUserOpCounts')
            if 'tag.Hostname' not in doc:
                doc['tag.Hostname'] = hostname
            else:
                doc['_tag_Hostname'] = doc.pop('tag.Hostname')
            doc['time'] = int(time.time())
            doc['_documentType'] = "nameNodeStats" + a

            for f in doc.keys():
                if '.' in f:
                    self.remove_dot(doc, f)
            json_name_node.append(doc)
        return json_name_node

    @staticmethod
    def add_common_params(namenode_dic, doc_type):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        hostname = gethostname()
        timestamp = int(round(time.time()))

        namenode_dic[HOSTNAME] = hostname
        namenode_dic[TIMESTAMP] = timestamp
        namenode_dic[PLUGIN] = 'name_node'
        namenode_dic[ACTUALPLUGINTYPE] = 'name_node'
        namenode_dic[PLUGINTYPE] = doc_type

    @staticmethod
    def dispatch_data(doc):
        """Dispatches dictionary to collectd."""
        collectd.info("Plugin Name_node: Values: %s" %(doc)) # pylint: disable=E1101
        dispatch(doc)


    def collect_data(self):
        """Collects all data."""
        namenode_dics = self.get_name_node_stats()
        for doc in namenode_dics:
            self.add_common_params(doc, doc['_documentType'])
            self.dispatch_data(deepcopy(doc))

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

namenodeinstance = Namenode()
collectd.register_config(namenodeinstance.read_config) # pylint: disable=E1101
collectd.register_read(namenodeinstance.read_temp) # pylint: disable=E1101
