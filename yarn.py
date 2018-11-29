import time
from copy import deepcopy
import collectd
from constants import * # pylint: disable=W
from utils import * # pylint: disable=W
from http_request import * # pylint: disable=W

class YarnStats:
    def __init__(self):
        """Plugin object will be created only once and \
           collects yarn statistics info every interval."""
        self.resource_manager = None
        self.yarn_node = None

    def read_config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            elif children.key == YARN_NODE:
                self.yarn_node = children.values[0]
            elif children.key == RESOURCE_MANAGER_PORT:
                self.resource_manager = children.values[0]

    def remove_dot(self, doc, field):
        """Function to remove dots in the field"""
        new_field = '_' + field.split('.')[0] + '_' + field.split('.')[1].lower()
        doc[new_field] = doc.pop(field)


    def get_yarn_stats(self):
        """Function to get yarn statistics"""
        location = self.yarn_node
        port = self.resource_manager
        path = "/jmx?qry=Hadoop:service=ResourceManager,name={}".format('JvmMetrics')
        json_yarn_node = http_request(location, port, path, scheme='http')
        if json_yarn_node is not None:
            json_yarn_node = json_yarn_node['beans']
            json_yarn_node[0]['time'] = int(time.time())
            json_yarn_node[0]['_documentType'] = "yarnStats" + 'JvmMetrics'
        else:
            return []
        hostname = json_yarn_node[0]['tag.Hostname']

        for name in ['RpcActivityForPort8031', 'RpcActivityForPort8032', \
                     'RpcActivityForPort8033', 'RpcActivityForPort8025', \
                     'RpcDetailedActivityForPort8050', 'QueueMetrics,q0=root', 'ClusterMetrics']:
            path = "/jmx?qry=Hadoop:service=ResourceManager,name={}".format(name)
            json_doc = http_request(location, port, path, scheme='http')
            if json_doc is None:
                continue
            try:
                if json_doc['beans'] == []:
                    continue
                doc = json_doc['beans'][0]
            except KeyError as error:
                collectd.error("Plugin yarn_stats: Error ", error)
                return None
            if 'tag.Hostname' not in doc:
                doc['tag.Hostname'] = hostname
            doc['_tag_hostname'] = doc.pop('tag.Hostname')
            doc['time'] = int(time.time())
            if 'Rpc' in name:
                doc['_documentType'] = "yarnStats" + 'RpcActivity'
            else:
                doc['_documentType'] = "yarnStats" + name.split(',')[0]
            for field in doc.keys():
                if '.' in field:
                    self.remove_dot(doc, field)

            json_yarn_node.append(doc)
        return json_yarn_node

    @staticmethod
    def add_common_params(namenode_dic, doc_type):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        hostname = gethostname()
        timestamp = int(round(time.time()))

        namenode_dic[HOSTNAME] = hostname
        namenode_dic[TIMESTAMP] = timestamp
        namenode_dic[PLUGIN] = 'yarn'
        namenode_dic[ACTUALPLUGINTYPE] = 'yarn'
        namenode_dic[PLUGINTYPE] = doc_type

    @staticmethod
    def dispatch_data(doc):
        """Dispatches dictionary to collectd."""
        collectd.info("Plugin Yarn_Stats: Values: %s" %(doc)) # pylint: disable=E1101
        dispatch(doc)


    def collect_data(self):
        """Collects all data."""
        namenode_dics = self.get_yarn_stats()
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

namenodeinstance = YarnStats()
collectd.register_config(namenodeinstance.read_config) # pylint: disable=E1101
collectd.register_read(namenodeinstance.read_temp) # pylint: disable=E1101
