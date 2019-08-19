import collectd
import signal
import time
import sys
import copy
import os
from os import path
import requests
import traceback
import json
from elasticsearch import Elasticsearch as ESearch
from elasticsearch import RequestsHttpConnection
from elasticsearch import ElasticsearchException as ESException

# user imports
import utils
from constants import *


class ElasticsearchStats(object):
    def __init__(self):
        self.interval = DEFAULT_INTERVAL
        self.host = "127.0.0.1"
        self.es = None
        self.pollCounter = 0
        self.node_name = None
        self.port = None
        self.clusters = None
        self.es_protocol = None
        self.documentsTypes = {}
        self.nodeStatsNodes = []
        self.previousData = {}

    def read_config(self, cfg):
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == PORT:
                self.port = children.values[0]
            if children.key == "es_protocol":
                self.es_protocol = children.values[0]
            if children.key == DOCUMENTSTYPES:
                self.documentsTypes = children.values[0]

    def add_common_params(self, doc, data_dict):
        """Adds TIMESTAMP, PLUGIN, PLUGITYPE to dictionary."""
        timestamp = int(round(time.time()))
        data_dict[TIMESTAMP] = timestamp
        data_dict[PLUGIN] = "elastic"
        data_dict[PLUGINTYPE] = doc
        data_dict[ACTUALPLUGINTYPE] = "elastic"
        # dict_jmx[PLUGIN_INS] = doc
        collectd.info("Plugin elasticsearch: Added common parameters successfully for %s doctype" % doc)

    def conv_b_to_mb(self, input_bytes):
        if input_bytes is None:
            input_bytes = 0
        if input_bytes == 'None':
            input_bytes = 0
        convert_mb = input_bytes / (1024 * 1024)
        return convert_mb

    def ping_server(self):
        '''
        Check if the Host IP is reachable or not from the controller.
        :return: True if host is reachable from controller, false otherwise
        '''
        try:
            if isinstance(self.es, ESearch):
                return self.es.ping()
        except Exception as e:
            collectd.error("Plugin elasticsearch: Error in getting ping for Elasticsearch %s due to %s" % (str(self.es), str(e)))
            return False

    def check_master(self, node_id):
        es_url = "{}://{}:{}/_cat/master".format(str(self.es_protocol), str(self.host), str(self.port))
        response = requests.get(es_url, verify=False)
        for line in response:
            if line.split()[0] == node_id:
                return True

        return False

    def get_nodes(self):
        nodes_list = []
        nodes = self.es.nodes.info()
        nodes_list = nodes['nodes'].keys()
        return nodes_list

    def get_node_stats(self, node_id):
        """
                collect node level statistics
                :param: nodeclient: instance of class elasticsearch.client.nodes.NodesClient
                :param: node: name of the node in the cluster whose stats to be collected.
                :return: dictionary of the collected stats
                """
        collectd.debug('Plugin elasticsearch: Function: collect_node_stats.')
        try:
            stats = self.es.nodes.stats(self.node_name)
            collectd.debug('Plugin elasticsearch: Node stats received: %s' % stats)
            node_stats = {}
            single_node_stats = {}
            nodes_list = stats['nodes'].keys()
        except Exception as err:
            collectd.error("Plugin elasticsearch: Plugin elasticsearch: Error in collecting node stats due to %s" % str(err))

        roles_list = []
        roles = ''
        try:
            roles_list = stats['nodes'][node_id]['roles']
            roles = (",".join(roles_list)).encode('utf8')
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting node roles for node_id %s : '
                              '%s' % (node_id, kerr.message))


        document_count = None
        try:
            document_count = stats['nodes'][node_id]['indices']['docs']['count']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting document count for node_id %s : '
                              '%s' % (node_id, kerr.message))

        store_size_in_bytes = None
        try:
            store_size_in_bytes = stats['nodes'][node_id]['indices']['store']['size_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting store size for node_id %s : '
                              '%s' % (node_id, kerr.message))
        try:
            store_size_in_mb = round(self.conv_b_to_mb(float(store_size_in_bytes)), 2)
        except Exception as err:
            collectd.error('Plugin elasticsearch: Error in converting store_size into mb due to %s' % str(err))
            store_size_in_mb = 0.0

        cache_size = None
        try:
            cache_size = stats['nodes'][node_id]['indices']['query_cache']['cache_size']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting cache size for node_id %s : '
                              '%s' % (node_id, kerr.message))

        field_data_memory_size = None
        try:
            field_data_memory_size = stats['nodes'][node_id]['indices']['fielddata']['memory_size_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting field data memory size for node_id %s : '
                              '%s' % (node_id, kerr.message))
        try:
            field_data_memory_size_in_mb = round(self.conv_b_to_mb(float(field_data_memory_size)), 2)
        except Exception as err:
            collectd.error('Plugin elasticsearch: Error in converting field_data_memory_size into mb due to %s' % str(err))
            field_data_memory_size_in_mb = 0.0

        field_data_evictions = None
        try:
            field_data_evictions = stats['nodes'][node_id]['indices']['fielddata']['evictions']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting field data evictions for node_id %s : '
                              '%s' % (node_id, kerr.message))

        segment_count = None
        try:
            segment_count = stats['nodes'][node_id]['indices']['segments']['count']
        except KeyError as kerr:
                collectd.error('Plugin elasticsearch: Error getting segment count for node_id %s : '
                              '%s' % (node_id, kerr.message))

        os_cpu_percent = None
        try:
            os_cpu_percent = stats['nodes'][node_id]['os']['cpu']['percent']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting os_cpu_percent for node_id %s : '
                              '%s' % (node_id, kerr.message))

        os_mem_total_in_bytes = None
        try:
            os_mem_total_in_bytes = stats['nodes'][node_id]['os']['mem']['total_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting os_mem_total_in_bytes for node_id %s : '
                              '%s' % (node_id, kerr.message))
        try:
            os_mem_total = round(self.conv_b_to_mb(float(os_mem_total_in_bytes)), 2)
        except Exception as err:
            collectd.error('Plugin elasticsearch: Error in converting os_mem_total_in_bytes into mb due to %s' % str(err))
            os_mem_total = 0.0

        os_mem_free_in_bytes = None
        try:
            os_mem_free_in_bytes = stats['nodes'][node_id]['os']['mem']['free_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting os_mem_free_in_bytes for node_id %s : '
                              '%s' % (node_id, kerr.message))

        try:
            os_mem_free = round(self.conv_b_to_mb(float(os_mem_free_in_bytes)), 2)
        except Exception as err:
            collectd.error('Plugin elasticsearch: Error in converting os_mem_free_in_bytes into mb due to %s' % str(err))
            os_mem_free = 0.0

        os_mem_used_in_bytes = None
        try:
            os_mem_used_in_bytes = stats['nodes'][node_id]['os']['mem']['used_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting os_mem_used_in_bytes for node_id %s : '
                              '%s' % (node_id, kerr.message))

        try:
            os_mem_used = round(self.conv_b_to_mb(float(os_mem_used_in_bytes)), 2)
        except Exception as err:
            collectd.error('Plugin elasticsearch: Error in converting os_mem_used_in_bytes into mb due to %s' % str(err))
            os_mem_used = 0.0

        os_mem_free_percent = None
        try:
            os_mem_free_percent = stats['nodes'][node_id]['os']['mem']['free_percent']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting os_mem_free_percent for node_id %s : '
                              '%s' % (node_id, kerr.message))

        os_mem_used_percent = None
        try:
            os_mem_used_percent = stats['nodes'][node_id]['os']['mem']['used_percent']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting os_mem_used_percent for node_id %s : '
                              '%s' % (node_id, kerr.message))

        process_open_fds = None
        try:
            process_open_fds = stats['nodes'][node_id]['process']['open_file_descriptors']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting process_open_fds for node_id %s : '
                              '%s' % (node_id, kerr.message))

        process_cpu_percent = None
        try:
            process_cpu_percent = stats['nodes'][node_id]['process']['cpu']['percent']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting process_cpu_percent for node_id %s : '
                              '%s' % (node_id, kerr.message))

        io_stats_total_operations = None
        try:
            io_stats_total_operations = stats['nodes'][node_id]['fs']['io_stats']['total']['operations']
        except KeyError as kerr:
                collectd.error('Plugin elasticsearch: Error getting io_stats_total_operations for node_id %s : '
                              '%s' % (node_id, kerr.message))

        io_stats_total_read_operations = None
        try:
            io_stats_total_read_operations = stats['nodes'][node_id]['fs']['io_stats']['total']['read_operations']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting io_stats_total_read_operations for node_id %s : '
                              '%s' % (node_id, kerr.message))

        io_stats_total_write_operations = None
        try:
            io_stats_total_write_operations = stats['nodes'][node_id]['fs']['io_stats']['total']['write_operations']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting io_stats_total_write_operations for node_id %s : '
                              '%s' % (node_id, kerr.message))

        io_stats_total_read_kb = None
        try:
            io_stats_total_read_kb = stats['nodes'][node_id]['fs']['io_stats']['total']['read_kilobytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting io_stats_total_read_kb for node_id %s : '
                              '%s' % (node_id, kerr.message))

        io_stats_total_write_kb = None
        try:
            io_stats_total_write_kb = stats['nodes'][node_id]['fs']['io_stats']['total']['write_kilobytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting io_stats_total_write_kb for node_id %s : '
                              '%s' % (node_id, kerr.message))

        server_open = None
        try:
            server_open = stats['nodes'][node_id]['transport']['server_open']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting server_open for node_id %s : '
                              '%s' % (node_id, kerr.message))

        trans_rx_count = None
        try:
            trans_rx_count = stats['nodes'][node_id]['transport']['rx_count']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting trans_rx_count for node_id %s : '
                              '%s' % (node_id, kerr.message))

        trans_rx_size_in_bytes = None
        try:
            trans_rx_size_in_bytes = stats['nodes'][node_id]['transport']['rx_size_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting trans_rx_size_in_bytes for node_id %s : '
                              '%s' % (node_id, kerr.message))

        trans_tx_count = None
        try:
            trans_tx_count = stats['nodes'][node_id]['transport']['tx_count']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting trans_tx_count for node_id %s : '
                              '%s' % (node_id, kerr.message))

        trans_tx_size_in_bytes = None
        try:
            trans_tx_size_in_bytes = stats['nodes'][node_id]['transport']['tx_size_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting trans_tx_size_in_bytes for node_id %s : '
                              '%s' % (node_id, kerr.message))

        http_current_open_conn = None
        try:
            http_current_open_conn = stats['nodes'][node_id]['http']['current_open']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting http_current_open_conn for node_id %s : '
                              '%s' % (node_id, kerr.message))

        http_total_open_conn = None
        try:
            http_total_open_conn = stats['nodes'][node_id]['http']['total_opened']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting http_total_open_conn for node_id %s : '
                              '%s' % (node_id, kerr.message))

        brkers_limit_size_in_bytes = None
        try:
            brkers_limit_size_in_bytes = stats['nodes'][node_id]['breakers']['fielddata']['limit_size_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting brkers_limit_size_in_bytes for node_id %s : '
                              '%s' % (node_id, kerr.message))

        brkers_estimated_size_in_bytes = None
        try:
            brkers_estimated_size_in_bytes = stats['nodes'][node_id]['breakers']['fielddata']['limit_size_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting brkers_estimated_size_in_bytes for node_id %s : '
                              '%s' % (node_id, kerr.message))

        brkers_overhead = None
        try:
            brkers_overhead = stats['nodes'][node_id]['breakers']['fielddata']['overhead']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting brkers_overhead for node_id %s : '
                              '%s' % (node_id, kerr.message))

        brkers_tripped = None
        try:
            brkers_tripped = stats['nodes'][node_id]['breakers']['fielddata']['tripped']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting brkers_tripped for node_id %s : '
                              '%s' % (node_id, kerr.message))

        jvm_heap_usage_in_bytes = None
        try:
            jvm_heap_usage_in_bytes = stats['nodes'][node_id]['jvm'] \
                    ['mem']['heap_used_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm heap usage for node_id %s : '
                              '%s' % (node_id, kerr.message))
        try:
            jvm_heap_usage = round(self.conv_b_to_mb(float(jvm_heap_usage_in_bytes)), 2)
        except Exception as err:
            collectd.error('Plugin elasticsearch: Error in converting jvm_heap_usage due to %s' % str(err))
            jvm_heap_usage = 0.0

        jvm_heap_usage_percent = None
        try:
            jvm_heap_usage_percent = stats['nodes'][node_id]['jvm'] \
                    ['mem']['heap_used_percent']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm heap usage percent for node_id %s : '
                              '%s' % (node_id, kerr.message))

        jvm_heap_committed_in_bytes = None
        try:
            jvm_heap_committed_in_bytes = stats['nodes'][node_id]['jvm'] \
                    ['mem']['heap_committed_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm heap committed for node_id %s : '
                              '%s' % (node_id, kerr.message))

        try:
            jvm_heap_committed = round(self.conv_b_to_mb(float(jvm_heap_committed_in_bytes)), 2)
        except Exception as e:
            collectd.error('Plugin elasticsearch: Error in converting jvm_heap_committed due to %s' % str(e))
            jvm_heap_committed = 0.0

        jvm_heap_max_in_bytes = None
        try:
            jvm_heap_max_in_bytes = stats['nodes'][node_id]['jvm'] \
                    ['mem']['heap_max_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm heap max for node_id %s : '
                              '%s' % (node_id, kerr.message))
        try:
            jvm_heap_max = round(self.conv_b_to_mb(float(jvm_heap_max_in_bytes)), 2)
        except Exception as err:
            collectd.error('Plugin elasticsearch: Error in converting jvm_heap_max due to %s' % str(err))
            jvm_heap_max = 0.0

        jvm_non_heap_used_in_bytes = None
        try:
            jvm_non_heap_used_in_bytes = stats['nodes'][node_id]['jvm'] \
                    ['mem']['non_heap_used_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm non heap used for node_id %s : '
                              '%s' % (node_id, kerr.message))
        try:
            jvm_non_heap_usage = round(self.conv_b_to_mb(float(jvm_non_heap_used_in_bytes)), 2)
        except Exception as err:
            collectd.error('Plugin elasticsearch: Error in converting jvm_non_heap_usage due to %s' % str(err))
            jvm_non_heap_usage = 0.0

        jvm_non_heap_committed_in_bytes = None
        try:
            jvm_non_heap_committed_in_bytes = stats['nodes'][node_id]['jvm'] \
                    ['mem']['non_heap_committed_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm non heap committed for node_id %s : '
                              '%s' % (node_id, kerr.message))
        try:
            jvm_non_heap_committed = round(self.conv_b_to_mb(float(jvm_non_heap_committed_in_bytes)), 2)
        except Exception as err:
            collectd.error('Plugin elasticsearch: Error in converting jvm_non_heap_committed due to %s' % str(err))
            jvm_non_heap_committed = 0.0

        jvm_pools_young_used_in_bytes = None
        try:
            jvm_pools_young_used_in_bytes = stats['nodes'][node_id]['jvm'] \
                    ['mem']['pools']['young']['used_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm pools young usage for node_id %s : '
                              '%s' % (node_id, kerr.message))
        try:
            jvm_pools_young_usage = round(self.conv_b_to_mb(float(jvm_pools_young_used_in_bytes)), 2)
        except Exception as err:
            collectd.error('Plugin elasticsearch: Error in converting jvm_pools_young_usage due to %s' % str(err))
            jvm_pools_young_usage = 0.0

        # jvm_pools_young_max_in_bytes = None
        # try:
        #    jvm_pools_young_max_in_bytes = stats['nodes'][node_id]['jvm'] \
        #                                        ['mem']['pools']['young']['max_in_bytes']
        # except KeyError as kerr:
        #    collectd.error('Plugin elasticsearch: Error getting jvm pools young max usage for node_id %s : '
        #                  '%s' % (node_id, kerr.message))
        # if jvm_pools_young_max_in_bytes:
        #    jvm_pools_young_max = round(self.conv_b_to_mb(float(jvm_pools_young_max_in_bytes)), 2)

        jvm_pools_young_peak_used = None
        try:
            jvm_pools_young_peak_used = stats['nodes'][node_id]['jvm'] \
                    ['mem']['pools']['young']['peak_used_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm pools young peak usage for node_id %s : '
                              '%s' % (node_id, kerr.message))
        try:
            jvm_pools_young_peak_usage = round(self.conv_b_to_mb(float(jvm_pools_young_peak_used)), 2)
        except Exception as err:
            collectd.error('Plugin elasticsearch: Error in converting jvm_pools_young_peak_usage due to %s' % str(err))
            jvm_pools_young_peak_usage = 0.0

          # jvm_pools_young_peak_max_usage = None
            # try:
            #    jvm_pools_young_peak_max_usage = stats['nodes'][node_id]['jvm'] \
            #                                        ['mem']['pools']['young']['peak_max_in_bytes']
            # except KeyError as kerr:
            #    collectd.error('Plugin elasticsearch: Error getting jvm pools young peak max usage for node_id %s : '
            #                  '%s' % (node_id, kerr.message))

            # if jvm_pools_young_peak_max_usage:
            #    jvm_pools_young_peak_max = round(self.conv_b_to_mb(float(jvm_pools_young_peak_max_usage)), 2)

        jvm_pools_survivor_used_in_bytes = None

        try:
            jvm_pools_survivor_used_in_bytes = stats['nodes'][node_id]['jvm'] \
                ['mem']['pools']['survivor']['used_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm pools survivor usage for node_id %s : '
                              '%s' % (node_id, kerr.message))
        try:
            jvm_pools_survivor_usage = round(self.conv_b_to_mb(float(jvm_pools_survivor_used_in_bytes)), 2)
        except Exception as err:
            collectd.error('Plugin elasticsearch: Error in converting jvm_pools_survivor_usage due to %s' % str(err))
            jvm_pools_survivor_usage = 0.0

            # jvm_pools_survivor_max_in_bytes = None
            # try:
            #    jvm_pools_survivor_max_in_bytes = stats['nodes'][node_id]['jvm'] \
            #                                            ['mem']['pools']['survivor']['max_in_bytes']
            # except KeyError as kerr:
            #    collectd.error('Plugin elasticsearch: Error getting jvm pools survivor max usage for node_id %s : '
            #                  '%s' % (node_id, kerr.message))

            # if jvm_pools_survivor_max_in_bytes:
            #    jvm_pools_survivor_max = round(self.conv_b_to_mb(float(jvm_pools_survivor_max_in_bytes)), 2)

        jvm_pools_survivor_peak_used = None
        try:
            jvm_pools_survivor_peak_used = stats['nodes'][node_id]['jvm'] \
                ['mem']['pools']['survivor']['peak_used_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm pools survivor peak usage for node_id %s : '
                          '%s' % (node_id, kerr.message))
        try:
            jvm_pools_survivor_peak_usage = round(self.conv_b_to_mb(float(jvm_pools_survivor_peak_used)), 2)
        except Exception as err:
            collectd.error('Plugin elasticsearch: Error in converting jvm_pools_survivor_peak_usage due to %s' % str(err))
            jvm_pools_survivor_peak_usage = 0.0

            # jvm_pools_survivor_peak_max_usage = None
            # try:
            #    jvm_pools_survivor_peak_max_usage = stats['nodes'][node_id]['jvm'] \
            #                                            ['mem']['pools']['survivor']['peak_max_in_bytes']
            # except KeyError as kerr:
            #    collectd.error('Plugin elasticsearch: Error getting jvm pools survivor peak max usage for node_id %s : '
            #                  '%s' % (node_id, kerr.message))

            # if jvm_pools_survivor_peak_max_usage:
            #    jvm_pools_survivor_peak_max = round(self.conv_b_to_mb(float(jvm_pools_survivor_peak_max_usage)), 2)

        jvm_pools_old_used_in_bytes = None
        try:
            jvm_pools_old_used_in_bytes = stats['nodes'][node_id]['jvm'] \
                ['mem']['pools']['old']['used_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm pools old usage for node_id %s : '
                          '%s' % (node_id, kerr.message))
        try:
            jvm_pools_old_usage = round(self.conv_b_to_mb(float(jvm_pools_old_used_in_bytes)), 2)
        except Exception as err:
            collectd.error('Plugin elasticsearch: Error in converting jvm_pools_old_usage due to %s' % str(err))
            jvm_pools_old_usage = 0.0

            # jvm_pools_old_max_in_bytes = None
            # try:
            #    jvm_pools_old_max_in_bytes = stats['nodes'][node_id]['jvm'] \
            #                                        ['mem']['pools']['old']['max_in_bytes']
            # except KeyError as kerr:
            #    collectd.error('Plugin elasticsearch: Error getting jvm pools old max usage for node_id %s : '
            #                  '%s' % (node_id, kerr.message))

            # if jvm_pools_old_max_in_bytes:
            #    jvm_pools_old_max = round(self.conv_b_to_mb(float(jvm_pools_old_max_in_bytes)), 2)

        jvm_pools_old_peak_used = None
        try:
            jvm_pools_old_peak_used = stats['nodes'][node_id]['jvm'] \
                ['mem']['pools']['old']['peak_used_in_bytes']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm pools old peak usage for node_id %s : '
                          '%s' % (node_id, kerr.message))
        try:
            jvm_pools_old_peak_usage = round(self.conv_b_to_mb(float(jvm_pools_old_peak_used)), 2)
        except Exception as err:
            collectd.error('Plugin elasticsearch: Error in converting jvm_pools_old_peak_used due to %s' % str(err))
            jvm_pools_old_peak_usage = 0.0

            # jvm_pools_old_peak_max_usage = None
            # try:
            #    jvm_pools_old_peak_max_usage = stats['nodes'][node_id]['jvm'] \
            #                                        ['mem']['pools']['old']['peak_max_in_bytes']
            # except KeyError as kerr:
            #    collectd.error('Plugin elasticsearch: Error getting jvm pools old peak max usage for node_id %s : '
            #                  '%s' % (node_id, kerr.message))

            # if jvm_pools_old_peak_max_usage:
            #    jvm_pools_old_peak_max = round(self.conv_b_to_mb(float(jvm_pools_old_peak_max_usage)), 2)

        jvm_threads = None
        try:
            jvm_threads = stats['nodes'][node_id]['jvm']['threads']['count']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm thread count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        jvm_gc = None
        try:
            jvm_gc = stats['nodes'][node_id]['jvm']['gc'] \
                ['collectors']['old']['collection_count']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm garbage collection count for node_id %s: %s',
                              node_id, kerr.message)

        jvm_gct = None
        try:
            jvm_gct = stats['nodes'][node_id]['jvm']['gc'] \
                ['collectors']['old']['collection_time_in_millis']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm garbage collection time for node_id %s: %s',
                          node_id, kerr.message)

            # Jvm garbage collection time in seconds
        if jvm_gct:
            jvm_gct = round(float(jvm_gct) / 1000, 2)
            jvm_young_gc = None
        try:
            jvm_young_gc = stats['nodes'][node_id]['jvm']['gc'] \
                ['collectors']['young']['collection_count']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm young garbage collection count for node_id %s: %s',
                          node_id, kerr.message)

        jvm_young_gct = None
        try:
            jvm_young_gct = stats['nodes'][node_id]['jvm']['gc'] \
                ['collectors']['young']['collection_time_in_millis']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting jvm young garbage collection time for node_id %s: %s',
                              node_id, kerr.message)

            # Jvm young garbage collection time in seconds
        if jvm_young_gct:
            jvm_young_gct = round(float(jvm_young_gct) / 1000, 2)
            thread_pool_get_threads = None
        try:
            thread_pool_get_threads = stats['nodes'][node_id]['thread_pool']['get']['threads']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool get threads count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        thread_pool_get_queue = None
        try:
            thread_pool_get_queue = stats['nodes'][node_id]['thread_pool']['get']['queue']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool get queue count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        thread_pool_get_active = None
        try:
            thread_pool_get_active = stats['nodes'][node_id]['thread_pool']['get']['active']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool get active count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        thread_pool_get_rejected = None
        try:
            thread_pool_get_rejected = stats['nodes'][node_id]['thread_pool']['get']['rejected']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool get rejected count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        thread_pool_get_largest = None
        try:
            thread_pool_get_largest = stats['nodes'][node_id]['thread_pool']['get']['largest']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool get largest count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        thread_pool_get_completed = None
        try:
            thread_pool_get_completed = stats['nodes'][node_id]['thread_pool']['get']['completed']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool get completed count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        thread_pool_index_threads = None
        try:
            thread_pool_index_threads = stats['nodes'][node_id]['thread_pool']['index']['threads']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool index threads count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        thread_pool_index_queue = None
        try:
            thread_pool_index_queue = stats['nodes'][node_id]['thread_pool']['index']['queue']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool index queue count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        thread_pool_index_active = None
        try:
            thread_pool_index_active = stats['nodes'][node_id]['thread_pool']['index']['active']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool index active count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        thread_pool_index_rejected = None
        try:
            thread_pool_index_rejected = stats['nodes'][node_id]['thread_pool']['index']['rejected']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool index rejected count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        thread_pool_index_largest = None
        try:
            thread_pool_index_largest = stats['nodes'][node_id]['thread_pool']['index']['largest']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool index largest count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        thread_pool_index_completed = None
        try:
            thread_pool_index_completed = stats['nodes'][node_id]['thread_pool']['index']['completed']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool index completed count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        thread_pool_search_threads = None
        try:
            thread_pool_search_threads = stats['nodes'][node_id]['thread_pool']['search']['threads']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool search threads count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        thread_pool_search_queue = None
        try:
            thread_pool_search_queue = stats['nodes'][node_id]['thread_pool']['search']['queue']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool search queue count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        thread_pool_search_active = None
        try:
            thread_pool_search_active = stats['nodes'][node_id]['thread_pool']['search']['active']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool search active count for node_id %s : '
                              '%s' % (node_id, kerr.message))

        thread_pool_search_rejected = None
        try:
            thread_pool_search_rejected = stats['nodes'][node_id]['thread_pool']['search']['rejected']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool search rejected count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        thread_pool_search_largest = None
        try:
            thread_pool_search_largest = stats['nodes'][node_id]['thread_pool']['search']['largest']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool search largest count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        thread_pool_search_completed = None
        try:
            thread_pool_search_completed = stats['nodes'][node_id]['thread_pool']['search']['completed']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting thread pool search completed count for node_id %s : '
                          '%s' % (node_id, kerr.message))

        hits = None
        try:
            hits = stats['nodes'][node_id]['indices']['query_cache']['hit_count']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting query cache hit count for node_id %s: %s' %
                           (node_id, kerr.message))

        misses = None
        try:
            misses = stats['nodes'][node_id]['indices']['query_cache']['miss_count']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting query cache miss count for node_id %s: %s' %
                           (node_id, kerr.message))

            # update older stats dictionary as we'd need it for our next poll
        if hits or misses:
            hit_ratio = (hits * 100) / (hits + misses)
        else:
            hit_ratio = 0

        loaded_classes = None
        try:
            loaded_classes = stats['nodes'][node_id]['jvm']['classes']['total_loaded_count']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting count of total jvm loaded classes for node_id %s: %s'
                          % (node_id, kerr.message))

        unloaded_classes = None
        try:
            unloaded_classes = stats['nodes'][node_id]['jvm']['classes']['total_unloaded_count']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting count of total jvm unloaded classes for node_id %s: %s'
                          % (node_id, kerr.message))

            # We need hit and miss count and jvm loaded and unloaded classes since our previous poll
        if 'nodeStats' in self.previousData and not self.pollCounter == 1:
            try:
                hit_count = hits - self.previousData["nodeStats"]['node_hit_count']
                if hit_count < 0:
                    hit_count = 0
                self.previousData["nodeStats"]['node_hit_count'] = hits

                miss_count = misses - self.previousData["nodeStats"]['node_miss_count']
                if miss_count < 0:
                    miss_count = 0
                self.previousData["nodeStats"]['node_miss_count'] = misses

                jvm_loaded_classes = loaded_classes - self.previousData["nodeStats"] \
                    ['node_jvm_loaded_classes']
                if jvm_loaded_classes < 0:
                    jvm_loaded_classes = 0
                self.previousData['node_jvm_loaded_classes'] = loaded_classes

                jvm_unloaded_classes = unloaded_classes - self.previousData["nodeStats"] \
                    ['node_jvm_unloaded_classes']
                if jvm_unloaded_classes < 0:
                    jvm_unloaded_classes = 0
                self.previousData['node_jvm_unloaded_classes'] = unloaded_classes

                search_thrd_pool_completed = thread_pool_search_completed - self.previousData["nodeStats"][
                    "node_thread_pool_search_completed"]
                if search_thrd_pool_completed < 0:
                    search_thrd_pool_completed = 0
                self.previousData["nodeStats"]["node_thread_pool_search_completed"] = thread_pool_search_completed

                get_thrd_pool_completed = thread_pool_get_completed - self.previousData["nodeStats"][
                    "node_thread_pool_get_completed"]
                if get_thrd_pool_completed < 0:
                    get_thrd_pool_completed = 0
                self.previousData["nodeStats"]["node_thread_pool_get_completed"] = thread_pool_get_completed

                index_thrd_pool_completed = thread_pool_index_completed - self.previousData["nodeStats"][
                    "node_thread_pool_index_completed"]
                if index_thrd_pool_completed < 0:
                    index_thrd_pool_completed = 0
                self.previousData["nodeStats"]["node_thread_pool_index_completed"] = thread_pool_index_completed

                rx_count = trans_rx_count - self.previousData["nodeStats"]["node_trans_rx_count"]
                if rx_count < 0:
                    rx_count = 0
                self.previousData["nodeStats"]["node_trans_rx_count"] = trans_rx_count

                tx_count = trans_tx_count - self.previousData["nodeStats"]["node_trans_tx_count"]
                if tx_count < 0:
                    tx_count = 0
                self.previousData["nodeStats"]["node_trans_tx_count"] = trans_tx_count

                rx_size_in_bytes = trans_rx_size_in_bytes - self.previousData["nodeStats"][
                    'node_trans_rx_size_in_bytes']
                if rx_size_in_bytes < 0:
                    rx_size_in_bytes = 0
                self.previousData["nodeStats"]["node_trans_rx_size_in_bytes"] = trans_rx_size_in_bytes

                try:
                    rx_size = round(self.conv_b_to_mb(float(rx_size_in_bytes)), 2)
                except Exception as err:
                    collectd.error('Plugin elasticsearch: Error in converting rx_size_in_bytes into mb due to %s' % str(err))
                    rx_size = 0.0

                tx_size_in_bytes = trans_tx_size_in_bytes - self.previousData["nodeStats"][
                    'node_trans_tx_size_in_bytes']
                if tx_size_in_bytes < 0:
                    tx_size_in_bytes = 0
                self.previousData["nodeStats"]["node_trans_tx_size_in_bytes"] = trans_tx_size_in_bytes

                try:
                    tx_size = round(self.conv_b_to_mb(float(tx_size_in_bytes)), 2)
                except Exception as err:
                    collectd.error('Plugin elasticsearch: Error in converting tx_size_in_bytes into mb due to %s' % str(err))
                    tx_size = 0.0

                brkers_lmt_size_in_bytes = brkers_limit_size_in_bytes - self.previousData["nodeStats"][
                    'node_brkers_lmt_size_in_bytes']
                if brkers_lmt_size_in_bytes < 0:
                    brkers_lmt_size_in_bytes = 0
                self.previousData["nodeStats"]['node_brkers_lmt_size_in_bytes'] = brkers_limit_size_in_bytes

                try:
                    brkers_limit_size = round(self.conv_b_to_mb(float(brkers_lmt_size_in_bytes)), 2)
                except Exception as err:
                    collectd.error('Plugin elasticsearch: Error in converting brkers_lmt_size_in_bytes into mb due to %s' % str(err))
                    brkers_limit_size = 0.0

                brkers_est_size_in_bytes = brkers_estimated_size_in_bytes - self.previousData["nodeStats"][
                    'node_brkers_est_size_in_bytes']
                if brkers_est_size_in_bytes < 0:
                    brkers_est_size_in_bytes = 0
                self.previousData["nodeStats"]['node_brkers_est_size_in_bytes'] = brkers_estimated_size_in_bytes

                try:
                    brkers_estimated_size = round(self.conv_b_to_mb(float(brkers_est_size_in_bytes)), 2)
                except Exception as err:
                    collectd.error('Plugin elasticsearch: Error in converting brkers_est_size_in_bytes into mb due to %s' % str(err))
                    brkers_estimated_size = 0.0

                io_stats_tot_op = io_stats_total_operations - self.previousData["nodeStats"]["node_io_stats_tot_op"]
                if io_stats_tot_op < 0:
                    io_stats_tot_op = 0
                self.previousData["nodeStats"]['node_io_stats_tot_op'] = io_stats_total_operations

                io_stats_tot_read_op = io_stats_total_read_operations - self.previousData["nodeStats"][
                    "node_io_stats_tot_read_op"]
                if io_stats_tot_read_op < 0:
                    io_stats_tot_read_op = 0
                self.previousData["nodeStats"]['node_io_stats_tot_read_op'] = io_stats_total_read_operations

                io_stats_tot_write_op = io_stats_total_write_operations - self.previousData["nodeStats"][
                    "node_io_stats_tot_write_op"]
                if io_stats_tot_write_op < 0:
                    io_stats_tot_write_op = 0
                self.previousData["nodeStats"]['node_io_stats_tot_write_op'] = io_stats_total_write_operations

                io_stats_tot_read_kb = io_stats_total_read_kb - self.previousData["nodeStats"][
                    "node_io_stats_total_read_kb"]
                if io_stats_tot_read_kb < 0:
                    io_stats_tot_read_kb = 0
                self.previousData["nodeStats"]["node_io_stats_total_read_kb"] = io_stats_total_read_kb

                try:
                    io_stats_total_read = round(float(io_stats_tot_read_kb) / 1024.0, 2)
                except Exception as err:
                    collectd.error('Plugin elasticsearch: Error in converting io_stats_tot_read_kb into mb due to %s' % str(err))
                    io_stats_total_read = 0.0

                io_stats_tot_write_kb = io_stats_total_write_kb - self.previousData["nodeStats"][
                    "node_io_stats_total_write_kb"]
                if io_stats_tot_write_kb < 0:
                    io_stats_tot_write_kb = 0
                self.previousData["nodeStats"]["node_io_stats_total_write_kb"] = io_stats_total_write_kb

                try:
                    io_stats_total_write = round(float(io_stats_tot_read_kb) / 1024.0, 2)
                except Exception as err:
                    collectd.error('Plugin elasticsearch: Error in converting io_stats_tot_write_kb into mb due to %s' % str(err))
                    io_stats_total_write = 0.0
            except Exception as err:
                collectd.error("Plugin elasticsearch: Exception in finding difference value of node stats due to %s" % str(err))
        else:
            self.previousData.update({"nodeStats": {'node_id': node_id,
                                                    'node_hit_count': hits,
                                                    'node_miss_count': misses,
                                                    'node_jvm_loaded_classes': loaded_classes,
                                                    'node_jvm_unloaded_classes': unloaded_classes,
                                                    'node_thread_pool_search_completed': thread_pool_search_completed,
                                                    'node_thread_pool_get_completed': thread_pool_get_completed,
                                                    'node_thread_pool_index_completed': thread_pool_index_completed,
                                                    'node_trans_rx_count': trans_rx_count,
                                                    'node_trans_tx_count': trans_tx_count,
                                                    'node_trans_rx_size_in_bytes': trans_rx_size_in_bytes,
                                                    'node_trans_tx_size_in_bytes': trans_tx_size_in_bytes,
                                                    'node_brkers_lmt_size_in_bytes': brkers_limit_size_in_bytes,
                                                    'node_brkers_est_size_in_bytes': brkers_estimated_size_in_bytes,
                                                    'node_io_stats_tot_op': io_stats_total_operations,
                                                    'node_io_stats_tot_read_op': io_stats_total_read_operations,
                                                    'node_io_stats_tot_write_op': io_stats_total_write_operations,
                                                    'node_io_stats_total_read_kb': io_stats_total_read_kb,
                                                    'node_io_stats_total_write_kb': io_stats_total_write_kb
                                                    }})
            hit_count = 0
            miss_count = 0
            jvm_loaded_classes = 0
            jvm_unloaded_classes = 0
            search_thrd_pool_completed = 0
            get_thrd_pool_completed = 0
            index_thrd_pool_completed = 0
            rx_count = 0
            tx_count = 0
            rx_size = 0.0
            tx_size = 0.0
            brkers_limit_size = 0.0
            brkers_estimated_size = 0.0
            io_stats_tot_op = 0
            io_stats_tot_read_op = 0
            io_stats_tot_write_op = 0
            io_stats_total_read = 0.0
            io_stats_total_write = 0.0

        node_stats.update({"node_id": node_id, "_documentType": "nodeStats",
                                                     'nodeName': str(self.node_name),
                                                     'roles': roles,
                                                     'docCount': int(document_count),
                                                     'storeSize': store_size_in_mb,
                                                     'cacheSize': cache_size,
                                                     'segCount': int(segment_count),
                                                     'fielddataMemorySize': field_data_memory_size_in_mb,
                                                     'fielddataEvictions': field_data_evictions,
                                                     'osCpuPercent': os_cpu_percent,
                                                     'osTotMem': os_mem_total,
                                                     'osFreeMem': os_mem_free,
                                                     'osUsedMem': os_mem_used,
                                                     'osFreeMemPercent': os_mem_free_percent,
                                                     'osUsedMemPercent': os_mem_used_percent,
                                                     'processOpenFds': process_open_fds,
                                                     'processCpuPercent': process_cpu_percent,
                                                     'ioTotOper': io_stats_tot_op,
                                                     'ioTotReadOper': io_stats_tot_read_op,
                                                     'ioTotWriteOper': io_stats_tot_write_op,
                                                     'ioTotReadSize': io_stats_total_read,
                                                     'ioTotWriteSize': io_stats_total_write,
                                                     'serverOpen': server_open,
                                                     'transRxCount': rx_count,
                                                     'transRxSize': rx_size,
                                                     'transTxCount': tx_count,
                                                     'transTxSize': tx_size,
                                                     'currOpenConn': http_current_open_conn,
                                                     'totConnOpened': http_total_open_conn,
                                                     'brkersLimitSize': brkers_limit_size,
                                                     'brkersEstimatedSize': brkers_estimated_size,
                                                     'brkersOverhead': brkers_overhead,
                                                     'brkersTripped': brkers_tripped,
                                                     'jvmHeapUsage': jvm_heap_usage,
                                                     'jvmHeapUsagePercent': jvm_heap_usage_percent,
                                                     'jvmHeapCommitted': jvm_heap_committed,
                                                     'jvmHeapMax': jvm_heap_max,
                                                     'jvmNonHeapUsage': jvm_non_heap_usage,
                                                     'jvmNonHeapCommitted': jvm_non_heap_committed,
                                                     'jvmPoolsYoungUsage': jvm_pools_young_usage,
                                                     # 'jvmPoolsYoungMax': jvm_pools_young_max,
                                                     'jvmPoolsYoungPeakUsage': jvm_pools_young_peak_usage,
                                                     # 'jvmPoolsYoungPeakMax': jvm_pools_young_peak_max,
                                                     'jvmPoolsSurvivorUsage': jvm_pools_survivor_usage,
                                                     # 'jvmPoolsSurvivorMax': jvm_pools_survivor_max,
                                                     'jvmPoolsSurvivorPeakUsage': jvm_pools_survivor_peak_usage,
                                                     # 'jvmPoolsSurvivorPeakMax': jvm_pools_survivor_peak_max,
                                                     'jvmPoolsOldUsage': jvm_pools_old_usage,
                                                     # 'jvmPoolsOldMax': jvm_pools_old_max,
                                                     'jvmPoolsOldPeakUsage': jvm_pools_old_peak_usage,
                                                     # 'jvmPoolsOldPeakMax': jvm_pools_old_peak_max,
                                                     'jvmThreads': int(jvm_threads),
                                                     'jvmGc': int(jvm_gc),
                                                     'jvmGct': jvm_gct,
                                                     'jvmYoungGc': int(jvm_young_gc),
                                                     'jvmYoungGct': jvm_young_gct,
                                                     'jvmLoadedClasses': jvm_loaded_classes,
                                                     'jvmUnloadedClasses': jvm_unloaded_classes,
                                                     'threadPoolGetThreads': int(thread_pool_get_threads),
                                                     'threadPoolGetQueue': int(thread_pool_get_queue),
                                                     'threadPoolGetActive': int(thread_pool_get_active),
                                                     'threadPoolGetRejected': int(thread_pool_get_rejected),
                                                     'threadPoolGetLargest': int(thread_pool_get_largest),
                                                     'threadPoolGetCompleted': get_thrd_pool_completed,
                                                     'threadPoolIndexThreads': int(thread_pool_index_threads),
                                                     'threadPoolIndexQueue': int(thread_pool_index_queue),
                                                     'threadPoolIndexActive': int(thread_pool_index_active),
                                                     'threadPoolIndexRejected': int(thread_pool_index_rejected),
                                                     'threadPoolIndexLargest': int(thread_pool_index_largest),
                                                     'threadPoolIndexCompleted': index_thrd_pool_completed,
                                                     'threadPoolSearchThreads': int(thread_pool_search_threads),
                                                     'threadPoolSearchQueue': int(thread_pool_search_queue),
                                                     'threadPoolSearchActive': int(thread_pool_search_active),
                                                     'threadPoolSearchRejected': int(thread_pool_search_rejected),
                                                     'threadPoolSearchLargest': int(thread_pool_search_largest),
                                                     'threadPoolSearchCompleted': search_thrd_pool_completed,
                                                     'cacheHits': hit_count,
                                                     'cacheMisses': miss_count,
                                                     'cacheHitRatio': hit_ratio,
                                                     })
            #node_stats.update(single_node_stats)
        return node_stats

    def get_disk_stats(self):
        collectd.info('Plugin elasticsearch: Collecting node disk stats')
        disk_stats = {}
        uptime_details = {}
        try:
            elastic_search = self.es.cat
            disk_details = elastic_search.allocation(node_id=self.node_name, format='json', bytes='mb')
        except ESException as es_err:
            collectd.error('Plugin elasticsearch: ElasticSearchExcpetion: Error collecting '
                          'disk stats for node : %s due to %s' % (self.node_name, es_err.message))
            return None
        try:
            uptime_details = elastic_search.nodes(format='json', h=['name', 'uptime'])
        except ESException as es_err:
            collectd.error('Plugin elasticsearch: ElasticSearchExcpetion: Error collecting '
                          'uptime for node : %s due to %s' % (self.node_name, es_err.message))
        uptime = None
        if uptime_details:
            try:
                for node_uptime in uptime_details:
                    if node_uptime["name"] == self.node_name:
                        uptime = node_uptime["uptime"]
            except Exception as err:
                collectd.error('Plugin elasticsearch: Plugin elasticsearch: Error in getting uptime for node : %s due to %s' % (self.node_name, err.message))
        else:
            collectd.error("Plugin elasticsearch: Plugin elasticsearch: No uptime stats found for node %s", self.node_name)

        if disk_details:
            for disk_metrics in disk_details:
                if disk_metrics['node'] == self.node_name:
                    disk_tot_mb = 0
                    try:
                        disk_tot_mb = disk_metrics['disk.total']
                    except KeyError as kerr:
                        collectd.error('Plugin elasticsearch: Plugin elasticsearch: Error getting disk_tot_mb for node %s '
                                      ': %s' % (self.node_name, kerr.message))

                    disk_tot = 0.0
                    try:
                        disk_tot = round(int(disk_tot_mb) / 1024.0, 2)
                    except Exception as err:
                        collectd.error('Plugin elasticsearch: Error in converting disk_tot_mb for node %s '
                                      'due to %s' % (self.node_name, str(err)))

                    shards = 0
                    try:
                        shards = disk_metrics['shards']
                    except KeyError as kerr:
                        collectd.error('Plugin elasticsearch: Error getting shards for node %s '
                                      ': %s' % (self.node_name, kerr.message))

                    disk_available_mb = 0
                    try:
                        disk_available_mb = disk_metrics['disk.avail']
                    except KeyError as kerr:
                        collectd.error('Plugin elasticsearch: Error getting disk_available_mb for node %s '
                                      ': %s' % (self.node_name, kerr.message))

                    disk_available = 0.0
                    try:
                        disk_available = round(int(disk_available_mb) / 1024.0, 2)
                    except Exception as err:
                        collectd.error('Plugin elasticsearch: Error in converting disk_available_mb for node %s '
                                      'due to %s' % (self.node_name, str(err)))

                    disk_used_mb = 0
                    try:
                        disk_used_mb = disk_metrics['disk.used']
                    except KeyError as kerr:
                        collectd.error('Plugin elasticsearch: Error getting disk_used_mb for node %s '
                                      ': %s' % (self.node_name, kerr.message))

                    disk_used = 0.0
                    try:
                        disk_used = round(int(disk_used_mb) / 1024.0, 2)
                    except Exception as err:
                        collectd.error('Plugin elasticsearch: Error in converting disk_used_mb for node %s '
                                      'due to %s' % (self.node_name, str(err)))

                    disk_percent = 0
                    try:
                        disk_percent = int(disk_metrics['disk.percent'])
                    except KeyError as kerr:
                        collectd.error('Plugin elasticsearch: Error getting disk_percent for node %s '
                                      ': %s' % (self.node_name, kerr.message))

                    disk_indices_mb = 0
                    try:
                        disk_indices_mb = disk_metrics['disk.indices']
                    except KeyError as kerr:
                        collectd.error('Plugin elasticsearch: Error getting disk_indices_mb for node %s '
                                      ': %s' % (self.node_name, kerr.message))

                    disk_indices = 0.0
                    try:
                        disk_indices = round(int(disk_indices_mb) / 1024.0, 2)
                    except Exception as err:
                        collectd.error('Plugin elasticsearch: Error in converting disk_indices_mb for node %s '
                                      'due to %s' % (self.node_name, str(err)))

                    disk_stats.update({'shards': shards,
                                       'diskTot': disk_tot,
                                       'diskAvail': disk_available,
                                       'diskUsed': disk_used,
                                       'diskPercentUsed': disk_percent,
                                       'diskIndices': disk_indices,
                                       'nodeUpTime': str(uptime)
                                      })
        return disk_stats

    def get_cluster_stats(self):
        """"
        Collects stats for the cluster
        :param: Instance of class elasticsearch.client.cluster.ClusterClient
        :returns: dictionary of the collected stats
        """
        collectd.debug('Collecting cluster stats')
        try:
            stats = self.es.cluster.stats()
            health = self.es.cluster.health()
        except ESException as es_err:
            collectd.error('Plugin elasticsearch: ElasticSearchExcpetion: Error collecting '
                          'stats for cluster : %s' % es_err.message)
            return None
        cluster_stats = {}
        node_count = 0
        try:
            node_count = int(stats['_nodes']['total'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting nodes count : %s' % kerr.message)

        cluster_name = ''
        try:
            cluster_name = str(stats['cluster_name'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting cluster name : %s' % kerr.message)

        master_node_count = 0
        try:
            master_node_count = int(stats['nodes']['count']['master'])
        except KeyError as kerr:
            collectd.error('Plugin Elasticsearch: Error getting master node count : %s' % kerr.message)

        data_node_count = 0
        try:
            data_node_count = int(stats['nodes']['count']['data'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting data nodes count : %s' % kerr.message)

        ingest_node_count = 0
        try:
            ingest_node_count = int(stats['nodes']['count']['ingest'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting ingest nodes count : %s' % kerr.message)

        index_count = 0
        try:
            index_count = int(stats['indices']['count'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting index nodes count : %s' % kerr.message)

        document_count = 0
        try:
            document_count = int(stats['indices']['docs']['count'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting document count : %s' % kerr.message)

        shard_count = 0
        try:
            shard_count = int(stats['indices']['shards']['total'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting shard count : %s' % kerr.message)

        hits = 0
        try:
            hits = int(stats['indices']['query_cache']['hit_count'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting cache hits count : %s' % kerr.message)

        misses = 0
        try:
            misses = int(stats['indices']['query_cache']['miss_count'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting cache miss count : %s' % kerr.message)


        # We need hit and miss count since our previous poll
        if 'clusterStats' in self.previousData:
            hit_count = hits - self.previousData['clusterStats']['cluster_hit_count']
            miss_count = misses - self.previousData['clusterStats']['cluster_miss_count']
            # update older stats dictionary as we'd need it for our next poll
            self.previousData['clusterStats']['cluster_hit_count'] = hits
            self.previousData['clusterStats']['cluster_miss_count'] = misses
        else:
            hit_count = 0
            miss_count = 0
            self.previousData['clusterStats'] = {}
            self.previousData['clusterStats']['cluster_hit_count'] = hits
            self.previousData['clusterStats']['cluster_miss_count'] = misses

        hit_ratio = 0
        try:
            if hits or misses:
                hit_ratio = (hits * 100)/(hits + misses)
        except Exception as e:
            collectd.error('Plugin elasticsearch: Error in calculating hit_ratio : %s' % e.message)

        cache_size = 0
        try:
            cache_size = int(stats['indices']['query_cache']['cache_size'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting cache size : %s' % kerr.message)

        store_size = 0
        try:
            store_size = float(stats['indices']['store']['size_in_bytes'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting store size : %s' % kerr.message)

        store_size_in_bytes = round(self.conv_b_to_mb(store_size), 2)

        segment_count = 0
        try:
            segment_count = int(stats['indices']['segments']['count'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting segment count : %s' % kerr.message)

        cluster_status = None
        try:
            cluster_status = health['status']
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting cluster status : %s' % kerr.message)

        active_primary_shards = 0
        try:
            active_primary_shards = int(health['active_primary_shards'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting active primary shards : %s' % kerr.message)

        active_shards = 0
        try:
            active_shards = int(health['active_shards'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting active shards : %s' % kerr.message)

        relocating_shards = 0
        try:
            relocating_shards = int(health['relocating_shards'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting relocating shards : %s' % kerr.message)

        initializing_shards = 0
        try:
            initializing_shards = int(health['initializing_shards'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting initializing shards : %s' % kerr.message)

        unassigned_shards = 0
        try:
            unassigned_shards = int(health['unassigned_shards'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting unassigned shards : %s' % kerr.message)

        delayed_unassigned_shards = 0
        try:
            delayed_unassigned_shards = int(health['delayed_unassigned_shards'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting delayed unassigned shards : %s' % kerr.message)

        number_of_pending_tasks = 0
        try:
            number_of_pending_tasks = int(health['number_of_pending_tasks'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting number of pending tasks : %s' % kerr.message)

        number_of_in_flight_fetch = 0
        try:
            number_of_in_flight_fetch = int(health['number_of_in_flight_fetch'])
        except KeyError as kerr:
            collectd.error('Plugin elasticsearch: Error getting number of in flight fetch : %s' % kerr.message)

        try:
            cluster_stats.update({'_documentType':'clusterStats',
                                  'totalNodes':node_count,
                                  '_clusterName':cluster_name,
                                  'masterNodes':master_node_count,
                                  'ingestNodes':data_node_count,
                                  'dataNodes':ingest_node_count,
                                  'indicesCount':index_count,
                                  'docCount':document_count,
                                  'shardCount': shard_count,
                                  'queryCacheHits': hit_count,
                                  'queryCacheMisses': miss_count,
                                  'segCount':segment_count,
                                  'clusterStatus': cluster_status,
                                  'activePrimaryShards':active_primary_shards,
                                  'activeShards':active_shards,
                                  'relocatingShards':relocating_shards,
                                  'initializingShards':initializing_shards,
                                  'unassignedShards':unassigned_shards,
                                  'delayedUnassignedShards':delayed_unassigned_shards,
                                  'pendingTasksCount':number_of_pending_tasks,
                                  'inflightFetchCount':number_of_in_flight_fetch,
                                  'queryCacheSize': cache_size,
                                  'queryCacheHitRatio': hit_ratio,
                                  'storeSize': store_size_in_bytes
                                 })
            return cluster_stats
        except KeyError as kerror:
            collectd.error('KeyError updating stats for the cluster : %s' % kerror.message)
            return None
        except Exception as err:
            collectd.error('Plugin elasticsearch: Error updating stats for the cluster : %s' % err.message)
            return None

    def get_index_stats(self, index_list=[]):
        """
        collect index level statistics
        :param: indicesclient: instance of class elasticsearch.client.nodes.IndicesClient
        :param: index_list: list of index names whose stats to be collected.
        :return: dictionary of the collected stats
        """
        try:
            stats = self.es.indices.stats(index_list)
            index_settings = self.es.indices.get_settings(index_list)
        except ESException as err:
            collectd.error('Plugin elasticsearch: Error updating index stats : %s' % err.message)
            return None
        index_stats = {}
        single_index_stats = {}
        if not index_list:
            index_list = stats['indices'].keys()
            if index_list:
                index_list.append("_all")
        for index in index_list:
            try:
                if index == "_all":
                    index_details = stats[index]['total']
                else:
                    index_details = stats['indices'][index]['primaries']
            except KeyError as err:
                collectd.error('Plugin elasticsearch: Error getting index details: %s' % err.message)
                continue

            try:
                if index == "_all":
                    settings_details = {}
                else:
                    settings_details = index_settings[index]['settings']['index'].get('blocks')
            except KeyError as err:
                collectd.error('Plugin elasticsearch: Error in getting index settings details: %s' % err.message)
                continue
            if settings_details:
                index_write = settings_details.get('write', True)
                index_read_only = settings_details.get('read_only', True)
                index_read_only_allow_delete = settings_details.get('read_only_allow_delete', False)
            else:
                index_write = True
                index_read_only = True
                index_read_only_allow_delete = False


            index_creation_date = None
            try:
                index_creation_date = index_settings[index]['settings']['index'].get('creation_date')
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching index creation date for index %s : %s' % (index, ex.message))

            document_count = None
            try:
                document_count = int(index_details['docs']['count'])
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching document count for index %s : %s' % (index, ex.message))

            documents_deleted = None
            try:
                documents_deleted = int(index_details['docs']['deleted'])
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching deleted document count for index %s : %s' % (index, ex.message))

            total_indexed_count = 0
            try:
                total_indexed = int(index_details['indexing']['index_total'])
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching Total indexing count for index %s : %s' % (index, ex.message))

            total_deleted = None
            try:
                total_deleted = int(index_details['indexing']['delete_total'])
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching Total indexing delete count '
                              'for index %s : %s' % (index, ex.message))

            is_throttled = None
            try:
                is_throttled = index_details['indexing']['is_throttled']
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching throttle status '
                              'for index %s : %s' % (index, ex.message))

            total_get_query_count = 0
            try:
                total_get_query = int(index_details['get']['total'])
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total get query count'
                              'for index %s : %s' % (index, ex.message))

            total_search_query_count = 0
            try:
                total_search_query = int(index_details['search']['query_total'])
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total search query count'
                              'for index %s : %s' % (index, ex.message))

            total_search_fetch_count = 0
            try:
                total_search_fetch = int(index_details['search']['fetch_total'])
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total search fetch count'
                              'for index %s : %s' % (index, ex.message))

            total_refresh_count = 0
            try:
                total_refresh = int(index_details['refresh']['total'])
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total refresh query count'
                              'for index %s : %s' % (index, ex.message))

            total_merge_query_count = 0
            try:
                total_merge_query = int(index_details['merges']['total'])
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total merge query count'
                              'for index %s : %s' % (index, ex.message))

            total_warmer_count = None
            try:
                total_warmer_count = int(index_details['warmer']['total'])
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total warmer count'
                              'for index %s : %s' % (index, ex.message))

            total_flush_count = 0
            try:
                total_flush = int(index_details['flush']['total'])
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total flush count'
                              'for index %s : %s' % (index, ex.message))

            total_segment_count = None
            try:
                total_segment_count = int(index_details['segments']['count'])
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total segment count'
                              'for index%s : %s' % (index, ex.message))

            total_translog_operations = None
            try:
                total_translog_operations = int(index_details['translog']['operations'])
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total translog operations'
                              'for index %s : %s' % (index, ex.message))

            store_size_in_mb = None
            try:
                store_size_in_bytes = index_details['store']['size_in_bytes']
                store_size_in_mb = round(self.conv_b_to_mb(float(store_size_in_bytes)), 2)
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching index store size'
                              'for index %s : %s' % (index, ex.message))

            total_indexing_time = None
            try:
                #total_indexing_time_in_millis = index_details['indexing']['index_time_in_millis']
                #total_indexing_time = round(float(total_indexing_time_in_millis) / 1000, 2)
                total_indexing_time = index_details['indexing']['index_time_in_millis']
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total indexing time'
                              'for index %s : %s' % (index, ex.message))

            total_deleting_time = None
            try:
                #total_deleting_time_in_millis = index_details['indexing']['delete_time_in_millis']
                #total_deleting_time = round(float(total_deleting_time_in_millis) / 1000, 2)
                total_deleting_time = index_details['indexing']['delete_time_in_millis']
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total deleting time'
                              'for index %s : %s' % (index, ex.message))

            total_throttle_time = None
            try:
                #total_throttle_time_in_millis = index_details['indexing']['throttle_time_in_millis']
                #total_throttle_time = round(float(total_throttle_time_in_millis) / 1000, 2)
                total_throttle_time = index_details['indexing']['throttle_time_in_millis']
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total throttle time'
                              'for index %s : %s' % (index, ex.message))

            total_get_query_time = None
            try:
                #total_get_query_time_in_millis = index_details['get']['time_in_millis']
                #total_get_query_time = round(float(total_get_query_time_in_millis) / 1000, 2)
                total_get_query_time = index_details['get']['time_in_millis']
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total get query time'
                              'for index %s : %s' % (index, ex.message))

            search_query_latency = None
            total_search_query_time = None
            try:
                #total_search_query_time_in_millis = index_details['search']['query_time_in_millis']
                #total_search_query_time = round(float(total_search_query_time_in_millis) / 1000, 2)
                total_search_query_time = index_details['search']['query_time_in_millis']
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total search query time'
                              'for index %s : %s' % (index, ex.message))

            search_fetch_latency = None
            total_search_fetch_time = None
            try:
                #total_search_fetch_time_in_millis = index_details['search']['fetch_time_in_millis']
                #total_search_fetch_time = round(float(total_search_fetch_time_in_millis) / 1000, 2)
                total_search_fetch_time = index_details['search']['fetch_time_in_millis']
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total search fetch time'
                              'for index %s : %s' % (index, ex.message))

            total_merge_query_time = None
            try:
                #total_merge_query_time_in_millis = index_details['merges']['total_time_in_millis']
                #total_merge_query_time = round(float(total_merge_query_time_in_millis) / 1000, 2)
                total_merge_query_time = index_details['merges']['total_time_in_millis']
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total merge query time'
                              'for index %s : %s' % (index, ex.message))

            total_refresh_time = None
            try:
                #total_refresh_time_in_millis = index_details['refresh']['total_time_in_millis']
                #total_refresh_time = round(float(total_refresh_time_in_millis) / 1000, 2)
                total_refresh_time = index_details['refresh']['total_time_in_millis']
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total refresh time'
                              'for index %s : %s' % (index, ex.message))

            total_warmer_time = None
            try:
                #total_warmer_time_in_millis = index_details['warmer']['total_time_in_millis']
                #total_warmer_time = round(float(total_warmer_time_in_millis) / 1000, 2)
                total_warmer_time = index_details['warmer']['total_time_in_millis']
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total warmer time'
                              'for index %s : %s' % (index, ex.message))

            total_flush_time = None
            try:
                #total_flush_time_in_millis = index_details['flush']['total_time_in_millis']
                #total_flush_time = round(float(total_flush_time_in_millis) / 1000, 2)
                total_flush_time = index_details['flush']['total_time_in_millis']
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching total warmer time'
                              'for index %s : %s' % (index, ex.message))

            query_cache_size_in_mb = None
            try:
                query_cache_size_in_bytes = index_details['query_cache']['memory_size_in_bytes']
                query_cache_size_in_mb = round(float(query_cache_size_in_bytes) / 1024, 2)
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching query cache size'
                              'for index %s : %s' % (index, ex.message))

            query_cache_size = None
            try:
                query_cache_size = index_details['query_cache']['cache_size']
            except KeyError as ex:
                collectd.error('Plugin elasticsearch: Error fetching query cache size'
                              'for index %s : %s' % (index, ex.message))

            field_data_memory_size_in_mb = None
            try:
                field_data_memory_in_bytes = index_details['fielddata']['memory_size_in_bytes']
                field_data_memory_size_in_mb = round(float(field_data_memory_in_bytes) / 1024, 2)
            except Exception as ex:
                collectd.error('Plugin elasticsearch: Error fetching field data memory size'
                              'for index %s : %s' % (index, ex.message))

            field_data_evictions = None
            try:
                field_data_evictions = index_details['fielddata']['evictions']
            except Exception as ex:
                collectd.error('Plugin elasticsearch: Error fetching field data evictions'
                              'for index %s : %s' % (index, ex.message))

            segment_memory_in_mb = None
            try:
                segment_memory_in_bytes = index_details['segments']['memory_in_bytes']
                segment_memory_in_mb = round(float(segment_memory_in_bytes) / 1024, 2)
            except Exception as ex:
                collectd.error('Plugin elasticsearch: Error fetching segment memory size'
                              'for index %s : %s' % (index, ex.message))

            segment_terms_memory_in_mb = None
            try:
                segment_terms_memory_in_bytes = index_details['segments']['terms_memory_in_bytes']
                segment_terms_memory_in_mb = round(float(segment_terms_memory_in_bytes) / 1024, 2)
            except Exception as ex:
                collectd.error('Plugin elasticsearch: Error fetching segment terms memory size'
                              'for index %s : %s' % (index, ex.message))

            segment_stored_fileds_memory_in_mb = None
            try:
                segment_stored_fileds_memory_in_bytes = index_details['segments']['stored_fields_memory_in_bytes']
                segment_stored_fileds_memory_in_mb = round(float(segment_stored_fileds_memory_in_bytes) / 1024, 2)
            except Exception as ex:
                collectd.error('Plugin elasticsearch: Error fetching segment stored field memory size'
                              'for index %s : %s' % (index, ex.message))

            segment_terms_vector_memory_in_mb = None
            try:
                segment_terms_vector_memory_in_bytes = index_details['segments']['term_vectors_memory_in_bytes']
                segment_terms_vector_memory_in_mb = round(float(segment_terms_vector_memory_in_bytes) / 1024, 2)
            except Exception as ex:
                collectd.error('Plugin elasticsearch: Error fetching segment terms vector memory size'
                              'for index %s : %s' % (index, ex.message))

            segment_norms_memory_in_mb = None
            try:
                segment_norms_memory_in_bytes = index_details['segments']['norms_memory_in_bytes']
                segment_norms_memory_in_mb = round(float(segment_norms_memory_in_bytes) / 1024, 2)
            except Exception as ex:
                collectd.error('Plugin elasticsearch: Error fetching segment norms memory size'
                              'for index %s : %s' % (index, ex.message))

            segment_points_memory_in_mb = None
            try:
                segment_points_memory_in_bytes = index_details['segments']['points_memory_in_bytes']
                segment_points_memory_in_mb = round(float(segment_points_memory_in_bytes) / 1024, 2)
            except Exception as ex:
                collectd.error('Plugin elasticsearch: Error fetching segment norms memory size'
                              'for index %s : %s' % (index, ex.message))

            segment_docvalues_memory_in_mb = None
            try:
                segment_docvalues_memory_in_bytes = index_details['segments']['doc_values_memory_in_bytes']
                segment_docvalues_memory_in_mb = round(float(segment_docvalues_memory_in_bytes) / 1024, 2)
            except Exception as ex:
                collectd.error('Plugin elasticsearch: Error fetching doc values memory size'
                              'for index %s : %s' % (index, ex.message))

            segment_indexwriter_memory_in_mb = None
            try:
                segment_indexwriter_memory_in_bytes = index_details['segments']['index_writer_memory_in_bytes']
                segment_indexwriter_memory_in_mb = round(float(segment_indexwriter_memory_in_bytes) / 1024, 2)
            except Exception as ex:
                collectd.error('Plugin elasticsearch: Error fetching index writer memory size'
                              'for index %s : %s' % (index, ex.message))

            segment_versionmap_memory_in_mb = None
            try:
                segment_versionmap_memory_in_bytes = index_details['segments']['version_map_memory_in_bytes']
                segment_versionmap_memory_in_mb = round(float(segment_versionmap_memory_in_bytes) / 1024, 2)
            except Exception as ex:
                collectd.error('Plugin elasticsearch: Error fetching doc values memory size'
                              'for index %s : %s' % (index, ex.message))

            segment_fixbitset_memory_in_mb = None
            try:
                segment_fixbitset_memory_in_bytes = index_details['segments']['fixed_bit_set_memory_in_bytes']
                segment_fixbitset_memory_in_mb = round(float(segment_fixbitset_memory_in_bytes) / 1024, 2)
            except Exception as ex:
                collectd.error('Plugin elasticsearch: Error fetching fixed bit set memory size'
                              'for index %s : %s' % (index, ex.message))

            translog_size_in_mb = None
            try:
                translog_size_in_bytes = index_details['translog']['size_in_bytes']
                translog_size_in_mb = round(float(translog_size_in_bytes) / 1024, 2)
            except Exception as ex:
                collectd.error('Plugin elasticsearch: Error fetching translog size'
                              'for index %s : %s' % (index, ex.message))

            request_cache_memory_size_in_mb = None
            try:
                request_cache_memory_size_in_bytes = index_details['request_cache']['memory_size_in_bytes']
                request_cache_memory_size_in_mb = round(float(request_cache_memory_size_in_bytes) / 1024, 2)
            except Exception as ex:
                collectd.error('Plugin elasticsearch: Error fetching request cache memory size'
                              'for index %s : %s' % (index, ex.message))

            query_cache_hit_count = None
            try:
                query_cache_hits = index_details['query_cache']['hit_count']
            except KeyError as err:
                collectd.error('Plugin elasticsearch: Error fetching query cache hits count'
                              'for index %s : %s' % (index, ex.message))

            query_cache_miss_count = None
            try:
                query_cache_misses = index_details['query_cache']['miss_count']
            except KeyError as err:
                collectd.error('Plugin elasticsearch: Error fetching query cache miss count'
                              'for index %s : %s' % (index, ex.message))

            request_cache_hit_count = None
            try:
                request_cache_hits = index_details['request_cache']['hit_count']
            except KeyError as err:
                collectd.error('Plugin elasticsearch: Error fetching query cache hits count'
                              'for index %s : %s' % (index, ex.message))

            request_cache_miss_count = None
            try:
                request_cache_misses = index_details['request_cache']['miss_count']
            except KeyError as err:
                collectd.error('Plugin elasticsearch: Error fetching query cache miss count'
                              'for index %s : %s' % (index, ex.message))

            if 'indexStats' in self.previousData and index in self.previousData['indexStats'] and not self.pollCounter == 1:
                try:
                    doc_del_count = documents_deleted - self.previousData['indexStats'][index]['documents_deleted']
                    if doc_del_count < 0:
                        doc_del_count = 0
                    self.previousData['indexStats'][index]['documents_deleted'] = documents_deleted

                    query_cache_hit_count = query_cache_hits - self.previousData['indexStats'][index]['query_cache_hits']
                    if query_cache_hit_count < 0:
                        query_cache_hit_count = 0
                    self.previousData['indexStats'][index]['query_cache_hits'] = query_cache_hits

                    query_cache_miss_count = query_cache_misses - self.previousData['indexStats'][index]['query_cache_misses']
                    if query_cache_miss_count < 0:
                        query_cache_miss_count = 0
                    self.previousData['indexStats'][index]['query_cache_misses'] = query_cache_misses

                    request_cache_hit_count = request_cache_hits - self.previousData['indexStats'][index]['request_cache_hits']
                    if request_cache_hit_count < 0:
                        request_cache_hit_count = 0
                    self.previousData['indexStats'][index]['request_cache_hits'] = request_cache_hits

                    request_cache_miss_count = request_cache_misses - self.previousData['indexStats'][index]['request_cache_misses']
                    if request_cache_miss_count < 0:
                        request_cache_miss_count = 0
                    self.previousData['indexStats'][index]['request_cache_misses'] = request_cache_misses

                    total_get_query_count = total_get_query - self.previousData['indexStats'][index]['total_get_query_count']
                    if total_get_query_count < 0:
                        total_get_query_count = 0
                    self.previousData['indexStats'][index]['total_get_query_count'] = total_get_query

                    total_merge_query_count = total_merge_query - self.previousData['indexStats'][index]['total_merge_query_count']
                    if total_merge_query_count < 0:
                        total_merge_query_count = 0
                    self.previousData['indexStats'][index]['total_merge_query_count'] = total_merge_query

                    total_refresh_count = total_refresh - self.previousData['indexStats'][index]['total_refresh_count']
                    if total_refresh_count < 0:
                        total_refresh_count = 0
                    self.previousData['indexStats'][index]['total_refresh_count'] = total_refresh_count

                    total_indexed_count = total_indexed - self.previousData['indexStats'][index]['total_indexed_count']
                    if total_indexed_count < 0:
                        total_indexed_count = 0
                    self.previousData['indexStats'][index]['total_indexed_count'] = total_indexed

                    total_flush_count = total_flush - self.previousData['indexStats'][index]['total_flush_count']
                    if total_flush_count < 0:
                        total_flush_count = 0
                    self.previousData['indexStats'][index]['total_flush_count'] = total_flush

                    total_search_query_count = total_search_query - self.previousData['indexStats'][index]['total_search_query_count']
                    if total_search_query_count < 0:
                        total_search_query_count = 0
                    self.previousData['indexStats'][index]['total_search_query_count'] = total_search_query

                    total_search_fetch_count = total_search_fetch - self.previousData['indexStats'][index]['total_search_fetch_count']
                    if total_search_fetch_count < 0:
                        total_search_fetch_count = 0
                    self.previousData['indexStats'][index]['total_search_fetch_count'] = total_search_fetch

                    total_get_query_diff_time = round(total_get_query_time - self.previousData['indexStats'][index]['total_get_query_time'], 2)
                    if total_get_query_diff_time < 0:
                        total_get_query_diff_time = 0
                    self.previousData['indexStats'][index]['total_get_query_time'] = total_get_query_time

                    total_merge_query_diff_time = round(total_merge_query_time - self.previousData['indexStats'][index]['total_merge_query_time'], 2)
                    if total_merge_query_diff_time < 0:
                        total_merge_query_diff_time = 0
                    self.previousData['indexStats'][index]['total_merge_query_time'] = total_merge_query_time

                    total_refresh_diff_time = round(total_refresh_time - self.previousData['indexStats'][index]['total_refresh_time'], 2)
                    if total_refresh_diff_time < 0:
                        total_refresh_diff_time = 0
                    self.previousData['indexStats'][index]['total_refresh_time'] = total_refresh_time

                    total_indexed_diff_time = round(total_indexing_time - self.previousData['indexStats'][index]['total_indexing_time'], 2)
                    if total_indexed_diff_time < 0:
                        total_indexed_diff_time = 0
                    self.previousData['indexStats'][index]['total_indexing_time'] = total_indexing_time

                    total_flush_diff_time = round(total_flush_time - self.previousData['indexStats'][index]['total_flush_time'], 2)
                    if total_flush_diff_time < 0:
                        total_flush_diff_time = 0
                    self.previousData['indexStats'][index]['total_flush_time'] = total_flush_time

                    total_search_query_diff_time = round(total_search_query_time - self.previousData['indexStats'][index]['total_search_query_time'], 2)
                    if total_search_query_diff_time < 0:
                        total_search_query_diff_time = 0
                    self.previousData['indexStats'][index]['total_search_query_time'] = total_search_query_time

                    total_search_fetch_diff_time = round(total_search_fetch_time - self.previousData['indexStats'][index]['total_search_fetch_time'], 2)
                    if total_search_fetch_diff_time < 0:
                        total_search_fetch_diff_time = 0
                    self.previousData['indexStats'][index]['total_search_fetch_time'] = total_search_fetch_time

                    try:
                        search_query_latency = round((total_search_query - self.previousData['indexStats'][index]['total_search_query_count']) / (
                                                total_search_query_time - self.previousData['indexStats'][index]['total_search_query_time']), 2)
                        if search_query_latency < 0:
                            search_query_latency = 0.0
                    except ZeroDivisionError as err:
                        search_query_latency = 0.0

                    try:
                        search_fetch_latency = round((total_search_fetch - self.previousData['indexStats'][index]['total_search_fetch_count']) / (
                                                total_search_fetch_time - self.previousData['indexStats'][index]['total_search_fetch_time']), 2)
                        if search_fetch_latency < 0:
                            search_fetch_latency = 0.0
                    except ZeroDivisionError as err:
                        search_fetch_latency = 0.0

                except Exception as err:
                    collectd.error("Plugin Elasticsearch: Exception in finding difference value of index stats due to %s" % str(err))
            else:
                total_get_query_diff_time = 0
                total_merge_query_diff_time = 0
                total_refresh_diff_time = 0
                total_indexed_diff_time = 0
                total_flush_diff_time = 0
                total_search_query_diff_time = 0
                total_search_fetch_diff_time = 0
                query_cache_hit_count = 0
                query_cache_miss_count = 0
                request_cache_hit_count = 0
                request_cache_miss_count = 0
                search_query_latency = 0.0
                search_fetch_latency = 0.0
                doc_del_count = 0
                if not 'indexStats' in self.previousData:
                    self.previousData['indexStats'] = {}

                self.previousData['indexStats'].update({index: {'query_cache_hits': query_cache_hits,
                                                  'query_cache_misses': query_cache_misses,
                                                  'request_cache_hits': request_cache_hits,
                                                  'request_cache_misses': request_cache_misses,
                                                  'total_search_query_count': total_search_query,
                                                  'total_search_fetch_count': total_search_fetch,
                                                  'total_get_query_count': total_get_query,
                                                  'total_merge_query_count': total_merge_query,
                                                  'total_refresh_count': total_refresh,
                                                  'total_indexed_count': total_indexed,
                                                  'total_flush_count': total_flush,
                                                  'total_get_query_time': total_get_query_time,
                                                  'total_merge_query_time': total_merge_query_time,
                                                  'total_refresh_time': total_refresh_time,
                                                  'total_indexing_time': total_indexing_time,
                                                  'total_flush_time': total_flush_time,
                                                  'total_search_query_time': total_search_query_time,
                                                  'total_search_fetch_time': total_search_fetch_time,
                                                  'documents_deleted': documents_deleted

                                         }})

            single_index_stats.update({str(index): {'_documentType': 'indexStats',
                                                    'indexName': str(index),
                                                    'docCount': document_count,
                                                    'docsDeleted':doc_del_count,
                                                    'storeSize':store_size_in_mb,
                                                    'totalIndexed': total_indexed_count,
                                                    'totalIndexedTime': total_indexed_diff_time,
                                                    'totalDeletes': total_deleted,
                                                    'totalDeletedTime': total_deleting_time,
                                                    'isThrottled': is_throttled,
                                                    'throttledTime': total_throttle_time,
                                                    'totalGetQueries': total_get_query_count,
                                                    'totalGetQueryTime': total_get_query_diff_time,
                                                    'totalSearchQueries': total_search_query_count,
                                                    'totalSearchFetchQueries': total_search_fetch_count,
                                                    'totalSearchQueryTime': total_search_query_diff_time,
                                                    'totalSearchFetchTime': total_search_fetch_diff_time,
                                                    'totalMergeQueries': total_merge_query_count,
                                                    'totalMergeQueryTime': total_merge_query_diff_time,
                                                    'totalRefreshs': total_refresh_count,
                                                    'totalRefreshTime': total_refresh_diff_time,
                                                    'totalWarmers': total_warmer_count,
                                                    'totalWarmerTime': total_warmer_time,
                                                    'totalFlushes': total_flush_count,
                                                    'totalFlushTime': total_flush_diff_time,
                                                    'queryCacheMemorySize': query_cache_size_in_mb,
                                                    'queryCacheSize': query_cache_size,
                                                    'fielddataMemorySize': field_data_memory_size_in_mb,
                                                    'fielddataEvictions': field_data_evictions,
                                                    'segCount': total_segment_count,
                                                    'segMemory': segment_memory_in_mb,
                                                    'segTermsMemory': segment_terms_memory_in_mb,
                                                    'segStoredFieldsMemory': segment_stored_fileds_memory_in_mb,
                                                    'segTermsVectorMemory': segment_terms_vector_memory_in_mb,
                                                    'segNormsMemory': segment_norms_memory_in_mb,
                                                    'segPointsMemory': segment_points_memory_in_mb,
                                                    'segDocValuesMemory': segment_docvalues_memory_in_mb,
                                                    'segIndexWriterMemory': segment_indexwriter_memory_in_mb,
                                                    'segVersionMapMemory': segment_versionmap_memory_in_mb,
                                                    'segfixedBitSetMemory': segment_fixbitset_memory_in_mb,
                                                    'translogOperations': total_translog_operations,
                                                    'translogSize': translog_size_in_mb,
                                                    'requestCacheMemorySize': request_cache_memory_size_in_mb,
                                                    'queryCacheHits': query_cache_hit_count,
                                                    'queryCacheMisses': query_cache_miss_count,
                                                    'requestCacheHits': request_cache_hit_count,
                                                    'requestCacheMisses': request_cache_miss_count,
                                                    'searchQueryLatency': search_query_latency,
                                                    'searchFetchLatency': search_fetch_latency,
                                                    'indexWrite': index_write,
                                                    'indexReadOnly': index_read_only,
                                                    'indexReadOnlyAllowDelete': index_read_only_allow_delete,
                                                    'indexCreationDate': index_creation_date
                                      }})
            index_stats.update(single_index_stats)
        return index_stats

    def get_index_health(self):
        """
        Collect health of an index in elasticsearch
        :param elastic_search:
        :return:
        """
        collectd.info('Plugin elasticsearch: Collecting index health stats')
        try:
            health_details = self.es.cat.indices(format='json', h=['index', 'status', 'health'])
        except ESException as es_err:
            collectd.error('Plugin Elasticsearch: ElasticSearchExcpetion: Error collecting '
                          'health for indices : due to %s' % es_err.message)
            return None
        health_stats = {}
        single_health_stats = {}
        if health_details:
            try:
                for health_detail in health_details:
                    index_name = health_detail["index"]
                    index_health = health_detail["health"]
                    single_health_stats.update({str(index_name): {"indexHealth": index_health}})
                    health_stats.update(single_health_stats)
            except Exception as err:
                collectd.error("Plugin elasticsearch: Error in getting health stats for indices due to %s" % err.message)
            return health_stats
        else:
            collectd.error("Plugin elasticsearch: No health stats found for indices ")
            return None

    def collect_es_data(self):
        try:
            es_data = {}
            es_info = self.es.info()
            self.node_name = es_info['name']
            nodes_list = self.get_nodes()
            es_nodes = self.es.nodes.info()

            for node in nodes_list:
                if es_nodes['nodes'][node]['name'] == self.node_name:
                    node_id = node
                    es_data['nodeStats'] = self.get_node_stats(node_id=node_id)
                    collectd.info("Plugin elasticsearch: Node stats : %s" % str(es_data['nodeStats']))
                    if es_data['nodeStats']:
                        disk_stats = self.get_disk_stats()
                        es_data['nodeStats'].update(disk_stats)

                    if self.check_master(node_id):
                        es_data['clusterStats'] = self.get_cluster_stats()
                        index_stats = self.get_index_stats()
                        index_health = self.get_index_health()

                        if index_health:
                            for index in index_health:
                                index_details = index_stats[index]
                                index_details.update({"indexHealth": index_health[index]["indexHealth"]})

                            index_details = index_stats["_all"]
                            index_details.update({"indexHealth": "green"})
                        es_data['indexStats'] = index_stats

            if es_data:
                for doc, data in es_data.items():
                    if doc == 'indexStats':
                        for index in data:
                            self.add_common_params(doc, data[index])
                    else:
                        self.add_common_params(doc, data)
            return es_data

        except Exception as err:
            collectd.error("Plugin elasticsearch: Error in collect_es_data function due to %s" % str(err))
            collectd.error("Plugin elasticsearch: %s" % str(traceback.format_exc()))
            return

    def read(self):
        try:
            self.pollCounter += 1
            connection = "{}://{}:{}".format(str(self.es_protocol), str(self.host), str(self.port))
            self.es = ESearch([connection], verify_certs=False, connection_class=RequestsHttpConnection, timeout=90)

            if self.ping_server():
                es_stats = self.collect_es_data()

                if es_stats:
                    if 'indexStats' in es_stats and 'indexStats' in self.documentsTypes:
                        for index in es_stats['indexStats']:
                            collectd.info("Plugin elasticsearch: Dispatching index stats for index %s" % index)
                            self.dispatch_data(es_stats['indexStats'][index])
                    if 'clusterStats' in es_stats and 'clusterStats' in self.documentsTypes:
                        collectd.info("Plugin elasticsearch: Dispatching cluster stats")
                        self.dispatch_data(es_stats['clusterStats'])
                    if 'nodeStats' in es_stats and 'nodeStats' in self.documentsTypes:
                        collectd.info("Plugin elasticsearch: Dispatching node stats")
                        self.dispatch_data(es_stats['nodeStats'])

        except Exception as err:
            collectd.error("Plugin Elasticsearch: Error in read function due to %s" % str(err))
            collectd.info("Plugin Elasticsearch: %s" % traceback.format_exc())

    def dispatch_data(self, details):
        collectd.info("Plugin elasticsearch: Values: " + json.dumps(details))
        utils.dispatch(details)

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


def init():
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)


obj = ElasticsearchStats()
collectd.register_config(obj.read_config)
collectd.register_read(obj.read_temp)

