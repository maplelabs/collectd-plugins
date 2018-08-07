"""
A collectd plugin for analysing tpcc
test results
"""

import collectd
import signal
import time
import json
import glob
import os
import subprocess

from constants import *
from utils import *
from copy import deepcopy

class TpccResults:

    def __init__(self):
        self.interval = DEFAULT_INTERVAL
        self.pollCounter = 0
        self.previousData = {}

    def read_config(self, cfg):
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]

    def phase_transactions(self, runId):
        filepath = "/opt/VDriver/results/" + runId + "/results/*/mixlog_validation.log"
        fileName = glob.glob(filepath)

        with open(fileName[0]) as file_obj:
            lines = file_obj.readlines()

        result = []

        interval_data = False
        Trade_order = Trade_result = Trade_lookup = Trade_update = Trade_status = False
        Customer_position = Broker_volume = Security_detail = Market_feed = False
        Market_watch = Data_maintenance = False

        for line in lines:
            if line.startswith("Measurement Interval Data"):
                interval_data = True
                continue

            if line.startswith("==============="):
                interval_data = False

            if interval_data:
                if not line == '\n' and not line == '\r\n':
                    if line.startswith("Trade-Order"):
                        val_dict = {}
                        val_dict["transactionName"] = "Trade-Order"
                        val_dict["group"] = []
                        Trade_order = True
                        continue
                    elif line.startswith("Trade-Result"):
                        val_dict = {}
                        val_dict["transactionName"] = "Trade-Result"
                        val_dict["group"] = []
                        Trade_result = True
                        continue
                    elif line.startswith("Trade-Lookup"):
                        val_dict = {}
                        val_dict["transactionName"] = "Trade-Lookup"
                        val_dict["group"] = []
                        Trade_lookup = True
                        continue
                    elif line.startswith("Trade-Update"):
                        val_dict = {}
                        val_dict["transactionName"] = "Trade-Update"
                        val_dict["group"] = []
                        Trade_update = True
                        continue
                    elif line.startswith("Trade-Status"):
                        val_dict = {}
                        val_dict["transactionName"] = "Trade-Status"
                        val_dict["group"] = []
                        Trade_status = True
                        continue
                    elif line.startswith("Customer-Position"):
                        val_dict = {}
                        val_dict["transactionName"] = "Customer-Position"
                        val_dict["group"] = []
                        Customer_position = True
                        continue
                    elif line.startswith("Broker-Volume"):
                        val_dict = {}
                        val_dict["transactionName"] = "Broker-Volume"
                        val_dict["group"] = []
                        Broker_volume = True
                        continue
                    elif line.startswith("Security-Detail"):
                        val_dict = {}
                        val_dict["transactionName"] = "Security-Detail"
                        val_dict["group"] = []
                        Security_detail = True
                        continue
                    elif line.startswith("Market-Feed"):
                        val_dict = {}
                        val_dict["transactionName"] = "Market-Feed"
                        val_dict["group"] = []
                        Market_feed = True
                        continue
                    elif line.startswith("Market-Watch"):
                        val_dict = {}
                        val_dict["transactionName"] = "Market-Watch"
                        val_dict["group"] = []
                        Market_watch = True
                        continue
                    elif line.startswith("Data-Maintenance"):
                        val_dict = {}
                        val_dict["transactionName"] = "Data-Maintenance"
                        val_dict["group"] = []
                        Data_maintenance = True
                        continue

                    if Trade_order:
                        if line.startswith("Group 1"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 2"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 3"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 4"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                            Trade_order = False
                            for group_index, each_group in enumerate(val_dict["group"]):
                                for phase_index, each_phase in enumerate(each_group):
                                    new_dict = {}
                                    new_dict["transactionName"] = val_dict["transactionName"]
                                    new_dict["_group"] = str(group_index + 1)
                                    new_dict["_phase"] = str(phase_index + 1)
                                    new_dict["transactionCount"] = long(each_phase)
                                    new_dict["_runId"] = runId
                                    new_dict["_uniqueId"] = "G" + str(group_index + 1) \
                                                            + "P" + str(phase_index + 1)
                                    result.append(new_dict)

                    if Trade_result:
                        if line.startswith("Group 1"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 2"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 3"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 4"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                            Trade_result = False
                            for group_index, each_group in enumerate(val_dict["group"]):
                                for phase_index, each_phase in enumerate(each_group):
                                    new_dict = {}
                                    new_dict["transactionName"] = val_dict["transactionName"]
                                    new_dict["_group"] = str(group_index + 1)
                                    new_dict["_phase"] = str(phase_index + 1)
                                    new_dict["transactionCount"] = long(each_phase)
                                    new_dict["_runId"] = runId
                                    new_dict["_uniqueId"] = "G" + str(group_index + 1) \
                                                            + "P" + str(phase_index + 1)
                                    result.append(new_dict)

                    if Trade_lookup:
                        if line.startswith("Group 1"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 2"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 3"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 4"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                            Trade_lookup = False
                            for group_index, each_group in enumerate(val_dict["group"]):
                                for phase_index, each_phase in enumerate(each_group):
                                    new_dict = {}
                                    new_dict["transactionName"] = val_dict["transactionName"]
                                    new_dict["_group"] = str(group_index + 1)
                                    new_dict["_phase"] = str(phase_index + 1)
                                    new_dict["transactionCount"] = long(each_phase)
                                    new_dict["_runId"] = runId
                                    new_dict["_uniqueId"] = "G" + str(group_index + 1) \
                                                            + "P" + str(phase_index + 1)
                                    result.append(new_dict)

                    if Trade_update:
                        if line.startswith("Group 1"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 2"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 3"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 4"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                            Trade_update = False
                            for group_index, each_group in enumerate(val_dict["group"]):
                                for phase_index, each_phase in enumerate(each_group):
                                    new_dict = {}
                                    new_dict["transactionName"] = val_dict["transactionName"]
                                    new_dict["_group"] = str(group_index + 1)
                                    new_dict["_phase"] = str(phase_index + 1)
                                    new_dict["transactionCount"] = long(each_phase)
                                    new_dict["_runId"] = runId
                                    new_dict["_uniqueId"] = "G" + str(group_index + 1) \
                                                            + "P" + str(phase_index + 1)
                                    result.append(new_dict)

                    if Trade_status:
                        if line.startswith("Group 1"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 2"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 3"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 4"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                            Trade_status = False
                            for group_index, each_group in enumerate(val_dict["group"]):
                                for phase_index, each_phase in enumerate(each_group):
                                    new_dict = {}
                                    new_dict["transactionName"] = val_dict["transactionName"]
                                    new_dict["_group"] = str(group_index + 1)
                                    new_dict["_phase"] = str(phase_index + 1)
                                    new_dict["transactionCount"] = long(each_phase)
                                    new_dict["_runId"] = runId
                                    new_dict["_uniqueId"] = "G" + str(group_index + 1) \
                                                            + "P" + str(phase_index + 1)
                                    result.append(new_dict)

                    if Customer_position:
                        if line.startswith("Group 1"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 2"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 3"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 4"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                            Customer_position = False
                            for group_index, each_group in enumerate(val_dict["group"]):
                                for phase_index, each_phase in enumerate(each_group):
                                    new_dict = {}
                                    new_dict["transactionName"] = val_dict["transactionName"]
                                    new_dict["_group"] = str(group_index + 1)
                                    new_dict["_phase"] = str(phase_index + 1)
                                    new_dict["transactionCount"] = long(each_phase)
                                    new_dict["_runId"] = runId
                                    new_dict["_uniqueId"] = "G" + str(group_index + 1) \
                                                            + "P" + str(phase_index + 1)
                                    result.append(new_dict)

                    if Broker_volume:
                        if line.startswith("Group 1"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 2"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 3"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 4"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                            Broker_volume = False
                            for group_index, each_group in enumerate(val_dict["group"]):
                                for phase_index, each_phase in enumerate(each_group):
                                    new_dict = {}
                                    new_dict["transactionName"] = val_dict["transactionName"]
                                    new_dict["_group"] = str(group_index + 1)
                                    new_dict["_phase"] = str(phase_index + 1)
                                    new_dict["transactionCount"] = long(each_phase)
                                    new_dict["_runId"] = runId
                                    new_dict["_uniqueId"] = "G" + str(group_index + 1) \
                                                            + "P" + str(phase_index + 1)
                                    result.append(new_dict)

                    if Security_detail:
                        if line.startswith("Group 1"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 2"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 3"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 4"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                            Security_detail = False
                            for group_index, each_group in enumerate(val_dict["group"]):
                                for phase_index, each_phase in enumerate(each_group):
                                    new_dict = {}
                                    new_dict["transactionName"] = val_dict["transactionName"]
                                    new_dict["_group"] = str(group_index + 1)
                                    new_dict["_phase"] = str(phase_index + 1)
                                    new_dict["transactionCount"] = long(each_phase)
                                    new_dict["_runId"] = runId
                                    new_dict["_uniqueId"] = "G" + str(group_index + 1) \
                                                            + "P" + str(phase_index + 1)
                                    result.append(new_dict)

                    if Market_feed:
                        if line.startswith("Group 1"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 2"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 3"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 4"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                            Market_feed = False
                            for group_index, each_group in enumerate(val_dict["group"]):
                                for phase_index, each_phase in enumerate(each_group):
                                    new_dict = {}
                                    new_dict["transactionName"] = val_dict["transactionName"]
                                    new_dict["_group"] = str(group_index + 1)
                                    new_dict["_phase"] = str(phase_index + 1)
                                    new_dict["transactionCount"] = long(each_phase)
                                    new_dict["_runId"] = runId
                                    new_dict["_uniqueId"] = "G" + str(group_index + 1) \
                                                            + "P" + str(phase_index + 1)
                                    result.append(new_dict)

                    if Market_watch:
                        if line.startswith("Group 1"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 2"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 3"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 4"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                            Market_watch = False
                            for group_index, each_group in enumerate(val_dict["group"]):
                                for phase_index, each_phase in enumerate(each_group):
                                    new_dict = {}
                                    new_dict["transactionName"] = val_dict["transactionName"]
                                    new_dict["_group"] = str(group_index + 1)
                                    new_dict["_phase"] = str(phase_index + 1)
                                    new_dict["transactionCount"] = long(each_phase)
                                    new_dict["_runId"] = runId
                                    new_dict["_uniqueId"] = "G" + str(group_index + 1) \
                                                            + "P" + str(phase_index + 1)
                                    result.append(new_dict)

                    if Data_maintenance:
                        if line.startswith("Group 1"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 2"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 3"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                        elif line.startswith("Group 4"):
                            line = line.split(":")[1].strip()
                            value = line.split("]")
                            value = [each_value.replace("[", "") for each_value in value]
                            val_dict["group"].append(value[:-1])
                            Data_maintenance = False
                            for group_index, each_group in enumerate(val_dict["group"]):
                                for phase_index, each_phase in enumerate(each_group):
                                    new_dict = {}
                                    new_dict["transactionName"] = val_dict["transactionName"]
                                    new_dict["_group"] = str(group_index + 1)
                                    new_dict["_phase"] = str(phase_index + 1)
                                    new_dict["transactionCount"] = long(each_phase)
                                    new_dict["_runId"] = runId
                                    new_dict["_uniqueId"] = "G" + str(group_index + 1) \
                                                            + "P" + str(phase_index + 1)
                                    result.append(new_dict)

        return result

    def group_tps(self, runId):

        vcfg_file = "/opt/VDriver/results/" + runId + "/vcfg.properties"
        with open(vcfg_file) as file_obj:
            file_content = file_obj.readlines()

        for line in file_content:
            if line.startswith("VM_TILES"):
                tiles = int(line.split("=")[1].strip().replace("\"", ""))
                break

        total_entries = 4 * tiles

        filepath = "/opt/VDriver/results/" + runId + "/results/*/audit_check.log"
        fileName = glob.glob(filepath)

        with open(fileName[0]) as file_obj:
            lines = file_obj.readlines()

        throughput = False
        group = 1
        phase = 1
        phase_value = 1
        result = []
        for line in lines:
            if "Measured Throughput Data" in line:
                throughput = True
                continue

            if line.startswith("=============="):
                throughput = False

            if throughput and not "combined" in line:
                value = {}
                line = line.split(":")
                value["status"] = line[0].strip()
                line = line[1].split("=")
                value["_tile"] = str((int(line[0].split("[")[1].replace("]", "").strip())) + 1)
                value["actual"] = float(((line[1].split(", ")[0]).strip()).replace(",", ""))
                value["min"] = float(((line[2].split(", ")[0]).strip()).replace(",", ""))
                value["max"] = float((line[3].strip()).replace(",", ""))
                value["_phase"] = str(phase)
                value["_group"] = str(group)
                value["_runId"] = runId
                value["_uniqueId"] = "T" + str((int(line[0].split("[")[1].replace("]", "").strip())) + 1) \
                                     + "G" + str(group) + "P" + str(phase)
                result.append(value)
                group += 1
                phase_value += 1

            if group == 5:
                group = 1

            if phase_value > total_entries:
                phase_value = 1
                phase += 1

            if "combined" in line:
                value = {}
                line = line.split(":")
                value["status"] = line[0].strip()
                line = line[1].split("=")
                value["actual"] = float(((line[1].split(", ")[0]).strip()).replace(",", ""))
                value["min"] = float(((line[2].split(", ")[0]).strip()).replace(",", ""))
                value["max"] = float((line[3].strip()).replace(",", ""))
                value["_phase"] = "combined"
                value["_group"] = "combined"
                value["_tile"] = "combined"
                value["_runId"] = runId
                value["_uniqueId"] = runId + "_Over All"
                result.append(value)

        return result

    @staticmethod
    def add_common_params(tpcc_dict, doc_type):
        hostname = gethostname()
        timestamp = int(round(time.time()))

        tpcc_dict[HOSTNAME] = hostname
        tpcc_dict[TIMESTAMP] = timestamp
        tpcc_dict[PLUGIN] = TPCC
        tpcc_dict[ACTUALPLUGINTYPE] = TPCC
        tpcc_dict[PLUGINTYPE] = doc_type
        tpcc_dict[PLUGIN_INS] = doc_type

    @staticmethod
    def dispatch_data(tpcc_dict):
        collectd.info("Plugin TPCC: Values: " + json.dumps(tpcc_dict))
        dispatch(tpcc_dict)

    def collect_results(self):

        runIds = "/opt/VDriver/results"
        runs = os.listdir(runIds)
        runs = sorted(runs)

        if "PROCESSEDID" in runs:
            pass
        else:
            with open(runIds + "/PROCESSEDID", "w") as file_obj:
                file_obj.write("0"+ "\n")

        with open(runIds + "/PROCESSEDID", "r") as file_obj:
            processed_id = file_obj.read().strip()

        for each_run in runs:
            if os.path.isdir(runIds + "/" + each_run):
                files = os.listdir(runIds + "/" + each_run)
                if int(each_run) > int(processed_id):
                    if "Executive Summary Report.html" in files:
                        phase_data = self.phase_transactions(each_run)
                        for each_dict in phase_data:
                            self.add_common_params(each_dict, "tpccPhaseTxn")
                            self.dispatch_data(deepcopy(each_dict))

                        group_tps_values = self.group_tps(each_run)
                        for each_dict in group_tps_values:
                            self.add_common_params(each_dict, "tpccGroupTps")
                            self.dispatch_data(deepcopy(each_dict))

                        with open(runIds + "/PROCESSEDID", "w") as file_obj:
                            file_obj.write(each_run + "\n")

    def read(self):
        self.pollCounter += 1
        self.collect_results()

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))

obj = TpccResults()
collectd.register_config(obj.read_config)
collectd.register_read(obj.read_temp)
