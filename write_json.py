import os
import json
import collectd

# user imports
from constants import *


class WriteJson:
    def __init__(self):
        self.max_entries = 10
        self.path = PATH

    def read_config(self, cfg):
        for children in cfg.children:
            if children.key == MAX_LOG_ENTRIES:
                self.max_entries = int(children.values[0])

    def datadict_to_dirname(self, data):
        if PLUGIN not in data:
            collectd.error("Plugin values not set")
            return None
        # path = os.path.join(self.path, DATADIR, data[PLUGIN])

        if(data[PLUGIN] not in ["mysql", "postgres", "tpcc"]):
            path = os.path.join(self.path, DATADIR, data[PLUGIN] + "/" + data[PLUGINTYPE])
        else:
            path = os.path.join(self.path, DATADIR, data[PLUGIN])

        if PLUGIN_INS in data:
            path = os.path.join(path, data[PLUGIN_INS])
        return path

    # creates index file in the required directory if it doesn't exits
    # otherwise just returns index
    def get_index(self, dirname):
        index = ERRNO
        filename = os.path.join(dirname, INDEX_FILE)
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    collectd.error("Creation of directory failed")
                    return index
            with open(filename, "w") as f:
                f.write(INDEX_NEW_FILE)
                f.write("\n")
                f.write(str(self.max_entries))
                f.write("\n")
        with open(filename, "r") as f:
            try:
                index = int(f.readline())
                collectd.debug("reading index.txt index = %s" % index)
            except Exception as e:
                collectd.debug("reading index.txt index = %s" % str(e))
                index = ERRNO
        return index

    def get_filename_from_index(self, index):
        new_index = (index + 1) % self.max_entries
        filename = str(new_index) + LOG_EXTENSION
        return filename, new_index

    def write(self, data, dirname):
        index = self.get_index(dirname)
        if index == ERRNO:
            return
        (filename, new_index) = self.get_filename_from_index(index)
        fpath_log = os.path.join(dirname, filename)
        fpath_index = os.path.join(dirname, INDEX_FILE)
        try:
            fh = open(fpath_log, "w")
            data = json.dumps(data)
            fh.write(data)
            fh.write("\n")
            fh.close()
            fh = open(fpath_index, "w")
            fh.write(str(new_index))
            fh.write("\n")
            fh.write(str(self.max_entries))
            fh.write("\n")
            fh.close()
        except:
            fh.close()

    def format_json(self, data):
        delete_list = []
        update_dict = {}
        for key, value in data.items():
            if key == ACTUALPLUGINTYPE:
                delete_list.append(key)
            if isinstance(value, str):
                if len(value) > 0 and value[0] == "[":
                    delete_list.append(key)
                    value = value.replace("\\", "")
                    update_dict[key] = json.loads(value)
        for key in delete_list:
            data.pop(key, None)
        data.update(update_dict)
        return data

    def write_json(self, ds):
        data_dict = self.format_json(ds)
        dirname = self.datadict_to_dirname(data_dict)
        if dirname is None:
            return
        self.write(data_dict, dirname)


def write(data):
    if not WRITE_JSON_ENABLED:
        return

    collectd.info("write_json invoked")
    obj = WriteJson()
    obj.write_json(data)
