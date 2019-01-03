from pytz import timezone, utc
from datetime import datetime
import json
import os
import time
import logging
from configuration import cluster_timezones
import math
import errno


logger = logging.getLogger(__name__)

def remove_dot(doc, field):
    new_field = '_' + field.split('.')[0] + '_' + field.split('.')[1].lower()
    doc[new_field] = doc.pop(field)

def convert_to_epoch(gmt):
    t = gmt.replace("GMT", "")
    ts = time.strptime(t, '%Y-%m-%dT%H:%M:%S.%f')
    timestamp = int(time.mktime(ts))
    return timestamp

def get_current_gmt_in_isoformat():
    currentutciniso = datetime.utcnow().isoformat()
    return currentutciniso.replace(currentutciniso[-3:], 'GMT')

def get_localized_datetime(utc_timestamp, totzStr):
    utcdt = datetime.utcfromtimestamp(utc_timestamp).replace(tzinfo = utc)
    toTimeZone = timezone(totzStr)
    toTimeZonedt = utcdt.astimezone(toTimeZone)
    return toTimeZonedt


def get_unix_timestamp(date_text):
    dateInput = date_text.rsplit(' ', 1)
    is_dst = False
    if 'D' in dateInput[1]:
        is_dst = True
    if dateInput[1] in cluster_timezones:
        pytzTimezone = timezone(cluster_timezones[dateInput[1]])
    else:
        pytzTimezone = timezone(dateInput[1])
    dt_with_tz = pytzTimezone.localize(datetime.strptime(dateInput[0], "%a, %d %b %Y %H:%M:%S"), is_dst=is_dst)
    ts = int((dt_with_tz - datetime(1970, 1, 1, tzinfo=utc)).total_seconds())
    return ts



def convert_camelcase(str_to_convert, separator):

    c = ''.join(x for x in str_to_convert.title() if not x == separator)
    c = c[0].lower() + c[1::]
    return c


def write_json_to_file(data, outfile):
    if os.path.exists(outfile):
        with open(outfile, 'a') as ofile:
            json.dump(data, ofile)
            ofile.write("\n")

def isInt(value):
    try:
        int(value)
        return True
    except ValueError:
        return False

def isFloat(value):
    try:
       float(value)
       return True
    except ValueError:
       return False

def mkdir_p(path):
    try:
        print(path)
        os.makedirs(path)
        result = True
        print("Directory successfully created")
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            print("Directory already exists")
            result = True
        else:
            result = False
    return result

