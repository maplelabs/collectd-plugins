from pytz import timezone
import _strptime
from datetime import datetime
import json
import os
import time
import logging
import math


logger = logging.getLogger("__name__")


def get_unix_timestamp(date_text):
    #logger.debug("date_text %s" % date_text)
    dateInput = date_text.rsplit(' ', 1)
    utc = timezone(dateInput[1])
    datetimedata = datetime.strptime(dateInput[0], "%a, %d %b %Y %H:%M:%S")
    utctimestamp = utc.localize(datetimedata)
    #logger.debug(utctimestamp.tzinfo)
    return int(time.mktime(utctimestamp.timetuple()))


def convert_camelcase(str_to_convert, separator):

    c = ''.join(x for x in str_to_convert.title() if not x == separator)
    c = c[0].lower() + c[1::]
    return c


def write_json_to_file(data, outfile):
    if os.path.exists(outfile):
        with open(outfile, 'a') as ofile:
            json.dump(data, ofile)
            ofile.write("\n")

