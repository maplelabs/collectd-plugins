from pytz import timezone
from pytz import utc
import _strptime
from datetime import datetime
import json
import os
import time
import math



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

def get_localized_datetime(utc_timestamp, totzStr):
    utcdt = datetime.utcfromtimestamp(utc_timestamp).replace(tzinfo = utc)
    toTimeZone = timezone(totzStr)
    toTimeZonedt = utcdt.astimezone(toTimeZone)
    return toTimeZonedt
