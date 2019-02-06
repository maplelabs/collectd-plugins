import logging
import redis
import json
from configuration import redis_server

logger = logging.getLogger(__name__)

redis_connection = None
redis_error = None

def get_redis_conn():
    """Function to connect redis database"""
    global redis_connection, redis_error
    try:
        if not redis_connection:
            redis_connection = redis.Redis(host=redis_server["host"], port=redis_server["port"], password=redis_server["password"])
            return redis_connection
    except:
        logger.exception("Plugin Oozie: Unable to connect redis")
        redis_error = True
        return None


def read_from_redis(key):
    """Function to read data from redis database"""
    global redis_connection, redis_error
    value = None
    try:
        if not redis_connection:
            redis_connection = get_redis_conn()
        if redis_connection:
            if not redis_connection.exists(key):
                value = {}
            else:
                value = {}
                data = redis_connection.get(key)
                logger.debug("the value for key:{0} is {1}".format(key, data))
                if data:
                    value = json.loads(data)
    except Exception as e:
        logger.exception("Plugin Oozie: Failed to get data from redis for key:{0}".format(key))
        redis_error = True
    return value


def handle_redis_error():
    global redis_error, redis_connection
    if redis_error:
        redis_connection = get_redis_conn()

    if not redis_connection:
        return False
    else:
        redis_error = False
        return True

def write_to_redis(key, value):
    """Function to write data into redis"""
    global redis_connection, redis_error
    try:
        if not redis_connection:
            redis_connection = get_redis_conn()
        if redis_connection:
            return redis_connection.set(key, value)
    except Exception as e:
        redis_error = True
        logger.exception("Failed to set key:{0} with value:{1} in redis".format(key, value))
    return False

