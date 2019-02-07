"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""

import json
from time import time, sleep

import asyncio
import tornado.httpserver
import tornado.options
import tornado.web
import tornado.websocket
from tornado.ioloop import IOLoop
from tornado.web import asynchronous
from tornado.options import define, options

from concurrent.futures import ThreadPoolExecutor
from tornado.platform.asyncio import AnyThreadEventLoopPolicy

from util import SparkHandler
from util import HdfsHandler

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r'/log/runquery', QueryHandler),
            (r'/log/fetchresult', DataHandler),
            (r'/log/getprogress', ProgressHandler)
        ]
        tornado.web.Application.__init__(self, handlers)

class WorkerHandler(object):
    def do_work(self, task, on_error=None):
        try:
            asyncio.set_event_loop_policy(AnyThreadEventLoopPolicy())
            return task()
        except Exception as ex:
            if on_error:
                return on_error(ex)

class BaseHandler(WorkerHandler, tornado.web.RequestHandler):

    def handle_error(self, ex):
        self.set_status(500)
        self.finish(str(ex))

    def send_result(self,val):
        self.set_status(200)
        self.finish(val)

    def send_status(self):
        self.set_status(200)
        self.finish()

    def send_accepted(self, ret_val):
        self.set_status(202)
        self.finish(ret_val)

    def send_client_error(self, msg):
        self.set_status(400)
        self.finish(msg)

class QueryHandler(BaseHandler):
    
    async def post(self):
        try:
            request_body = json.loads(self.request.body)
        except ValueError:
            self.send_client_error("Invalid request body")

        query_id =("%s_%d" % ("query", time()))

        def to_do():
            spark.executeJob(request_body, query_id)
            self.send_result(query_id)

        await IOLoop.current().run_in_executor(executor, self.do_work, to_do, self.handle_error)

class DataHandler(BaseHandler):

    async def get(self):

        query_id = self.get_argument('query_id')

        def to_do():
            self.send_result(hdfs.readFile(query_id))

        await IOLoop.current().run_in_executor(executor, self.do_work, to_do, self.handle_error)

class ProgressHandler(WorkerHandler, tornado.websocket.WebSocketHandler):
    async def open(self):
        query_id = self.get_argument('query_id')
        print("WebSocket opened for %s" % (query_id))

        def to_do():
            spark.sendProgress(self, query_id)

        await IOLoop.current().run_in_executor(executor, self.do_work, to_do, self.handle_error) 

spark = None
hdfs = None
executor = None

def main():

    global spark
    global hdfs
    global executor

    spark = SparkHandler()
    hdfs = HdfsHandler()
    executor = ThreadPoolExecutor(5)

    define("port", default=4400, help="run on the given port", type=int)
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)

    print("Server started and listening on %d"% options.port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
