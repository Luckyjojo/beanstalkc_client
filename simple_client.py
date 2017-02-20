#!/usr/bin/python2
# -*- coding:utf-8 -*-

import sys
import json
import time
import beanstalkc

from ConfigParser import ConfigParser

class beanstalkClient(object):
    """Beanstalk's Client instances"""

    def __init__(self, conf_path):
        self.cf = ConfigParser()
        self.cf.read(conf_path)
        self.localhost = self.cf.get("beanstalkd","localhost")
        self.port = self.cf.get("beanstalkd","port")
        self.wait_query = self.cf.get("beanstalkd","wait_query")

    def connect(self):
        try:
            self.bstk = beanstalkc.Connection(host=self.localhost, port=int(self.port))
        except Exception as err:
            return False
        return True

    def put(self, message):
        if isinstance(message,dict):
            message = json.dumps(message)
        self.bstk.put(message)
    
    def use(self, bar):
        self.bstk.use(bar)

    def pull(self, bar, runFunc, bury=False, delete=True):
        """Pull job's body from bar.

        Args:
            bar: A bar to pull.
            runFunc: A function using job's body as variable. Job's body is in the bar.
            bury: Whether to bury job's body.
            delete: Whether to delete job's body.
        """
        while True:
            self.bstk.watch(bar)
            job = self.bstk.reserve(0)
            if not job:
                time.sleep(int(self.wait_query))
            else:
                result = json.loads(job.body)
                runFunc(result)
                if bury:
                    job.bury()
                    job.kick()
                if delete:
                    job.delete()

    def close(self):
        self.bstk.close()
