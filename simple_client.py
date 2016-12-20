#!/usr/bin/env Python
# -*- coding:utf-8 -*-

import json
import sys
import time
import beanstalkc
from ConfigParser import ConfigParser

class beanstalkClient(object):
    """beanstalk client operation"""

    def __init__(self,conf_path):
        self.cf = ConfigParser()
        self.cf.read(conf_path)
        self.localhost = self.cf.get("beanstalkd","localhost")
        self.port = self.cf.get("beanstalkd","port")
        self.wait_query = self.cf.get("beanstalkd","wait_query")

    def connect(self):
        try:
            self.bstk = beanstalkc.Connection(host=self.localhost, port=int(self.port))
        except Exception,err:
            return False
        return True

    def put(self,message):
        if not isinstance(message,str):
            message = json.dumps(message)
        self.bstk.put(message)
    
    def use(self,bar):
        self.bstk.use(bar)

    def pull(self,bar,runFunc,bury = False,delete=True):
        while True:
            self.bstk.watch(bar)
            job = self.bstk.reserve(0)
            if job == None:
                time.sleep(int(self.wait_query))
            else:
                result = json.loads(job.body)
                response = runFunc(result)    # response follows RESTful API style
                if response.get('error'):
                    job.bury()
                    job.kick()
                else:
                    job.delete()

    def close():
        self.bstk.close()
