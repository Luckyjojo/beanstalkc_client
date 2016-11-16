#!/usr/bin/env Python
# -*- coding:utf-8 -*-

import json
import sys
import time
import beanstalkc
from ConfigParser import ConfigParser

class beanstalkClient(object):
    """docstring for message"""
    def __init__(self,conf_path):
        self.cf = ConfigParser()
        self.cf.read(conf_path)
        self.localhost = self.cf.get("beanstalk","localhost")
        self.port = self.cf.get("beanstalk","port")
        self.wait_query = self.cf.get("beanstalk","wait_query")

    def connect(self):
        try:
            self.bstk = beanstalkc.Connection(host=self.localhost, port=int(self.port))
        except Exception,err:
            print err
            return False
        return True

    def put(self,message):
        self.bstk.put(message)

    
    def use(self,bar):
        self.bstk.use(bar)

    def pull(self,bar,runFunc,bury = False,delete=True):
        # try:
        results = []
        while True:
            self.bstk.watch(bar)
            job = self.bstk.reserve(1)
            if job == None:
                time.sleep(int(self.wait_query))
            else:
                result = json.loads(job.body)
                runFunc(result)
                if bury:
                    job.bury()
                    job.kick()
                if delete:
                    job.delete()
        return results
        # except Exception, e:
        #     print e
        #     return False

    def __del__(self):
        if self.bstk in locals():
            self.bstk.close()
