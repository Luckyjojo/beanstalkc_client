#!/usr/bin/env python
#-*-coding:utf8-*-

import beanstalkc
import json
from ConfigParser import ConfigParser

class beanstalkClient(object):
    """docstring for message"""
    def __init__(self,conf_path):
        self.cf = ConfigParser()
        self.cf.read(conf_path)
        self.localhost = self.cf.get("beanstalk","localhost")
        self.port = self.cf.get("beanstalk","port")
        self.pull_count = self.cf.get("beanstalk","count")


    def connect(self):
        try:
            self.bstk = beanstalkc.Connection(host=self.localhost, port=self.port)
        except Exception,err:
            print err
            return False
        return True

    def put(self,message,bar):
        try:
            if self.connect():
                self.bstk.watch(bar)
                self.bstk.put(message)
                return True
            return False
        except Exception,err:
            print err
            return False

    def pull(self,bar,bury = True,delete=False,pull_limit = self.pull_count):
        try:
            results = []
            if self.connect():
                for i in range(pull_limit):
                    self.bstk.watch(bar)
                    job = self.bstk.reserve()
                    result = job.body
                    result = json.loads(result)
                    results.append(result)
                if bury:
                    job.bury()
                    job.kick()
                if delete:
                    job.delete()
            return results
        except Exception, e:
            print e
            return False

    def __del__(self):
        if self.bstk in locals():
            self.bstk.close()
