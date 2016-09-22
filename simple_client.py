#!/usr/bin/env python
#-*-coding:utf8-*-
# authored by Jojo
# email lishixuanmail@gmail.com
import beanstalkc
import json

class beanstalkClient(object):
    """docstring for message"""
    def __init__(self):
        self.localhost = "192.168.50.42"
        self.port = 11300
        self.bar = "feed_result"

    def connect(self):
        try:
            self.bstk = beanstalkc.Connection(host=self.localhost, port=self.port)
        except Exception,err:
            print err
            return False
        return True

    def put(self,message):
        try:
            if self.connect():
                self.bstk.watch(self.bar)
                self.bstk.put(message)
                return True
            return False
        except Exception,err:
            print err
            return False

    def pull(self,bury = True,delete=False,pull_limit = 3):
        try:
            results = []
            if self.connect():
                for i in range(pull_limit):
                    self.bstk.watch(self.bar)
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
