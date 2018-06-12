# -*- coding: utf-8 -*-
import beanstalkc


class PyBeanstalk(object):
    def __init__(self, host, port=11300):
        self.host = host
        self.port = port
        self.__conn = beanstalkc.Connection(host, port)

    def __del__(self):
        self.__conn.close()

    # beanstalk重连
    def reconnect(self):
        self.__conn.reconnect()

    def put(self, tube, body, priority=2 ** 31, delay=0, ttr=10):
        self.__conn.use(tube)
        if len(body) >= 3145728:
            return None
        return self.__conn.put(body, priority, delay, ttr)

    def get_tube_count(self, tube):
        return self.__conn.stats_tube(tube)["current-jobs-ready"]

    def reserve(self, tube, timeout=20):
        self.__conn.watch(tube)
        return self.__conn.reserve(timeout)

    def clear(self, tube):
        try:
            while 1:
                job = self.reserve(tube, 1)
                if job is None:
                    break
                else:
                    job.delete()
        except Exception as e:
            pass

    def stats_tube(self, tube):
        return self.__conn.stats_tube(tube)
