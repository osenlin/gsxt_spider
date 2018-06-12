#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: queue_mq_thread.py
@time: 2016/12/8 13:45
"""
import threading
import time
from Queue import Queue

from beanstalkc import SocketError

from common.pybeanstalk import PyBeanstalk
from thriftRPC.entity_extractor.ttypes import EntityExtractorInfo

__all__ = ['EntityExtractorInfo']


class MqQueueThread(threading.Thread):
    PAUSE_COUNT_LV1 = 1000
    PAUSE_COUNT_LV2 = 10000
    PAUSE_COUNT_LV3 = 50000
    PAUSE_COUNT_LV4 = 100000
    PAUSE_COUNT_LV5 = 1000000

    PAUSE_TIME_LV1 = 1
    PAUSE_TIME_LV2 = 3
    PAUSE_TIME_LV3 = 10
    PAUSE_TIME_LV4 = 20
    PAUSE_TIME_LV5 = 300

    def __init__(self, server_conf=None, log=None, is_open=True):
        threading.Thread.__init__(self)
        self.daemon = True

        self.log = log

        # 判断是否需要开启消息队列
        self.is_open = is_open
        if not self.is_open:
            return

        # 判断是否消息队列已中断
        self.is_connect = True

        # 判断是否需要暂停
        self.is_pause = False
        self.pause_time = self.PAUSE_TIME_LV1

        # 输送队列
        self.queue = Queue()

        if server_conf is None:
            raise StandardError('没有消息队列配置信息...')

        # 获取消息队列配置
        self.server_conf = server_conf

        # 消息队列
        if self.is_open:
            self.beanstalk = PyBeanstalk(self.server_conf['host'], self.server_conf['port'])
        else:
            self.beanstalk = None
        self.output_tube = self.server_conf['tube']

    def __del__(self):
        self.log.info('消息队列线程退出...')

    # 判断是否需要暂停
    def is_need_pause(self):
        try:
            count = self.beanstalk.get_tube_count(self.output_tube)
        except Exception as e:
            self.log.error('获取当前队列数目失败..开启消息队列休眠...')
            self.log.exception(e)
            count = self.PAUSE_COUNT_LV1

        if count < self.PAUSE_COUNT_LV1:
            self.is_pause = False
            self.pause_time = self.PAUSE_TIME_LV1
            return

        self.is_pause = True
        if count >= self.PAUSE_COUNT_LV5:
            self.pause_time = self.PAUSE_TIME_LV5
        elif count >= self.PAUSE_COUNT_LV4:
            self.pause_time = self.PAUSE_TIME_LV4
        elif count >= self.PAUSE_COUNT_LV3:
            self.pause_time = self.PAUSE_TIME_LV3
        elif count >= self.PAUSE_COUNT_LV2:
            self.pause_time = self.PAUSE_TIME_LV2
        else:
            self.pause_time = self.PAUSE_TIME_LV1

        # 开始休眠
        time.sleep(self.pause_time)

    def close(self):
        self.queue.put_nowait('@@##$$')
        self.log.info('发送线程退出指令...')

    def push_msg(self, msg):
        if self.is_open:
            self.queue.put_nowait(str(msg))

    def run(self):
        self.log.info('开始运行消息队列...')
        while True:
            # 判断是否打开了消息队列
            if not self.is_open:
                self.log.info('没有打开消息队列, 退出!')
                break

            try:
                msg = self.queue.get()
                if msg == '@@##$$':
                    break

                while True:
                    try:
                        self.beanstalk.put(self.output_tube, msg)

                        # 发送前先判断是否需要休眠
                        # self.is_need_pause()

                        # 设置消息队列连接状态
                        self.is_connect = True
                        break
                    except SocketError as e:
                        # 设置当前消息队列已中断, 减缓发送数据速度
                        self.is_connect = False
                        time.sleep(10)
                        self.beanstalk.reconnect()
                        self.log.warn("reconnect beanstalk...")
                        self.log.exception(e)
                    except Exception as e:
                        self.is_connect = False
                        self.log.error('捕获异常休眠...')
                        self.log.exception(e)
                        time.sleep(10)
            except Exception as e:
                self.log.info('当前队列大小: size = {size}'.format(size=self.queue.qsize()))
                self.log.exception(e)
                time.sleep(5)

        self.log.info('消息队列线程正常退出.')
