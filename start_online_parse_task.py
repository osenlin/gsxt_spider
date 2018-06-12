#!/usr/bin/env python
# -*- coding:utf-8 -*-
import signal
import sys
import time

from common import util
from common.config_parser import ConfigParser
from common.generator import create_crawl_object
from common.global_resource import global_log
from common.pybeanstalk import PyBeanstalk
from config.conf import beanstalk_consumer_conf

is_running = True


def process_quit(signo, frame):
    global is_running
    global_log.info('收到信号退出进程...')
    is_running = False


class StartTaskCrawler(object):
    def __init__(self, config_file='config/online_gsxt_parse.conf'):
        self.worker_list = {}
        self.config_list = {}
        self.pool = None
        self.beanstalk_consumer_conf = beanstalk_consumer_conf

        # 没有指定配置文件直接抛异常
        if config_file is None or config_file == '':
            raise StandardError('province error...')

        # 加载配置
        self.load_config(config_file)

        # 开启日志
        self.log = global_log

        # 开启beanstalk
        self.beanstalk = PyBeanstalk(self.beanstalk_consumer_conf['host'], self.beanstalk_consumer_conf['port'])
        self.tube = self.beanstalk_consumer_conf['tube']

        # 初始化worker
        self.init_worker(self.config_list)

    # def __del__(self):
    #     merge_mq.close()
    #     merge_mq.join()

    def init_worker(self, config_list):
        self.log.info('初始化worker')
        for key, value in config_list.iteritems():
            self.worker_list[key] = create_crawl_object(value, key)
            self.log.info('初始化 {key} 完成..'.format(key=key))
        self.log.info('初始化全部worker完成...')

    def load_config(self, config_file):

        # 读取配置信息
        conf_parse = ConfigParser(config_file)

        # 加载单独省份信息
        self.config_list = conf_parse.get_all_session()

    def task_run(self):
        self.log.info('服务已开启, 等待消费数据')
        # 创建线程池
        count = 0
        while True:
            if not is_running:
                break

            job = self.beanstalk.reserve(self.tube, 3)
            if job is not None:
                count += 1
                body = job.body
                job.delete()
                self.log.info('当前消费数据索引: {count}'.format(count=count))

                json_data = util.json_loads(body)
                if json_data is None:
                    self.log.error('数据不是json格式: data = {data}'.format(data=body))
                    continue

                company = json_data.get('company', None)
                province = json_data.get('province', None)
                if company is None or province is None:
                    self.log.error('数据格式错误: data = {data}'.format(data=json_data))
                    continue

                if company == '':
                    self.log.error('company = 空字符串')
                    continue

                if province not in self.worker_list:
                    self.log.error('不支持当前省份: province = {province}'.format(province=province))
                    continue

                self.log.info('当前消费数据为: company = {company}'.format(company=company))
                self.worker_list[province].query_online_task(company)

        self.log.info('收到退出信号, 安全退出...')

    def start_worker(self):
        start_time = time.time()

        try:
            self.task_run()
        except Exception as e:
            self.log.error('周期任务异常!!!!')
            self.log.exception(e)

        end_time = time.time()
        self.log.info('起始时间: {st}'.format(st=start_time))
        self.log.info('结束时间: {et}'.format(et=end_time))
        self.log.info('消耗时间: {t}s'.format(t=end_time - start_time))


def main():
    config = 'config/online_gsxt_parse.conf'
    signal.signal(signal.SIGINT, process_quit)
    signal.signal(signal.SIGTERM, process_quit)
    signal.signal(signal.SIGQUIT, process_quit)
    signal.signal(signal.SIGUSR1, process_quit)

    length = len(sys.argv)
    global_log.info('当前参数个数为: length = {length}'.format(length=length))
    if length > 1:
        config = sys.argv[1]

    crawler = StartTaskCrawler(config_file=config)
    crawler.start_worker()


if __name__ == "__main__":
    main()
