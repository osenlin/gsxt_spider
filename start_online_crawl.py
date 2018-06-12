#!/usr/bin/env python
# -*- coding:utf-8 -*-
import sys
import time

from config.conf import beanstalk_consumer_conf

if len(sys.argv) > 1:
    import gevent.pool
    from gevent import monkey

    monkey.patch_all()
else:
    from multiprocessing.dummy import Pool as ThreadPool

__all__ = ['ThreadPool']

import signal
from common.global_resource import source_db, global_log, is_debug
from common import util
from common.config_parser import ConfigParser
from common.generator import create_crawl_object
from common.pybeanstalk import PyBeanstalk

is_running = True


def process_quit(signo, frame):
    global is_running
    global_log.info('收到信号退出进程...')
    is_running = False


class StartTaskCrawler(object):

    # 最大运行时间
    MAX_RUN_TIME = 12 * 60 * 60

    def __init__(self, config_file='config/cmb_gsxt.conf', province=None):
        self.worker_list = {}
        self.config_list = {}
        self.thread_num = 8
        self.province = province
        self.pool = None
        self.beanstalk_consumer_conf = beanstalk_consumer_conf
        self.crawl_flag = 'crawl_online'
        self.source_table = 'online_all_list'
        self.tube = ''

        # 不指定抓取的站点直接抛异常
        if province is None or province == '':
            raise StandardError('province error...')

        # 没有指定配置文件直接抛异常
        if config_file is None or config_file == '':
            raise StandardError('province error...')

        # 加载配置
        self.load_config(config_file)

        # 日志信息
        self.log = global_log

        # 开启beanstalk
        self.beanstalk = PyBeanstalk(self.beanstalk_consumer_conf['host'], self.beanstalk_consumer_conf['port'])

        # 连接mongodb
        self.source_db = source_db

        # 初始化worker
        self.init_worker(self.config_list)

    def init_worker(self, config_list):
        for key, value in config_list.iteritems():
            self.worker_list[key] = create_crawl_object(value, key)
            self.log.info('初始化 {key} 完成..'.format(key=key))

    def load_config(self, config_file):

        # 读取配置信息
        conf_parse = ConfigParser(config_file)

        # 加载单独省份信息
        config_dict = conf_parse.get_session(self.province)
        if config_dict is None:
            raise StandardError('站点信息错误...{province}'.format(province=self.province))

        # 更改线程数目
        if config_dict.get('thread_num', None) is not None:
            self.thread_num = int(config_dict['thread_num'])

        # 改变种子表指向
        if config_dict.get('source_table', None) is not None:
            self.source_table = config_dict['source_table']
        else:
            raise StandardError('没有指定原始种子表: source_table')

        # 获得beanstalk配置信息
        config = eval(config_dict.get('beanstalk_consumer_conf', 'None'))
        if config is not None:
            self.beanstalk_consumer_conf = config

        # 标志位
        crawl_flag = config_dict.get('crawl_flag', 'crawl_online')
        if crawl_flag is not None:
            self.crawl_flag = crawl_flag

        # 标志位
        consumer_tube = config_dict.get('consumer_tube', '')
        if consumer_tube is not None and consumer_tube != '':
            self.tube = consumer_tube
        else:
            raise StandardError('没有tube!!!')

        # 添加到配置列表
        self.config_list[self.province] = config_dict

    def task_run(self):

        result_list = []

        # 创建协程池
        if not is_debug:
            self.pool = gevent.pool.Pool(self.thread_num)
        else:
            self.pool = ThreadPool(processes=self.thread_num)

        self.log.info('当前开启协程数目: thread_num = {num}'.format(num=self.thread_num))
        self.log.info('province: {province}服务已开启, 等待消费数据'.format(province=self.province))
        # 创建线程池
        count = 0
        start_run_time = time.time()
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
                    self.log.error('数据格式错误: msg = {msg}'.format(msg=body))
                    time.sleep(5)
                    continue

                province = json_data.get('province')
                if province is None or province == '':
                    self.log.error('没有province: {msg}'.format(msg=body))
                    continue

                company_name = json_data.get('company_name')
                unified_social_credit_code = json_data.get('unified_social_credit_code')
                start_schedule_time = json_data.get('start_schedule_time', '')
                if company_name is None and unified_social_credit_code is None:
                    self.log.error('没有company 与 unified_social_credit_code: {msg}'.format(msg=body))
                    continue

                if company_name is not None and company_name == '':
                    self.log.error('company = 空字符串, data = {data}'.format(
                        data=body))
                    continue

                if unified_social_credit_code is not None and unified_social_credit_code == '':
                    self.log.error('unified_social_credit_code = 空字符串, data = {data}'.format(
                        data=body))
                    continue

                if province != self.province:
                    self.log.warn('province 不正确: province = {province} data = {body}'.format(
                        province=self.province, body=body))
                    continue

                if company_name is not None:
                    self.log.info('当前消费数据为: province = {province} company = {company}'.format(
                        province=province, company=company_name))
                elif unified_social_credit_code is not None:
                    self.log.info('当前消费数据为: province = {province} unified_social_credit_code = {code}'.format(
                        province=province, code=unified_social_credit_code))

                # 优先使用企业名单
                if company_name is not None:
                    data = {
                        '_id': util.generator_id({}, company_name, province),
                        'company_name': company_name,
                        'province': province,
                        'in_time': util.get_now_time(),
                        'start_schedule_time': start_schedule_time,
                    }
                else:
                    data = {
                        '_id': util.generator_id({}, unified_social_credit_code, province),
                        'unified_social_credit_code': unified_social_credit_code.strip().upper(),
                        'province': province,
                        'in_time': util.get_now_time(),
                        'start_schedule_time': start_schedule_time,
                    }

                pool_result = self.pool.apply_async(self.worker_list[self.province].query_online_task,
                                                    args=(data,))

                result_list.append(pool_result)
                if len(result_list) >= 1000:
                    for result in result_list:
                        result.get()
                    del result_list[:]

            # 如果达到最大运行时间 则重启服务
            run_time = time.time()
            if int(run_time) - int(start_run_time) >= self.MAX_RUN_TIME:
                break

        if is_debug:
            self.pool.close()
        self.pool.join()

        for result in result_list:
            result.get()
        del result_list[:]
        del result_list

        self.log.info('收到退出信号, 安全退出...')

    def start_worker(self):
        start_time = time.time()

        try:
            self.task_run()
        except Exception as e:
            self.log.error('周期任务异常!!!!')
            self.log.exception(e)
            exit(1)

        end_time = time.time()
        self.log.info('扫描起始时间: {st}'.format(st=start_time))
        self.log.info('扫描结束时间: {et}'.format(et=end_time))
        self.log.info('扫描消耗时间: {t}s'.format(t=end_time - start_time))


def main():
    config = 'config/online_gsxt_crawl.conf'
    province = 'anhui'

    signal.signal(signal.SIGINT, process_quit)
    signal.signal(signal.SIGTERM, process_quit)
    signal.signal(signal.SIGQUIT, process_quit)
    signal.signal(signal.SIGUSR1, process_quit)

    length = len(sys.argv)
    global_log.info('当前参数个数为: length = {length}'.format(length=length))
    if length > 1:
        config = sys.argv[1]
        if length > 2:
            province = sys.argv[2]

    try:
        crawler = StartTaskCrawler(config_file=config,
                                   province=province)
        crawler.start_worker()
    except Exception as e:
        global_log.error('初始化异常!!!!')
        global_log.exception(e)


if __name__ == "__main__":
    main()
