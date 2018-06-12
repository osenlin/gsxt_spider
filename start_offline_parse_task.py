#!/usr/bin/env python
# -*- coding:utf-8 -*-
import signal
import sys
import time

import gevent.pool
from gevent import monkey

monkey.patch_all()

from common.config_parser import ConfigParser
from common.generator import create_crawl_object
from common.global_resource import webpage_db_old, global_log, webpage_db_new, CHOOSE_DB_NEW, CHOOSE_DB_OLD

is_running = True


def process_quit(signo, frame):
    global is_running
    global_log.info('收到信号退出进程...')
    is_running = False


class StartTaskCrawler(object):
    def __init__(self, config_file='config/offline_gsxt_parse.conf',
                 province=None):
        self.worker_list_new = {}
        self.worker_list_old = {}
        self.config_list = {}

        # 没有指定配置文件直接抛异常
        if config_file is None or config_file == '':
            raise StandardError('province error...')

        self.province = province

        # 加载配置
        self.source_table = 'cmb_list_all'
        self.source_select_param = {}
        self.load_config(config_file)

        # 开启日志
        self.log = global_log

        # 连接mongodb
        self.webpage_db_old = webpage_db_old
        self.webpage_db_new = webpage_db_new

        # 初始化worker
        self.init_worker(self.config_list)

        self.count = 0

    def init_worker(self, config_list):
        for key, value in config_list.iteritems():
            self.worker_list_new[key] = create_crawl_object(value, key)
            self.worker_list_old[key] = create_crawl_object(value, key)
            self.log.info('初始化 {key} 完成..'.format(key=key))

    def load_config(self, config_file):
        # 读取配置信息
        conf_parse = ConfigParser(config_file)
        self.config_list = conf_parse.get_all_session()
        # 如果有省份信息 且 省份信息如果不相等 则不生成对象
        if self.province is not None and self.province in self.config_list:
            temp = self.config_list
            self.config_list = {self.province: temp[self.province]}

    @staticmethod
    def __get_iterator_company_list(db, source_table, source_select_param):
        return db.traverse_batch(source_table, source_select_param)

    # 单个省份运行任务
    def task(self, province, work_list, source_table, source_select_param, db, which_db):
        self.log.info("开始扫描: {}".format(province))
        company_list = self.__get_iterator_company_list(db, source_table, source_select_param)
        for item in company_list:
            self.count += 1
            work_list[province].query_offline_task(item, which_db)
            self.log.info('当前任务进度: province = {province} count = {count}'.format(
                province=province, count=self.count))
            if not is_running:
                self.log.info('kill break')
                break

    # 运行所有省份解析
    def task_all(self, work_list, db, which_db):
        for province, config_dict in self.config_list.iteritems():
            source_table = config_dict['source_table']

            # 获取抓取成功标志 这里不能删掉never_success_flag success_flag， eval需要用到
            never_success_flag = int(config_dict.get('never_success_flag', '-100'))
            success_flag = int(config_dict.get('success_flag', '50'))
            source_select_param = eval(config_dict['source_select_param'])
            self.log.info("当前省份筛选条件: province = {} {}".format(province, source_select_param))
            self.task(province, work_list, source_table, source_select_param, db, which_db)
            if not is_running:
                self.log.info('kill break')
                return

        self.log.info("所有省份解析完成: which_db = {}".format(which_db))

    def task_run(self):
        pool = gevent.pool.Pool(2)

        pool.apply_async(self.task_all,
                         args=(self.worker_list_new, self.webpage_db_new, CHOOSE_DB_NEW))

        pool.apply_async(self.task_all,
                         args=(self.worker_list_old, self.webpage_db_old, CHOOSE_DB_OLD))

        self.log.info("所有任务加载完成..")
        pool.join()

        current_time = 0
        sleep_time = 300
        self.log.info('完成解析!!, 开始休眠休眠时间为: {rand}'.format(rand=sleep_time))

        while is_running and current_time < 300:
            time.sleep(1)
            current_time += 1

    def start_worker(self):

        start_time = time.time()

        try:
            self.task_run()
        except Exception as e:
            self.log.error('周期任务异常!!!!')
            self.log.exception(e)

        end_time = time.time()
        self.log.info('扫描起始时间: {st}'.format(st=start_time))
        self.log.info('扫描结束时间: {et}'.format(et=end_time))
        self.log.info('扫描消耗时间: {t}s'.format(t=end_time - start_time))

        self.log.info('完成扫描... 退出程序')


def main():
    config = 'config/offline_gsxt_parse.conf'
    province = None

    signal.signal(signal.SIGINT, process_quit)
    signal.signal(signal.SIGTERM, process_quit)
    signal.signal(signal.SIGQUIT, process_quit)
    signal.signal(signal.SIGUSR1, process_quit)

    length = len(sys.argv)
    if length > 1:
        config = sys.argv[1]
        if length > 2:
            province = sys.argv[2]
        else:
            province = None

    # 如果省份信息是all 则默认为全部站点进行抓取
    if province == 'all':
        province = None

    global_log.info("当前需要处理的省份为: {}".format(province))

    try:
        crawler = StartTaskCrawler(config_file=config, province=province)
        crawler.start_worker()
    except Exception as e:
        global_log.error("初始化错误:")
        global_log.exception(e)


if __name__ == "__main__":
    main()
