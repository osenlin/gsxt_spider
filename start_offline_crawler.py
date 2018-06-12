#!/usr/bin/env python
# -*- coding:utf-8 -*-
import random
import sys
import time

import gevent.pool
from gevent import monkey

monkey.patch_all()

from common.config_parser import ConfigParser
from common.generator import create_crawl_object
from common.global_resource import global_log, source_db


class StartTaskCrawler(object):
    def __init__(self, config_file='config/cmb_gsxt.conf',
                 source_table="offline_all_list",
                 province=None):
        self.worker_list = {}
        self.config_list = {}
        self.cur_thread_dict = {}
        self.limit_thread_dict = {}
        self.db_iter_dict = {}
        self.db_iter_valid_dict = {}
        self.count = 0

        # 所有省份加起来的协程数目
        self.thread_num = 8

        # 当前所指定的省份信息，如没有指定省份则默认全部省份
        self.province = province
        self.pool = None
        self.crawl_flag = "crawl_online"

        # 没有指定配置文件直接抛异常
        if config_file is None or config_file == '':
            raise StandardError('province error...')

        # 加载配置
        self.source_table = source_table
        self.load_config(config_file)

        # 日志信息
        self.log = global_log

        # 连接mongodb
        self.source_db = source_db

        # 初始化worker
        self.init_worker(self.config_list)

    def init_worker(self, config_list):
        for key, value in config_list.iteritems():
            self.worker_list[key] = create_crawl_object(value, key)
            self.cur_thread_dict[key] = 0

            # 获得数据库读取迭代器
            source_select_param = eval(value['source_select_param'])
            self.db_iter_dict[key] = self.__get_iterator_company_list(self.source_table, source_select_param)
            self.db_iter_valid_dict[key] = True

            self.log.info('初始化 {key} 完成..'.format(key=key))

    # 判断是否所有迭代器都失效了
    def is_all_invalid(self):
        for key, value in self.db_iter_valid_dict.iteritems():
            if value is True:
                return False

        return True

    def load_config(self, config_file):

        # 读取配置信息
        conf_parse = ConfigParser(config_file)
        config_dict = conf_parse.get_all_session()

        self.thread_num = 0
        for key, value in config_dict.items():
            thread_num = 0
            if value.get('thread_num', None) is not None:
                thread_num = int(value['thread_num'])
            if self.province is None:
                self.config_list[key] = value
                self.thread_num += thread_num
            elif self.province == key:
                self.config_list[key] = value
                self.thread_num += thread_num
            self.limit_thread_dict[key] = thread_num

        if len(self.config_list) <= 0 or self.thread_num <= 0:
            raise StandardError("没有加载到任何省份信息...或者线程数目错误: thread = {}".format(self.thread_num))

    def __get_iterator_company_list(self, source_table, source_select_param):
        return self.source_db.traverse_batch(source_table, source_select_param)

    # 获得回调结果
    def get_callback_result(self, result):
        self.cur_thread_dict[result] -= 1
        if self.cur_thread_dict[result] < 0:
            self.cur_thread_dict[result] = 0

    # 获得数据库中的具体数据
    def get_db_item(self):

        for province, db_iter in self.db_iter_dict.iteritems():

            # 判断是否当前迭代器失效了
            if self.db_iter_valid_dict[province] is False:
                continue

            # 判断是否线程池已经塞满了 线程满了则不会有数据加载出来
            if self.cur_thread_dict[province] >= self.limit_thread_dict[province]:
                continue

            try:
                item = db_iter.next()
                self.count += 1
                self.log.info("当前处理进度: province = {} count = {}".format(province, self.count))
                return item
            except StopIteration:
                self.db_iter_valid_dict[province] = False
                self.log.info("当前迭代器结束: province = {}".format(province))
        return None

    def task_run(self):
        start_time = time.time()
        # 统计读取出的数据量

        # 创建协程池
        self.pool = gevent.pool.Pool(self.thread_num)
        self.log.info('启用协程...')
        self.log.info('当前开启协程数目: thread_num = {num}'.format(num=self.thread_num))

        # 当所有迭代器都失效时则退出
        while not self.is_all_invalid():

            # 如果抓取的数据量足够大了，则需要退出 重新加载数据, 防止最终只有一个省份在抓取
            if self.count % 100 == 0:
                if (time.time() - start_time) > 3600 * 12:
                    self.log.info("当前抓取数量已经足够大需要重新加载数据库资源...")
                    break

            item = self.get_db_item()
            if item is None:
                self.log.warn("没有从迭代器中获得任何信息: item = None")
                time.sleep(1)
                continue

            try:
                # 获得省份信息
                province = item.get("province", None)
                if province not in self.worker_list:
                    self.log.error("省份信息错误的 province = {} item = {}".format(
                        province, item))
                    # source_db.delete(self.source_table, {'_id': item.get("_id")})
                    continue

                # 对当前省份使用线程数目进行统计
                self.cur_thread_dict[province] += 1
                self.pool.apply_async(self.worker_list[province].query_task,
                                      args=(item,), callback=self.get_callback_result)

            except Exception as e:
                self.log.error('线程池异常退出...')
                self.log.exception(e)

        self.log.info('总共加载数据量为: province = {province} count = {count}'.format(
            province=self.province, count=self.count))
        self.log.info('线程任务加载完成....')
        self.pool.join()
        self.log.info('完成抓取!!')

    def start_worker(self):
        sleep_time = random.randint(60, 120)
        start_time = time.time()

        try:
            self.task_run()
        except Exception as e:
            self.log.error('周期任务异常, 强制退出程序!!!!!!')
            self.log.exception(e)
            sys.exit(1)

        end_time = time.time()
        self.log.info('扫描起始时间: {st}'.format(st=start_time))
        self.log.info('扫描结束时间: {et}'.format(et=end_time))
        self.log.info('扫描消耗时间: {t}s'.format(t=end_time - start_time))
        self.log.info('休眠{s}s'.format(s=sleep_time))
        time.sleep(sleep_time)
        self.log.info('完成扫描...')


def main():
    config = 'config/offline_gsxt_searchlist.conf'
    source_table = "offline_all_list"
    province = None

    length = len(sys.argv)
    global_log.info('当前参数个数为: length = {length}'.format(length=length))
    if length > 1:
        config = sys.argv[1]
        if length > 2:
            source_table = sys.argv[2]
            if length > 3:
                province = sys.argv[3]

    # 如果省份信息是all 则默认为全部站点进行抓取
    if province == 'all':
        province = None

    global_log.info("当前需要处理的省份为: {}".format(province))
    crawler = StartTaskCrawler(config_file=config,
                               source_table=source_table,
                               province=province)
    crawler.start_worker()


if __name__ == "__main__":
    main()
