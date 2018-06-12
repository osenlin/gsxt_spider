#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: hunan.py
@time: 2017/7/4 11:41
"""
import json

from common.mongo import MongDb
from common.pybeanstalk import PyBeanstalk
from logger import Gsxtlogger
from tools.company_list import data_list

beanstalk_parse_conf = {'host': 'cs0.sz-internal.haizhi.com', 'port': 11400, 'tube': 'online_gsxt_parse'}


def parse_task():
    parse_beanstalk = PyBeanstalk(beanstalk_parse_conf['host'], beanstalk_parse_conf['port'])
    parse_tube = beanstalk_parse_conf['tube']

    for company_name in data_list:
        data = {
            'company': company_name,
            'province': 'hunan',
        }

        parse_beanstalk.put(parse_tube, json.dumps(data))


def crawl_task():
    beanstalk_crawl_conf = {'host': 'cs0.sz-internal.haizhi.com', 'port': 11400, 'tube': 'gs_hunan_scheduler'}

    crawl_beanstalk = PyBeanstalk(beanstalk_crawl_conf['host'], beanstalk_crawl_conf['port'])
    crawl_tube = beanstalk_crawl_conf['tube']

    for company_name in data_list:
        data = {
            'company_name': company_name,
            'province': 'hunan',
        }
        data_str = json.dumps(data)
        crawl_beanstalk.put(crawl_tube, data_str)


def search_task():
    log = Gsxtlogger('hunan.log').get_logger()
    mongo_db_conf = {
        'host': '172.16.215.16',
        'port': 40042,
        'db': 'app_data',
        'username': 'read',
        'password': 'read'
    }

    # 搜索列表存储表
    source_db = MongDb(mongo_db_conf['host'], mongo_db_conf['port'], mongo_db_conf['db'],
                       mongo_db_conf['username'],
                       mongo_db_conf['password'], log=log)

    for company in data_list:
        item = source_db.find_one('enterprise_data_gov', {'company': company})
        if item is None:
            log.error(company)
            continue

        if 'shareholder_information' not in item:
            log.warn(company)
            continue


if __name__ == '__main__':
    # parse_task()
    # crawl_task()
    search_task()
