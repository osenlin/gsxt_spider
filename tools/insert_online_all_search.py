#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: insert_online_all_search.py
@time: 2017/6/7 16:32
"""

import json
from time import sleep

from common import util
from common.mongo import MongDb
from common.pybeanstalk import PyBeanstalk
from logger import Gsxtlogger
from tools.crawl_conf import company_info

mongo_db_source = {
    'host': '172.16.215.2',
    'port': 40042,
    'db': 'company_data',
    'username': 'work',
    'password': 'haizhi'
}

global_logger = Gsxtlogger('insert_company.log')
global_log = global_logger.get_logger()

# 搜索列表存储表
source_db = MongDb(mongo_db_source['host'], mongo_db_source['port'], mongo_db_source['db'],
                   mongo_db_source['username'],
                   mongo_db_source['password'], log=global_log)

beanstalk_consumer_conf = {'host': 'cs0.sz-internal.haizhi.com', 'port': 11400}
beanstalk = PyBeanstalk(beanstalk_consumer_conf['host'], beanstalk_consumer_conf['port'])


def main(search_name, province, unified_social_credit_code, param):
    item = {
        "_id": "9c9d8f8b848514f240f54a40b0a0c6f02622b3d87d54d353e525ca58d9dbe312",
        "province": province,
        "crawl_online": 0,
        "error_times": 0,
        "search_name": search_name,
        "rank": 1,
        "priority": 1,
        "in_time": "2017-04-25 01:42:30",
        "param": param,
        "crawl_online_time": "2017-02-28 04:35:18",
        "company_name": search_name,
        "unified_social_credit_code": unified_social_credit_code
    }

    item["_id"] = util.generator_id({'priority': 1}, unified_social_credit_code, item["province"])

    source_db.insert_batch_data("online_all_search", [item])
    sleep(2)

    tube = 'gs_{province}_scheduler'.format(province=item['province'])

    data = {
        'unified_social_credit_code': item['unified_social_credit_code'],
        'province': item['province'],
    }
    data_str = json.dumps(data)
    print data_str
    beanstalk.put(tube, data_str)


if __name__ == '__main__':
    for company, info in company_info.iteritems():
        sn = info['search_name']
        pro = info['province']
        code = info['unified_social_credit_code']
        par = info['param']
        main(sn, pro, code, par)
