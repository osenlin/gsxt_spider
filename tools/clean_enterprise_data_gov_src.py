#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: clean_enterprise_data_gov_src.py
@time: 2017/7/19 23:01
"""

import sys

sys.path.append('../')
from common import util
from common.mongo import MongDb

from logger import Gsxtlogger

db_conf = {
    'host': '172.16.215.16',
    'port': 40042,
    'db': 'app_data',
    'username': 'work',
    'password': 'haizhi',
}

log = Gsxtlogger('clean_enterprise_data_gov_src.log').get_logger()

source_db = MongDb(db_conf['host'], db_conf['port'], db_conf['db'],
                   db_conf['username'], db_conf['password'], log=log)


def main():
    result_list = []
    source_table = 'enterprise_data_gov'
    count = 0
    deal_total = 0
    for item in source_db.traverse_batch(source_table):
        item['_utime'] = util.get_now_time()

        _record_id = item.get('_record_id', '')
        company = item.get('company', '')

        count += 1

        src_list = item.get('_src')
        if not isinstance(src_list, list):
            log.info("没有_src {record} {company}".format(record=_record_id, company=company))
            continue

        if len(src_list) <= 10:
            continue

        temp_list = []
        temp_list.extend(src_list[0:10])

        item['_src'] = temp_list

        deal_total += 1
        result_list.append(item)
        if len(result_list) >= 1000:
            source_db.insert_batch_data(source_table, result_list)
            del result_list[:]
            log.info("当前扫描数目: count = {}".format(count))
            log.info("当前处理的数目: deal = {}".format(deal_total))

    source_db.insert_batch_data(source_table, result_list)
    log.info("当前扫描数目: count = {}".format(count))
    log.info("当前处理的数目: deal = {}".format(deal_total))


if __name__ == '__main__':
    main()
