#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: test_mongodb_result_total.py
@time: 2016/12/9 22:22
"""
import sys
import time

sys.path.append('../')
from common.config_parser import ConfigParser
from config.conf import mongo_db_target
from common.mongo import MongDb
from logger import Gsxtlogger

# 开启日志
log = Gsxtlogger('mongodb_count_total.py.log', for_mat='').get_logger()

target_db = MongDb(mongo_db_target['host'], mongo_db_target['port'], mongo_db_target['db'],
                   mongo_db_target['username'], mongo_db_target['password'], log=log)


def total_source(config_list):
    start_time = time.time()

    log.info('开始统计种子: ')
    province_total_list = []
    count_total = 0
    for key, value in config_list.iteritems():
        source_table = value.get('source_table', None)
        if source_table is None:
            log.error('读取表信息错误')
            continue

        province = value.get('province', None)
        if province is None:
            log.error('读取省份信息错误')
            continue

        total = target_db.select_count(source_table)
        province_total_list.append({'province': province, 'total': total})
        count_total += total

    sort_list = sorted(province_total_list, key=lambda x: x['total'])
    for item in sort_list:
        log.info('province = {province} total = {total}'.format(
            province=item['province'], total=item['total']
        ))

    log.info('总计数目: count_total = {total}'.format(total=count_total))

    end_time = time.time()

    log.info('')
    log.info('start_time: {start}'.format(start=start_time))
    log.info('end_time: {end}'.format(end=end_time))
    log.info('used = {used}s'.format(used=(end_time - start_time)))
    log.info('')


def main():
    config = 'online_gsxt_parse_to_mq.conf'
    if len(sys.argv) > 1:
        config = sys.argv[1]
    # conf_parse = ConfigParser('../config/cmb_gsxt_detail.conf')
    conf_parse = ConfigParser('../config/' + config)
    config_list = conf_parse.get_all_session()

    total_source(config_list)


if __name__ == '__main__':
    main()
