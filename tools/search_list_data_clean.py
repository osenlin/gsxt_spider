#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: kafka_producer.py
@time: 2016/12/19 16:32
"""
import sys

sys.path.append('../')
from common import util

from common.mongo import MongDb
from config.conf import mongo_db_source
from logger import Gsxtlogger

log = Gsxtlogger('search_list_data_clean.log').get_logger()

count = 0

source_db = MongDb(mongo_db_source['host'], mongo_db_source['port'], mongo_db_source['db'],
                   mongo_db_source['username'], mongo_db_source['password'], log=log)


def main():
    count = 0
    log.info('开始清洗数据')
    source_table = 'cs2_online_all_search'

    # 获得传入的表信息
    if len(sys.argv) > 1:
        source_table = sys.argv[1]

    source_table_cursor = source_db.db[source_table].find({'priority': {'$ne': 0}}, no_cursor_timeout=True).batch_size(
        10000)

    for item in source_table_cursor:
        try:
            count += 1
            search_name = item.get('search_name', None)
            company_name = item.get('company_name', None)
            if search_name is None or company_name is None:
                log.error('读取数据出错: item = {item}'.format(item=item))
                continue

            if 'priority' not in item:
                log.error('没有priority字段: search_name = {name}'.format(name=search_name))
                continue

            replace_name_1 = company_name.replace('(', '（').replace(')', '）')
            replace_name_2 = company_name.replace('（', '(').replace('）', ')')

            if search_name == replace_name_1 \
                    or search_name == replace_name_2 \
                    or search_name == company_name:
                item['priority'] = 0
                item['in_time'] = util.get_now_time()
                source_db.insert_batch_data(source_table, [item])
                # source_db.save(source_table, item)
                log.info('更新数据: search_name = {search_name} company_name = {company_name}'.format(
                    search_name=search_name, company_name=company_name))
            log.info('当前进度: count = {count}'.format(count=count))
        except Exception as e:
            log.exception(e)

    source_table_cursor.close()

    log.info('清洗数据完成, 退出程序')


if __name__ == '__main__':
    main()
