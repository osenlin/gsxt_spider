#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: clean_online_all_search.py
@time: 2017/2/26 15:40
"""
import sys

sys.path.append('../')
from logger import Gsxtlogger

log = Gsxtlogger('clean_online_all_search.log').get_logger()
from common.global_resource import source_db

'''
data = {
            # 以搜索列表名与省份信息作为唯一主键
            '_id': util.generator_id({'priority': priority}, search_name, self.province),
            'company_name': company,
            'search_name': search_name,
            'province': self.province,
            'in_time': util.get_now_time(),
            'param': param,
            'rank': rank,
            self.crawl_flag: 0,
            'priority': priority,
        }
'''


def main():
    source_table = 'online_all_search'
    count = 0
    clean_count = 0
    for item in source_db.traverse_batch(source_table):
        count += 1
        priority = item.get('priority', None)
        search_name = item.get('search_name', None)
        _id = item.get('_id', None)
        log.info('当前运行位置: count = {count}'.format(count=count))
        if _id is None:
            log.error('没有_id信息: item = {item}'.format(item=item))
            continue

        # 清理掉没有这些关键信息的链接
        if search_name is None or priority is None or search_name is None:
            source_db.db[source_table].remove(_id)
            clean_count += 1
            log.info('清理字段不全链接信息: _id = {_id}'.format(_id=_id))
            continue

        param = item.get('param', None)
        if param is None:
            source_db.db[source_table].remove(_id)
            clean_count += 1
            log.info('清理没有param字段信息: search_name = {search_name}'.format(search_name=search_name))
            continue

        if 'search_name' not in param:
            source_db.db[source_table].remove(_id)
            clean_count += 1
            log.info('清理param中没有search_name字段信息: search_name = {search_name}'.format(search_name=search_name))
            continue

        if '注销' in search_name or '吊销' in search_name:
            source_db.db[source_table].remove(_id)
            clean_count += 1
            log.info('清理search_name中包含注销吊销字段信息: search_name = {search_name}'.format(search_name=search_name))
            continue

    log.info('清理数目为: clean_count = {count}'.format(count=clean_count))
    log.info('完成清理, 退出程序!')


if __name__ == '__main__':
    main()
