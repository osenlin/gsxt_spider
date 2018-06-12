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


# pri_pid = item.get('pripid', None)
# pri_type = item.get('pritype', None)

def main():
    source_table = 'online_all_search'
    count = 0
    clean_count = 0
    for item in source_db.traverse_batch(source_table, {'province': 'chongqing'}):
        count += 1
        _id = item.get('_id', None)
        log.info('当前运行位置: count = {count}'.format(count=count))
        if _id is None:
            log.error('没有_id信息: item = {item}'.format(item=item))
            continue

        param = item.get('param', None)
        if param is None:
            source_db.db[source_table].remove(_id)
            clean_count += 1
            log.info('清理没有param字段信息: _id = {_id}'.format(_id=_id))
            continue

        if 'pripid' not in param or 'pritype' not in param:
            source_db.db[source_table].remove(_id)
            clean_count += 1
            log.info('清理没有pripid or pritype字段信息: _id = {_id}'.format(_id=_id))
            continue

    log.info('清理数目: count = {count}'.format(count=clean_count))
    log.info('完成清理, 退出程序!')


if __name__ == '__main__':
    main()
