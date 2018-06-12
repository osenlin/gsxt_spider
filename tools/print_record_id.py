#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: print_record_id.py
@time: 2017/2/23 11:40
"""
import sys

from common import tools

sys.path.append('../')

from common.mongo import MongDb
from logger import Gsxtlogger

log = Gsxtlogger('print_record_id', for_mat='').get_logger()

mongo_db_crawl_data = {
    'host': '172.16.215.2',
    'port': 40042,
    'db': 'crawl_data',
    'username': 'work',
    'password': 'haizhi'
}

crawl_data_db = MongDb(mongo_db_crawl_data['host'], mongo_db_crawl_data['port'], mongo_db_crawl_data['db'],
                       mongo_db_crawl_data['username'], mongo_db_crawl_data['password'], log=log)


def main():
    crawl_data_table = 'online_crawl_ningxia_new'
    source_table_curse = crawl_data_db.db[crawl_data_table].find({'crawl_online': 10},
                                                                 no_cursor_timeout=True).batch_size(
        500)
    for item in source_table_curse:
        company_name = item.get('_id', None)
        if company_name is None:
            continue

        replace_company = company_name.replace('(', '（').replace(')', '）')

        record = '|' + replace_company

        _record_id = tools.get_md5(record)
        log.info('{_record_id} {company}'.format(_record_id=_record_id, company=company_name))

    source_table_curse.close()


if __name__ == '__main__':
    main()
