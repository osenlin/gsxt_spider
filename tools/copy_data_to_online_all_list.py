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
from logger import Gsxtlogger

log = Gsxtlogger(util.get_pid_log_name('copy_data_to_online_all_list')).get_logger()

count = 0

mongo_db_crawl_data = {
    'host': '172.16.215.2',
    'port': 40042,
    'db': 'crawl_data',
    'username': 'work',
    'password': 'haizhi'
}

mongo_db_gov = {
    'host': '172.16.215.16',
    'port': 40042,
    'db': 'app_data_test',
    'username': 'work',
    'password': 'haizhi'
}

crawl_data_db = MongDb(mongo_db_crawl_data['host'], mongo_db_crawl_data['port'], mongo_db_crawl_data['db'],
                       mongo_db_crawl_data['username'], mongo_db_crawl_data['password'], log=log)

gov_db = MongDb(mongo_db_gov['host'], mongo_db_gov['port'], mongo_db_gov['db'],
                mongo_db_gov['username'], mongo_db_gov['password'], log=log)


# while True:
#     data = {'province': 'yunnan', 'company_name': '国网'}
#     producer.produce(json.dumps(data))
#     log.info(count)
#     # time.sleep()
#     count += 1
#     if count >= 10:
#         break
# time.sleep(30)

def main():
    log.info('开始读取数据发送到kafka..')
    crawl_data_table = 'online_crawl_ningxia_new'
    gov_table = 'enterprise_data_gov'
    source_table_curse = crawl_data_db.db[crawl_data_table].find({'crawl_online': 10}, ['_id'],
                                                                 no_cursor_timeout=True).batch_size(
        500)
    for item in source_table_curse:
        company_name = item.get('_id', None)
        if company_name is None:
            continue

        try:
            if gov_db.find_one(gov_table, {'company': company_name}) is None:
                log.info('数据不存在...company = {company}'.format(company=company_name))
                continue
        except Exception as e:
            log.exception(e)
            continue

    source_table_curse.close()

    log.info('数据发送完毕, 退出程序')


if __name__ == '__main__':
    main()
