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

log = Gsxtlogger('update_data_to_offline_all_list.log').get_logger()

mongo_db_company_data = {
    'host': '172.16.215.2',
    'port': 40042,
    'db': 'company_data',
    'username': 'work',
    'password': 'haizhi'
}

source_db = MongDb(mongo_db_company_data['host'], mongo_db_company_data['port'], mongo_db_company_data['db'],
                   mongo_db_company_data['username'], mongo_db_company_data['password'], log=log)


#
def main():
    count = 0
    log.info('开始读取数据...')
    source_table = 'offline_all_list'
    with open("guangdong.txt") as p_file:
        result_list = list()
        province = "guangdong"
        for line in p_file:
            company_name = line.strip().strip("\r").strip("\n")

            count += 1

            data = {
                '_id': util.generator_id({}, company_name, province),
                'company_name': company_name,
                'province': province,
                'in_time': util.get_now_time(),
                'crawl_online': 0,
            }

            result_list.append(data)
            if len(result_list) >= 1000:
                source_db.insert_batch_data(source_table, result_list)
                del result_list[:]

        source_db.insert_batch_data(source_table, result_list)

    log.info("总共发送数据: {}".format(count))
    log.info('数据发送完毕, 退出程序')


if __name__ == '__main__':
    main()
