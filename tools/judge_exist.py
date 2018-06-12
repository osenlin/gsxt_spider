#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: judge_exist.py
@time: 2017/6/13 14:09
"""
from common.mongo import MongDb
from logger import Gsxtlogger

mongo_db_conf = {
    'host': '172.16.215.16',
    'port': 40042,
    'db': 'app_data',
    'username': 'work',
    'password': 'haizhi'
}
table_name = 'enterprise_data_gov'

conf_name = 'company_list.csv'

global_logger = Gsxtlogger('judge_exist.log')
log = global_logger.get_logger()


def main():
    source_db = MongDb(mongo_db_conf['host'], mongo_db_conf['port'], mongo_db_conf['db'],
                       mongo_db_conf['username'],
                       mongo_db_conf['password'], log=log)

    count = 0
    total = 0
    already = 0
    with open(conf_name) as p_file:
        for line in p_file:
            total += 1
            company = line.strip("\n").strip("\r").strip(" ")
            item = source_db.find_one(table_name, {'company': company})
            if item is None:
                log.error("当前企业没有抓到: {company}".format(company=company))
                count += 1
            else:
                log.info("已抓到企业: {}".format(company))
                already += 1
        log.info("总共企业数目为: {}".format(total))
        log.info("当前已抓到的个数: {}".format(already))
        log.info("当前总共没有抓到企业数目为: {}".format(count))

if __name__ == '__main__':
    main()
