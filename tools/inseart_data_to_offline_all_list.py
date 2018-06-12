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

log = Gsxtlogger('inseart_data_to_offline_all_list.py.log').get_logger()

mongo_db_company_data = {
    'host': '172.16.215.2',
    'port': 40042,
    'db': 'company_data',
    'username': 'work',
    'password': 'haizhi'
}

source_db = MongDb(mongo_db_company_data['host'], mongo_db_company_data['port'], mongo_db_company_data['db'],
                   mongo_db_company_data['username'], mongo_db_company_data['password'], log=log)

app_data_config = {
    'host': '172.16.215.16',
    'port': 40042,
    'db': 'app_data',
    'username': 'work',
    'password': 'haizhi'
}

app_data_db = MongDb(app_data_config['host'], app_data_config['port'], app_data_config['db'],
                     app_data_config['username'], app_data_config['password'], log=log)

province_zh_to_py = {
    u'上海': 'shanghai',
    u'云南': 'yunnan',
    u'内蒙古': 'neimenggu',
    u'北京': 'beijing',
    u'吉林': 'jilin',
    u'四川': 'sichuan',
    u'天津': 'tianjin',
    u'宁夏': 'ningxia',
    u'安徽': 'anhui',
    u'山东': 'shandong',
    u'山西': 'shanxicu',
    u'广东': 'guangdong',
    u'广西': 'guangxi',
    u'新疆': 'xinjiang',
    u'江苏': 'jiangsu',
    u'江西': 'jiangxi',
    u'河北': 'hebei',
    u'河南': 'henan',
    u'浙江': 'zhejiang',
    u'海南': 'hainan',
    u'湖北': 'hubei',
    u'湖南': 'hunan',
    u'甘肃': 'gansu',
    u'福建': 'fujian',
    u'西藏': 'xizang',
    u'贵州': 'guizhou',
    u'辽宁': 'liaoning',
    u'重庆': 'chongqing',
    u'陕西': 'shanxi',
    u'青海': 'qinghai',
    u'黑龙江': 'heilongjiang',
    u'总局': 'gsxt',
}


#
def main():
    log.info('开始读取数据...')
    source_table = 'offline_all_list'
    app_data_table = 'enterprise_data_gov'
    with open("company_expection_list") as p_file:
        result_list = list()
        for line in p_file:
            company_name = line.strip().strip("\r").strip("\n")

            province = 'gsxt'

            item = app_data_db.find_one(app_data_table, {'company': company_name})
            if item is not None and 'province' in item and item['province'] in province_zh_to_py:
                province = province_zh_to_py[item['province']]
            else:
                log.error("省份查找失败: {}".format(company_name))

            data = {
                '_id': util.generator_id({}, company_name, province),
                'company_name': company_name,
                'province': province,
                'in_time': util.get_now_time(),
            }

            result_list.append(data)
            if len(result_list) >= 1000:
                source_db.insert_batch_data(source_table, result_list)
                del result_list[:]

        source_db.insert_batch_data(source_table, result_list)

    log.info('数据发送完毕, 退出程序')


if __name__ == '__main__':
    main()
