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

log = Gsxtlogger('copy_data_to_offline_all_list.log').get_logger()

mongo_db_company_data = {
    'host': '172.16.215.2',
    'port': 40042,
    'db': 'company_data',
    'username': 'work',
    'password': 'haizhi'
}

mongo_db_schedule_data = {
    'host': '172.16.215.2',
    'port': 40042,
    'db': 'schedule_data',
    'username': 'work',
    'password': 'haizhi'
}

target_db = MongDb(mongo_db_company_data['host'], mongo_db_company_data['port'], mongo_db_company_data['db'],
                   mongo_db_company_data['username'], mongo_db_company_data['password'], log=log)

source_db = MongDb(mongo_db_schedule_data['host'], mongo_db_schedule_data['port'], mongo_db_schedule_data['db'],
                   mongo_db_schedule_data['username'], mongo_db_schedule_data['password'], log=log)

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

province_code = {
    '11': 'beijing',
    '37': 'shandong',
    '43': 'hunan',
    '45': 'guangxi',
    '63': 'qinghai',
    '32': 'jiangsu',
    '64': 'ningxia',
    '51': 'sichuan',
    '21': 'liaoning',
    '44': 'guangdong',
    '22': 'jilin',
    '61': 'shanxi',
    '34': 'anhui',
    '12': 'tianjin',
    '31': 'shanghai',
    '54': 'xizang',
    '62': 'gansu',
    '13': 'hebei',
    '46': 'hainan',
    '14': 'shanxicu',
    '15': 'neimenggu',
    '53': 'yunnan',
    '42': 'hubei',
    '33': 'zhejiang',
    '23': 'heilongjiang',
    '50': 'chongqing',
    '65': 'xinjiang',
    '41': 'henan',
    '52': 'guizhou',
    '35': 'fujian',
    '36': 'jiangxi',
}


# if len(company) == 15 and company[0:2] in province_code:
#                 province = province_code[company[0:2]]
#                 return province
# if len(company) == 18 and company[2:4] in province_code:
#                 province = province_code[company[2:4]]
#                 return province

def get_province_by_code(registered_code, unified_social_credit_code):
    code = None

    if registered_code is not None:
        code = registered_code

    if unified_social_credit_code is not None:
        code = unified_social_credit_code

    if code is None:
        return None

    if len(code) == 15:
        if code[0:2] in province_code:
            province = province_code[code[0:2]]
            return province
    if len(code) == 18:
        if code[2:4] in province_code:
            province = province_code[code[2:4]]
            return province

    return None


# 计算省份信息
def get_province(province, registered_code, unified_social_credit_code):
    if province is None and registered_code is None and unified_social_credit_code is None:
        return None

    if province is None:
        return get_province_by_code(registered_code, unified_social_credit_code)

    if province == u'总局':
        temp_province = get_province_by_code(registered_code, unified_social_credit_code)
        if temp_province is not None:
            return temp_province
        return province_zh_to_py[province]

    if province in province_zh_to_py:
        return province_zh_to_py[province]

    return None


def main():
    log.info('开始读取数据...')
    source_table = 'zhuxiao_diaoxiao_company'
    target_table = 'offline_all_list'
    source_table_curse = source_db.db[source_table].find({}, ['_id', 'province', 'registered_code',
                                                              'unified_social_credit_code'],
                                                         no_cursor_timeout=True).batch_size(10000)
    cnt = 0
    insert_list = []
    count = 0
    real_insert_cnt = 0
    for item in source_table_curse:
        count += 1
        company_name = item.get('_id')
        if company_name is None:
            continue

        province = item.get('province')
        # if province is not None:
        #     log.info('province = {province}'.format(province=province))

        registered_code = item.get('registered_code')
        # if registered_code is not None:
        #     log.info('registered_code = {registered_code}'.format(registered_code=registered_code))

        unified_social_credit_code = item.get('unified_social_credit_code')
        # if unified_social_credit_code is not None:
        #     log.info('unified_social_credit_code = {unified_social_credit_code}'.format(
        #         unified_social_credit_code=unified_social_credit_code))

        province = get_province(province, registered_code, unified_social_credit_code)
        if province is None:
            log.error('计算省份信息失败: company = {company}'.format(company=company_name))
            continue

        data = {
            '_id': util.generator_id({}, company_name, province),
            'company_name': company_name,
            'province': province,
            'in_time': util.get_now_time(),
        }
        insert_list.append(data)
        cnt += 1
        real_insert_cnt += 1
        if cnt >= 10000:
            target_db.insert_batch_data(target_table, insert_list, insert=True)
            cnt = 0
            del insert_list[:]
            log.info('insert 10000')

        log.info('当前进度: count = {count} company = {company}'.format(
            count=count, company=company_name))

    if len(insert_list) > 0:
        target_db.insert_batch_data(target_table, insert_list, insert=True)
        log.info('insert last data')

    source_table_curse.close()

    log.info('总共插入数据为: {cnt}'.format(cnt=real_insert_cnt))
    log.info('数据发送完毕, 退出程序')


if __name__ == '__main__':
    main()
