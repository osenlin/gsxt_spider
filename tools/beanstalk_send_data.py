#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: beanstalk_send_data.py
@time: 2017/2/23 22:11
"""
import json

from common.pybeanstalk import PyBeanstalk

beanstalk_consumer_conf = {'host': 'cs0.sz-internal.haizhi.com', 'port': 11400, 'tube': 'gs_guizhou_scheduler'}


def main():
    beanstalk = PyBeanstalk(beanstalk_consumer_conf['host'], beanstalk_consumer_conf['port'])
    tube = beanstalk_consumer_conf['tube']

    company_list = ['贵州大龙帝国网吧',
                    '罗甸县网络帝国网咖',
                    '玉屏国网线下百货店',
                    '帝国网络会所',
                    '玉屏县帝国网吧']

    for company_name in company_list:
        data = {
            'company_name': company_name,
            'province': 'guizhou',
        }
        data_str = json.dumps(data)
        print data_str
        beanstalk.put(tube, data_str)


if __name__ == '__main__':
    main()
