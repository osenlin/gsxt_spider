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

beanstalk_consumer_conf = {'host': 'cs0.sz-internal.haizhi.com', 'port': 11400, 'tube': 'online_gsxt_parse'}


def main():
    beanstalk = PyBeanstalk(beanstalk_consumer_conf['host'], beanstalk_consumer_conf['port'])
    tube = beanstalk_consumer_conf['tube']

    data_str = '湖南汉璟真空玻璃科技有限公司'
    data = {
        'company': data_str,
        'province': 'hunan',
    }
    print data_str
    beanstalk.put(tube, json.dumps(data))


if __name__ == '__main__':
    main()
