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
from tools.crawl_conf import company_info

beanstalk_consumer_conf = {'host': 'cs0.sz-internal.haizhi.com', 'port': 11400, 'tube': 'online_gsxt_parse'}


def main():
    beanstalk = PyBeanstalk(beanstalk_consumer_conf['host'], beanstalk_consumer_conf['port'])
    tube = beanstalk_consumer_conf['tube']

    for company, info in company_info.iteritems():
        data = {
            'company': company,
            'province': info['province'],
        }
        print company
        beanstalk.put(tube, json.dumps(data))


if __name__ == '__main__':
    main()
