#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: parse_base_worker.py
@time: 2017/2/3 17:32
"""
from parse_guangxi_worker import GsxtParseGuangXiWorker


# todo 新疆网页在改版中
class GsxtParseXinJiangWorker(GsxtParseGuangXiWorker):
    def __init__(self, **kwargs):
        GsxtParseGuangXiWorker.__init__(self, **kwargs)
