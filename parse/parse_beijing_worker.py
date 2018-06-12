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


# 中信银行股份有限公司 变更信息有特殊case
class GsxtParseBeiJingWorker(GsxtParseGuangXiWorker):
    def __init__(self, **kwargs):
        GsxtParseGuangXiWorker.__init__(self, **kwargs)
