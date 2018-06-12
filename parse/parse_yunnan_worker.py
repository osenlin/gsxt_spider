#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: parse_base_worker.py
@time: 2017/2/3 17:32
"""
from parse.parse_hunan_worker import GsxtParseHuNanWorker


class GsxtParseYunNanWorker(GsxtParseHuNanWorker):
    def __init__(self, **kwargs):
        GsxtParseHuNanWorker.__init__(self, **kwargs)