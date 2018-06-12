#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: parse_base_worker.py
@time: 2017/2/3 17:32
"""
from parse_anhui_worker import GsxtParseAnHuiWorker


class GsxtParseHaiNanWorker(GsxtParseAnHuiWorker):
    def __init__(self, **kwargs):
        GsxtParseAnHuiWorker.__init__(self, **kwargs)
