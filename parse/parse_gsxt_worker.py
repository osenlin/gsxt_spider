#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: parse_base_worker.py
@time: 2017/2/3 17:32
"""
from parse.parse_tianjin_worker import GsxtParseTianJinWorker


class GsxtParseGsxtWorker(GsxtParseTianJinWorker):
    def __init__(self, **kwargs):
        GsxtParseTianJinWorker.__init__(self, **kwargs)
        self.min_gs_field_num = 14
        # 必须反馈抓取情况到种子列表
        self.report_status = self.REPORT_SEED
