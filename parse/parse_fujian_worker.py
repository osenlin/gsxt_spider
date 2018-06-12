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


class GsxtParseFuJianWorker(GsxtParseHuNanWorker):
    def __init__(self, **kwargs):
        GsxtParseHuNanWorker.__init__(self, **kwargs)
        # 必须反馈抓取情况到种子列表
        self.report_status = self.REPORT_SEED
