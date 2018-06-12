#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: check_anhui_worker.py
@time: 2017/3/3 10:18
"""
from check.check_tianjin_worker import CheckTianJinWorker


class CheckGsxtWorker(CheckTianJinWorker):
    def __init__(self, province, log):
        CheckTianJinWorker.__init__(self, province, log)
