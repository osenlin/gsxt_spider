#!/usr/bin/env python
# -*- coding:utf-8 -*-

'''
1. 验证码破解方式
2. 没有抓年报信息
3. 没有抓出资信息
4. 添加完成统计信息
6. 完成列表页名称提取
'''
from task.search.cracker.gsxt_tianjin_worker import GsxtTianJinWorker


class GsxtGsxtWorker(GsxtTianJinWorker):
    def __init__(self, **kwargs):
        GsxtTianJinWorker.__init__(self, **kwargs)
