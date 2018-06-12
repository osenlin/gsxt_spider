#!/usr/bin/env python
# -*- coding:utf-8 -*-

from task.search.cracker.gsxt_anhui_worker import GsxtAnHuiWorker

'''
1. 搜索没有结果判断功能添加
2. 包含出资信息 有出资详情
3. 包含年报信息
4. 添加完统计信息
5. 添加完成拓扑信息  已经check
6. 完成列表页名称提取
'''


class GsxtHaiNanWorker(GsxtAnHuiWorker):
    def __init__(self, **kwargs):
        GsxtAnHuiWorker.__init__(self, **kwargs)
