# -*- coding: utf-8 -*-

from task.search.cracker.gsxt_guangxi_worker import GsxtGuangXiWorker

'''
1. 搜索没有结果判断功能添加
2. 包含出资信息 包含详情页
3. 包含年报信息
4. 添加统计信息
5. 完成topo信息添加  已经check
6. 变更信息有详情页
6. 完成列表页名称提取
'''


class GsxtBeiJingWorker(GsxtGuangXiWorker):
    def __init__(self, **kwargs):
        GsxtGuangXiWorker.__init__(self, **kwargs)
