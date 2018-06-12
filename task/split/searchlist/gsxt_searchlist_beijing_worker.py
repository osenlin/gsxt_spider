#!/usr/bin/env python
# -*- coding:utf-8 -*-

from task.search.cracker.gsxt_beijing_worker import GsxtBeiJingWorker


class GsxtSearchListBeiJingWorker(GsxtBeiJingWorker):
    def __init__(self, **kwargs):
        GsxtBeiJingWorker.__init__(self, **kwargs)
        # 北京搜索列表页改为动态代理
        self.proxy_type = self.PROXY_TYPE_DYNAMIC

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
