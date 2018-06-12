#!/usr/bin/env python
# -*- coding:utf-8 -*-

from task.search.cracker.gsxt_guangxi_worker import GsxtGuangXiWorker


class GsxtSearchListGuangXiWorker(GsxtGuangXiWorker):
    def __init__(self, **kwargs):
        GsxtGuangXiWorker.__init__(self, **kwargs)
        self.proxy_type = self.PROXY_TYPE_DYNAMIC

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
