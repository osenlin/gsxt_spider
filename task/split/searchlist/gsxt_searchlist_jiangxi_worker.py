#!/usr/bin/env python
# -*- coding:utf-8 -*-

from task.search.cracker.gsxt_jiangxi_worker import GsxtJiangXiWorker


class GsxtSearchListJiangXiWorker(GsxtJiangXiWorker):
    def __init__(self, **kwargs):
        GsxtJiangXiWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
