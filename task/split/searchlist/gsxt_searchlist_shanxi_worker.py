#!/usr/bin/env python
# -*- coding:utf-8 -*-

from task.search.cracker.gsxt_shanxi_worker import GsxtShanXiWorker


class GsxtSearchListShanXiWorker(GsxtShanXiWorker):
    def __init__(self, **kwargs):
        GsxtShanXiWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
