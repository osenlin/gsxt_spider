#!/usr/bin/env python
# -*- coding:utf-8 -*-

from task.search.cracker.gsxt_jiangsu_worker import GsxtJiangSuWorker


class GsxtSearchListJiangSuWorker(GsxtJiangSuWorker):
    def __init__(self, **kwargs):
        GsxtJiangSuWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
