#!/usr/bin/env python
# -*- coding:utf-8 -*-

from task.search.cracker.gsxt_shanxicu_worker import GsxtShanXiCuWorker


class GsxtSearchListShanXiCuWorker(GsxtShanXiCuWorker):
    def __init__(self, **kwargs):
        GsxtShanXiCuWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
