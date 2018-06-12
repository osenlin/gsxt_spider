#!/usr/bin/env python
# -*- coding:utf-8 -*-
from task.search.cracker.gsxt_hunan_worker import GsxtHuNanWorker


class GsxtSearchListHuNanWorker(GsxtHuNanWorker):
    def __init__(self, **kwargs):
        GsxtHuNanWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
