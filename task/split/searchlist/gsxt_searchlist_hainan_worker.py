#!/usr/bin/env python
# -*- coding:utf-8 -*-
from task.search.cracker.gsxt_hainan_worker import GsxtHaiNanWorker


class GsxtSearchListHaiNanWorker(GsxtHaiNanWorker):
    def __init__(self, **kwargs):
        GsxtHaiNanWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
