#!/usr/bin/env python
# -*- coding:utf-8 -*-

from task.search.cracker.gsxt_anhui_worker import GsxtAnHuiWorker


class GsxtSearchListAnHuiWorker(GsxtAnHuiWorker):
    def __init__(self, **kwargs):
        GsxtAnHuiWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
