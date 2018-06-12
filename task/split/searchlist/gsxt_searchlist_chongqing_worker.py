#!/usr/bin/env python
# -*- coding:utf-8 -*-
from task.search.cracker.gsxt_chongqing_worker import GsxtChongQingWorker


class GsxtSearchListChongQingWorker(GsxtChongQingWorker):
    def __init__(self, **kwargs):
        GsxtChongQingWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
