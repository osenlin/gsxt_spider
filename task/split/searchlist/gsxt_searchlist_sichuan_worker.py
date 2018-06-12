#!/usr/bin/env python
# -*- coding:utf-8 -*-
from task.search.cracker.gsxt_sichuan_worker import GsxtSiChuanWorker


class GsxtSearchListSiChuanWorker(GsxtSiChuanWorker):
    def __init__(self, **kwargs):
        GsxtSiChuanWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
