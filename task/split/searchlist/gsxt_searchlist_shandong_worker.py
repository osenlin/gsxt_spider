#!/usr/bin/env python
# -*- coding:utf-8 -*-
from task.search.exploit.gsxt_shandong_worker import GsxtShanDongWorker


class GsxtSearchListShanDongWorker(GsxtShanDongWorker):
    def __init__(self, **kwargs):
        GsxtShanDongWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
