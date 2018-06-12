#!/usr/bin/env python
# -*- coding:utf-8 -*-
from task.search.exploit.gsxt_liaoning_worker import GsxtLiaoNingWorker


class GsxtSearchListLiaoNingWorker(GsxtLiaoNingWorker):
    def __init__(self, **kwargs):
        GsxtLiaoNingWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
