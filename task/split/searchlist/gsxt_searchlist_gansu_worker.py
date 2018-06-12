#!/usr/bin/env python
# -*- coding:utf-8 -*-
from task.search.cracker.gsxt_gansu_worker import GsxtGanSuWorker


class GsxtSearchListGanSuWorker(GsxtGanSuWorker):
    def __init__(self, **kwargs):
        GsxtGanSuWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
