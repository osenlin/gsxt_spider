#!/usr/bin/env python
# -*- coding:utf-8 -*-
from task.search.cracker.gsxt_shanghai_worker import GsxtShangHaiWorker


class GsxtSearchListShangHaiWorker(GsxtShangHaiWorker):
    def __init__(self, **kwargs):
        GsxtShangHaiWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
