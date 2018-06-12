#!/usr/bin/env python
# -*- coding:utf-8 -*-
from task.search.cracker.gsxt_jilin_worker import GsxtJiLinWorker


class GsxtSearchListJiLinWorker(GsxtJiLinWorker):
    def __init__(self, **kwargs):
        GsxtJiLinWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []