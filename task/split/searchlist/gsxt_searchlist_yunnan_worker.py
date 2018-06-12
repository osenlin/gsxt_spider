#!/usr/bin/env python
# -*- coding:utf-8 -*-
from task.search.cracker.gsxt_yunnan_worker import GsxtYunNanWorker


class GsxtSearchListYunNanWorker(GsxtYunNanWorker):
    def __init__(self, **kwargs):
        GsxtYunNanWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
