#!/usr/bin/env python
# -*- coding:utf-8 -*-
from task.search.cracker.gsxt_neimenggu_worker import GsxtNeiMengGuWorker


class GsxtSearchListNeiMengGuWorker(GsxtNeiMengGuWorker):
    def __init__(self, **kwargs):
        GsxtNeiMengGuWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
