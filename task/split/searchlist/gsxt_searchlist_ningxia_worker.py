#!/usr/bin/env python
# -*- coding:utf-8 -*-
from task.search.cracker.gsxt_ningxia_worker import GsxtNingXiaWorker


class GsxtSearchListNingXiaWorker(GsxtNingXiaWorker):
    def __init__(self, **kwargs):
        GsxtNingXiaWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
