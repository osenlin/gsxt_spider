#!/usr/bin/env python
# -*- coding:utf-8 -*-

from task.search.cracker.gsxt_henan_worker import GsxtHeNanWorker


class GsxtSearchListHeNanWorker(GsxtHeNanWorker):
    def __init__(self, **kwargs):
        GsxtHeNanWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
