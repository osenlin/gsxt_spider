#!/usr/bin/env python
# -*- coding:utf-8 -*-

from task.search.cracker.gsxt_xinjiang_worker import GsxtXinJiangWorker


class GsxtSearchListXinJiangWorker(GsxtXinJiangWorker):
    def __init__(self, **kwargs):
        GsxtXinJiangWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
