#!/usr/bin/env python
# -*- coding:utf-8 -*-
from task.search.cracker.gsxt_fujian_worker import GsxtFuJianWorker


class GsxtSearchListFuJianWorker(GsxtFuJianWorker):
    def __init__(self, **kwargs):
        GsxtFuJianWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
