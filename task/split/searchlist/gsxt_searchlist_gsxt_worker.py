#!/usr/bin/env python
# -*- coding:utf-8 -*-
from task.split.searchlist.gsxt_searchlist_tianjin_worker import GsxtSearchListTianJinWorker


class GsxtSearchListGsxtWorker(GsxtSearchListTianJinWorker):
    def __init__(self, **kwargs):
        GsxtSearchListTianJinWorker.__init__(self, **kwargs)
