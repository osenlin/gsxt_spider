#!/usr/bin/env python
# -*- coding:utf-8 -*-

from task.split.detail.gsxt_detail_tianjin_worker import GsxtDetailTianJinWorker


class GsxtDetailGsxtWorker(GsxtDetailTianJinWorker):
    def __init__(self, **kwargs):
        GsxtDetailTianJinWorker.__init__(self, **kwargs)
