#!/usr/bin/env python
# -*- coding:utf-8 -*-

from task.split.detail.gsxt_detail_anhui_worker import GsxtDetailAnHuiWorker


class GsxtDetailShanXiCuWorker(GsxtDetailAnHuiWorker):
    def __init__(self, **kwargs):
        GsxtDetailAnHuiWorker.__init__(self, **kwargs)
