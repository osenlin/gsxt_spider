# -*- coding: utf-8 -*-

from task.search.cracker.gsxt_hunan_worker import GsxtHuNanWorker

'''
1. 搜索没有结果判断功能添加
2. 包含出资信息 出资信息在基本信息里面
3. 包含年报信息
4. 添加统计信息
5. 添加完成拓扑信息 已经check
6. 完成列表页名称提取
'''


class GsxtShangHaiWorker(GsxtHuNanWorker):
    def __init__(self, **kwargs):
        GsxtHuNanWorker.__init__(self, **kwargs)
        self.url = 'http://{host}/notice/home'.format(host=self.host)
        self.pattern = 'window\.open\(\'http://sh\.gsxt\.gov\.cn/notice/notice/view\?uuid=(.*?)&tab=01\'\)'
        self.proxy_type = self.PROXY_TYPE_DYNAMIC
