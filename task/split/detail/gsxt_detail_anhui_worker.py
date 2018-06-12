#!/usr/bin/env python
# -*- coding:utf-8 -*-

from task.search.cracker.gsxt_anhui_worker import GsxtAnHuiWorker

'''
根据ID离线抓取数据
'''


class GsxtDetailAnHuiWorker(GsxtAnHuiWorker):
    def __init__(self, **kwargs):
        GsxtAnHuiWorker.__init__(self, **kwargs)

    def query_company(self, item):
        try:
            session = self.get_new_session()

            seed = item.get('company_name', '')
            # 进一步获取详情
            result_length, detail_list = self.get_detail_html_list(seed, session, [item.get('param', {})])
            if result_length > 0:
                return self.CRAWL_FINISH
        except Exception as e:
            self.log.exception(e)
        return self.CRAWL_UN_FINISH

    def query_task(self, item):

        if not isinstance(item, dict):
            self.log.info('参数错误: item = {item}'.format(item=item))
            return self.province

        search_name = item.get('search_name', None)
        if search_name is None:
            self.log.error('没有search_name 字段, 不进行搜索 item = {item}'.format(item=item))
            return self.province

        # 判断是否需要进行抓取
        if not self.check_detail_crawl_flag(item):
            # self.log.info('当前状态不进行抓取: search_name = {search_name}'.format(search_name=search_name))
            # self.log.info('item = {item}'.format(item=item))
            return self.province

        self.log.info('开始抓取任务...search_name = {search_name}'.format(search_name=search_name))
        status = self.query_company(item)

        # 标记抓取状态
        self.set_detail_crawl_flag(item, status)
        self.log.info('完成抓取任务...search_name = {search_name} status = {status}'.format(
            search_name=search_name, status=status))
        return self.province
