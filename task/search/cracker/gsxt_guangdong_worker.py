#!/usr/bin/env python
# -*- coding:utf-8 -*-

from task.search.cracker.gsxt_neimenggu_worker import GsxtNeiMengGuWorker
from task.search.cracker.gsxt_shezhen_worker import GsxtShenZhenWorker

'''
1. 搜索没有结果判断功能添加
2. 包含出资信息 有出资详情, 全部在json里面
3. 包含年报信息
4. 添加完成统计信息
5. 完成列表页名称提取
'''


class GsxtGuangDongWorker(GsxtNeiMengGuWorker):
    def __init__(self, **kwargs):
        GsxtNeiMengGuWorker.__init__(self, **kwargs)
        self.shenzhen = GsxtShenZhenWorker(**kwargs)
        self.host_old_v1 = 'www.szcredit.org.cn'
        self.host_old_v2 = 'gsxt.gzaic.gov.cn'
        self.host_old_v3 = 'gsxt.gdgs.gov.cn'
        self.url = 'http://{host}/aiccips/'.format(host=self.host)
        self.sub = '/aiccips'
        self.proxy_type = self.PROXY_TYPE_STATIC
        self.ssl_type = 'http'

    def get_detail_html_list(self, seed, session, param_list):

        data_list = []
        for item in param_list:
            try:
                detail_url = item.get('url', None)
                if detail_url is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                search_name = item.get('search_name', None)
                if search_name is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                data = None
                if detail_url.find(self.host) != -1:
                    data = self.get_detail_html_list_new(self.host, search_name, seed, session, detail_url)
                elif detail_url.find(self.host_old_v1) != -1:
                    data = self.shenzhen.get_detail_html_list_old(seed, search_name, session, detail_url)
                elif detail_url.find(self.host_old_v2) != -1:
                    data = self.get_detail_html_list_new(self.host_old_v2, search_name, seed, session, detail_url)
                elif detail_url.find(self.host_old_v3) != -1:
                    data = self.get_detail_html_list_new(self.host_old_v3, search_name, seed, session, detail_url)
                else:
                    self.log.error('url 无法解析: {url}'.format(url=detail_url))
                if data is not None:
                    data_list.append(data)
            except Exception as e:
                self.log.exception(e)

        return self.sent_to_target(data_list)
