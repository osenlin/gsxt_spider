#!/usr/bin/env python
# -*- coding:utf-8 -*-

from task.search.cracker.gsxt_anhui_worker import GsxtAnHuiWorker

'''
1. 搜索没有结果判断功能添加
2. 包含出资信息 有出资详情
3. 包含年报信息
4. 添加完统计信息
5. 添加完成拓扑信息 已经check
6. 完成列表页名称提取
'''


class GsxtQingHaiWorker(GsxtAnHuiWorker):
    def __init__(self, **kwargs):
        GsxtAnHuiWorker.__init__(self, **kwargs)

    # @staticmethod
    # def find_selector(selector, _id):
    #     if selector is None or selector == '':
    #         return None
    #
    #     title = selector.find('.gggscpnametitle')
    #
    #     rm_span = title.find('span[class!=ecps_solr_highlight]').remove()
    #     company = title.text()
    #     if company is None:
    #         return None
    #
    #     search_name = company.replace(' ', '')
    #     if search_name == '':
    #         return None
    #
    #     status = rm_span.eq(0).text()
    #     description = rm_span.eq(1).text()
    #
    #     seed_code = None
    #     name_text = selector.find('.gggscpnametext').find('.tongyi').text()
    #     if name_text is not None:
    #         part = name_text.split('：')
    #         if len(part) >= 2:
    #             seed_code = part[1]
    #
    #     param = {'id': _id, 'search_name': search_name}
    #     if status is not None and status != '':
    #         param['status'] = status
    #     if description is not None and description != '':
    #         param['description'] = description
    #
    #     # 统一社会信用号
    #     if seed_code is not None and seed_code.strip() != '':
    #         param['unified_social_credit_code'] = seed_code
    #
    #     return param
