#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: parse_base_worker.py
@time: 2017/2/3 17:32
"""
from base.parse_base_worker import ParseBaseWorker
from common.global_field import Model
from parse_neimenggu_worker import GsxtParseNeiMengGuWorker
from parse_shenzhen_worker import GsxtParseShenZhenWorker


# 专门处理深圳的格式
class GsxtParseGuangDongWorker(ParseBaseWorker):
    SHENZHEN = 'shenzhen'
    NEIMENGGU = 'neimenggu'

    def __init__(self, **kwargs):
        ParseBaseWorker.__init__(self, **kwargs)
        kwargs['logfile'] = 'log_guangdong_branch_neimenggu'
        self.neimenggu = GsxtParseNeiMengGuWorker(**kwargs)
        kwargs['logfile'] = 'log_guangdong_branch_shenzhen'
        self.shenzhen = GsxtParseShenZhenWorker(**kwargs)

    # 默认发给内蒙古新域名解析
    def is_sz_or_nm(self, param):
        if param is None:
            return None
        if isinstance(param, list):
            if len(param) <= 0:
                self.log.error('判断深圳 内蒙古解析器失败..len(param) = 0')
                return None
            url = param[0].get('url', None)
            if url is None:
                self.log.error('判断深圳 内蒙古解析器失败..url = None')
                return None
            if u'szcredit' in url:
                return self.SHENZHEN
            return self.NEIMENGGU
        if isinstance(param, dict):
            detail_list = param.get(Model.type_detail)
            list_list = param.get(Model.type_list)
            if detail_list is None and list_list is None:
                self.log.error('判断深圳 内蒙古解析器失败..detail_list = None list_list = None')
                return None
            if isinstance(detail_list, list):
                if len(detail_list) <= 0:
                    self.log.error('判断深圳 内蒙古解析器失败..len(detail_list) = 0')
                    return None
                url = detail_list[0].get('url', None)
                if url is None:
                    self.log.error('判断深圳 内蒙古解析器失败..url = None')
                    return None
                if u'szcredit' in url:
                    return self.SHENZHEN
                return self.NEIMENGGU

            if isinstance(list_list, list):
                if len(list_list) <= 0:
                    self.log.error('判断深圳 内蒙古解析器失败..len(list_list) = 0')
                    return None
                url = list_list[0].get('url', None)
                if url is None:
                    self.log.error('判断深圳 内蒙古解析器失败..url = None')
                    return None
                if u'szcredit' in url:
                    return self.SHENZHEN
                return self.NEIMENGGU
            self.log.error('detail list 都不是列表结构..')
            return None

        self.log.error('param 类型未知..')
        return None

    # 基本信息
    def get_base_info(self, base_info):
        flag = self.is_sz_or_nm(base_info)
        if flag is None:
            return {}
        if flag == self.NEIMENGGU:
            return self.neimenggu.get_base_info(base_info)
        return self.shenzhen.get_base_info(base_info)

    # 股东信息
    def get_shareholder_info(self, shareholder_info):
        flag = self.is_sz_or_nm(shareholder_info)
        if flag is None:
            return {}
        if flag == self.NEIMENGGU:
            return self.neimenggu.get_shareholder_info(shareholder_info)
        return self.shenzhen.get_shareholder_info(shareholder_info)

    # 变更信息
    def get_change_info(self, change_info):
        flag = self.is_sz_or_nm(change_info)
        if flag is None:
            return {}
        if flag == self.NEIMENGGU:
            return self.neimenggu.get_change_info(change_info)
        return self.shenzhen.get_change_info(change_info)

    # 主要人员
    def get_key_person_info(self, key_person_info):
        flag = self.is_sz_or_nm(key_person_info)
        if flag is None:
            return {}
        if flag == self.NEIMENGGU:
            return self.neimenggu.get_key_person_info(key_person_info)
        return self.shenzhen.get_key_person_info(key_person_info)

    # 分支机构
    def get_branch_info(self, branch_info):
        flag = self.is_sz_or_nm(branch_info)
        if flag is None:
            return {}
        if flag == self.NEIMENGGU:
            return self.neimenggu.get_branch_info(branch_info)
        return self.shenzhen.get_branch_info(branch_info)

    # 出资信息
    def get_contributive_info(self, contributive_info):
        flag = self.is_sz_or_nm(contributive_info)
        if flag is None:
            return {}
        if flag == self.NEIMENGGU:
            return self.neimenggu.get_contributive_info(contributive_info)
        return self.shenzhen.get_contributive_info(contributive_info)

    # 年报信息
    def get_annual_info(self, annual_item_list):
        flag = self.is_sz_or_nm(annual_item_list)
        if flag is None:
            return {}
        if flag == self.NEIMENGGU:
            return self.neimenggu.get_annual_info(annual_item_list)
        return self.shenzhen.get_annual_info(annual_item_list)

    # 动产抵押登记信息
    def get_chattel_mortgage_info(self, chattel_mortgage_info):
        flag = self.is_sz_or_nm(chattel_mortgage_info)
        if flag is None:
            return {}
        if flag == self.NEIMENGGU:
            return self.neimenggu.get_chattel_mortgage_info(chattel_mortgage_info)
        return self.shenzhen.get_chattel_mortgage_info(chattel_mortgage_info)

    # 列入经营异常名录信息
    def get_abnormal_operation_info(self, abnormal_operation_info):
        flag = self.is_sz_or_nm(abnormal_operation_info)
        if flag is None:
            return {}
        if flag == self.NEIMENGGU:
            return self.neimenggu.get_abnormal_operation_info(abnormal_operation_info)
        return self.shenzhen.get_abnormal_operation_info(abnormal_operation_info)

    # 股权出质登记信息 股权出资登记
    def get_equity_pledged_info(self, equity_pledged_info):
        flag = self.is_sz_or_nm(equity_pledged_info)
        if flag is None:
            return {}
        if flag == self.NEIMENGGU:
            return self.neimenggu.get_equity_pledged_info(equity_pledged_info)
        return self.shenzhen.get_equity_pledged_info(equity_pledged_info)

    # 股权变更信息
    def get_change_shareholding_info(self, change_shareholding_info):
        flag = self.is_sz_or_nm(change_shareholding_info)
        if flag is None:
            return {}
        if flag == self.NEIMENGGU:
            return self.neimenggu.get_change_shareholding_info(change_shareholding_info)
        return self.shenzhen.get_change_shareholding_info(change_shareholding_info)