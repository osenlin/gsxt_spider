#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: check_gansu_worker.py
@time: 2017/3/3 10:18
"""
from base.check_base_worker import CheckBaseWorker


class CheckGanSuWorker(CheckBaseWorker):
    def __init__(self, province, log):
        CheckBaseWorker.__init__(self, province, log)

    # 基本信息
    def get_base_info(self, base_info):
        """
        :param base_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        基本信息一般存储在list列表中, 因为基本信息不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        """
        keyword_list = [u'营业执照信息', u'股东及出资信息', u'变更事项']
        final_flag = self.html_data_flag(base_info, keyword_list)
        return final_flag

    # 出资信息
    def get_contributive_info(self, contributive_info):
        """
        :param contributive_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        出资信息一般会由两个列表分别进行存储, 但部分省份也可能只包含list列表, 没有详情页信息
        :return: 返回工商schema字典
        """
        list_keyword_list = [u'股东及出资信息', u'股东名称']
        list_flag = self.html_data_flag(contributive_info, list_keyword_list)
        detail_keyword_list = [u'认缴出资额', u'实缴出资额']
        detail_flag = self.html_data_flag(contributive_info, detail_keyword_list, 'detail')
        final_flag = self._get_flag([list_flag, detail_flag])
        return final_flag

    # 变更信息
    def get_change_info(self, change_info):
        """
        :param change_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        变更信息一般只包含list列表, 但是特殊情况下也会有detail详情页列表 比如 北京这个省份有发现过包含详情页的变更信息
        :return: 返回工商schema字典
        """
        return self.CORRECT

    # 主要人员
    def get_key_person_info(self, key_person_info):
        """
        :param key_person_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        主要人员一般存储在list列表中, 因为主要人员不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        """
        keyword_list = [u'主要人员信息', u'class="trade_content">', u'class="clear">']
        final_flag = self.html_data_flag(key_person_info, keyword_list)
        return final_flag

    # 分支机构
    def get_branch_info(self, branch_info):
        """
        :param branch_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        分支机构一般存储在list列表中, 因为分支机构不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        """
        keyword_list = [u'分支机构信息', u'class="trade_content">', u'class="clear">']
        final_flag = self.html_data_flag(branch_info, keyword_list)
        return final_flag

    # 股东信息
    def get_shareholder_info(self, shareholder_info):
        """
        :param shareholder_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        股东信息一般存储在list列表中, 因为股东信息不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        """
        return self.CORRECT

    # 清算信息
    def get_liquidation_info(self, liquidation_info):
        """
        :param liquidation_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        清算信息一般存储在list列表中, 因为清算信息不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        """
        return self.CORRECT

    # 年报信息
    def get_annual_info(self, annual_info):
        """
        :param annual_info:  网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        :return: 返回工商schema字典
        """
        return self.CORRECT

    def html_check(self, data, keyword_list):
        """对html的数据进行check"""
        if data.get('status') != 'success':
            return self.INCOMPLETE

        text = data.get('text')
        for key in keyword_list:
            if key in text:
                return self.CORRECT
        return self.DIRTY

    def _get_flag(self, flag_list):
        """
        flag_list: 所有情况flag的list
        Returns:按优先级别返回flag
        """
        if self.DIRTY in flag_list:
            return self.DIRTY
        elif self.INCOMPLETE in flag_list:
            return self.INCOMPLETE
        return self.CORRECT

    def html_data_flag(self, info_model, keyword_list, part='list'):
        """工商数据为html的通用方法入口"""
        flag_list = []
        info_mode_list = info_model.get(part, [])
        for data in info_mode_list:
            flag = self.html_check(data, keyword_list)
            flag_list.append(flag)

        final_flag = self._get_flag(flag_list)
        return final_flag
