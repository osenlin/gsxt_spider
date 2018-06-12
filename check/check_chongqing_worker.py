#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: check_chongqing_worker.py
@time: 2017/3/3 10:18
"""
import json

from base.check_base_worker import CheckBaseWorker


class CheckChongQingWorker(CheckBaseWorker):
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
        final_flag = self.json_data_flag(base_info)
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
        list_flag = self.json_data_flag(contributive_info)
        detail_flag = self.json_data_flag(contributive_info, 'detail')
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
        final_flag = self.json_data_flag(change_info)
        return final_flag

        # 主要人员

    def get_key_person_info(self, key_person_info):
        """
        :param key_person_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        主要人员一般存储在list列表中, 因为主要人员不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        """
        final_flag = self.json_data_flag(key_person_info)
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
        final_flag = self.json_data_flag(branch_info)
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

        final_flag = self.json_data_flag(shareholder_info)
        return final_flag

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

    def json_check(self, data):
        """对json的数据进行check"""
        if data.get('status', 'fail') != 'success':
            return self.INCOMPLETE
        else:
            try:
                json.loads(data.get('text'))
            except ValueError or TypeError:
                return self.DIRTY
            else:
                return self.CORRECT

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

    def json_data_flag(self, info_model, part='list'):
        """工商数据为json的通用方法入口"""
        flag_list = []
        info_mode_list = info_model.get(part, [])
        for data in info_mode_list:
            flag = self.json_check(data)
            flag_list.append(flag)

        final_flag = self._get_flag(flag_list)
        return final_flag
