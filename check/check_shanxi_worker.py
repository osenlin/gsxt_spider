#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: check_anhui_worker.py
@time: 2017/3/3 10:18
"""
from base.check_base_worker import CheckBaseWorker


class CheckShanXiWorker(CheckBaseWorker):
    def __init__(self, province, log):
        CheckBaseWorker.__init__(self, province, log)

    # 基本信息
    def get_base_info(self, base_info):
        '''
        :param base_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        基本信息一般存储在list列表中, 因为基本信息不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        '''
        feature_list = [u'名称', u'营业执照信息']
        check_flag = self.check_field(field_info=base_info, feature_list=feature_list, classify='list',
                                      data_type='html')
        return check_flag

    # 股东信息
    def get_shareholder_info(self, shareholder_info):
        '''
        :param shareholder_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        股东信息一般存储在list列表中, 因为股东信息不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        '''
        feature_list = [u'股东及出资信息', u'营业执照信息']
        check_flag = self.check_field(field_info=shareholder_info, feature_list=feature_list, classify='list',
                                      data_type='html')
        return check_flag

    # 变更信息
    def get_change_info(self, change_info):
        '''
        :param change_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        变更信息一般只包含list列表, 但是特殊情况下也会有detail详情页列表 比如 北京这个省份有发现过包含详情页的变更信息
        :return: 返回工商schema字典
        '''
        feature_list = [u'变更信息', u'营业执照信息']
        check_flag = self.check_field(field_info=change_info, feature_list=feature_list, classify='list',
                                      data_type='html')
        return check_flag

    # 主要人员
    def get_key_person_info(self, key_person_info):
        '''
        :param key_person_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        主要人员一般存储在list列表中, 因为主要人员不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        '''
        feature_list = [u'主要人员信息', u'营业执照信息']
        check_flag = self.check_field(field_info=key_person_info, feature_list=feature_list, classify='list',
                                      data_type='html')
        return check_flag

    # 分支机构
    def get_branch_info(self, branch_info):
        '''
        :param branch_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        分支机构一般存储在list列表中, 因为分支机构不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        '''
        feature_list = [u'分支机构信息', u'营业执照信息']
        check_flag = self.check_field(field_info=branch_info, feature_list=feature_list, classify='list',
                                      data_type='html')
        return check_flag

    # 出资信息
    def get_contributive_info(self, contributive_info):
        '''
        :param contributive_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        出资信息一般会由两个列表分别进行存储, 但部分省份也可能只包含list列表, 没有详情页信息
        :return: 返回工商schema字典
        '''
        normal_feature = [u'股东及出资信息', u'营业执照信息']
        detail_feature = [u'股东及出资详细信息']
        check_list_flag = self.check_field(field_info=contributive_info, feature_list=normal_feature, classify='list',
                                           data_type='html')
        check_detail_flag = self.check_field(field_info=contributive_info, feature_list=detail_feature,
                                             classify='detail',
                                             data_type='html')

        if check_list_flag == self.DIRTY or check_detail_flag == self.DIRTY:
            return self.DIRTY
        elif check_list_flag == self.INCOMPLETE or check_detail_flag == self.INCOMPLETE:
            return self.INCOMPLETE
        else:
            return self.CORRECT

    # 清算信息
    def get_liquidation_info(self, liquidation_info):
        '''
        :param liquidation_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        清算信息一般存储在list列表中, 因为清算信息不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        '''
        return self.CORRECT

    # 年报信息
    def get_annual_info(self, annual_info):
        '''
        :param annual_info:  网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        :return: 返回工商schema字典
        '''
        return self.CORRECT
