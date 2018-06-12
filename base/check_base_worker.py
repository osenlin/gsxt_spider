#!/usr/bin/env python
# encoding: utf-8
"""
"""
import json

from lxml.etree import XMLSyntaxError
from pyquery import PyQuery

from common.global_field import Model


class CheckBaseWorker(object):
    # 完全正确
    CORRECT = 0
    # 数据不完整, 抓取失败等
    INCOMPLETE = 1
    # 脏数据 抓取到验证码拦截页面
    DIRTY = 2
    # 其他异常
    EXCEPTION = 3

    def __init__(self, province, log):
        self.log = log
        self.province = province

    # 校验数据正确性
    def check_data(self, doc):

        flag_map = {}
        dirty_list = []
        incomplete_list = []

        detail_name = doc.get('_id')
        search_name = doc.get('search_name')
        data_list = doc.get('datalist')
        if detail_name is None:
            self.log.error('详情页名称为None')
            return self.EXCEPTION
        if search_name is None:
            self.log.error('列表页名称为None, _id = {name}'.format(name=detail_name))
            return self.EXCEPTION
        if data_list is None:
            self.log.error('没有datalist数据信息, _id = {name} search_name = {search_name}'.format(
                name=detail_name, search_name=search_name))
            return self.EXCEPTION

        if detail_name != search_name:
            self.log.info('列表页名称与详情页名称不一致: detail_name = {detail_name} search_name = {search_name}'.format(
                detail_name=detail_name, search_name=search_name))

        check_success = self.DIRTY
        while True:

            # 基本信息
            base_info_flag = self.get_base_info(data_list.get(Model.base_info))
            if base_info_flag == self.DIRTY:
                dirty_list.append(Model.base_info)
                break
            flag_map[Model.base_info] = base_info_flag

            # 股东信息
            shareholder_info_flag = self.get_shareholder_info(data_list.get(Model.shareholder_info))
            if shareholder_info_flag == self.DIRTY:
                dirty_list.append(Model.shareholder_info)
                break
            flag_map[Model.shareholder_info] = shareholder_info_flag

            # 变更信息
            change_info_flag = self.get_change_info(data_list.get(Model.change_info))
            if change_info_flag == self.DIRTY:
                dirty_list.append(Model.change_info)
                break
            flag_map[Model.change_info] = change_info_flag

            # 主要人员
            key_person_info_flag = self.get_key_person_info(data_list.get(Model.key_person_info))
            if key_person_info_flag == self.DIRTY:
                dirty_list.append(Model.key_person_info)
                break
            flag_map[Model.key_person_info] = key_person_info_flag

            # 分支结构
            branch_info_flag = self.get_branch_info(data_list.get(Model.branch_info))
            if branch_info_flag == self.DIRTY:
                dirty_list.append(Model.branch_info)
                break
            flag_map[Model.branch_info] = branch_info_flag

            # 出资信息
            contributive_info_flag = self.get_contributive_info(data_list.get(Model.contributive_info))
            if contributive_info_flag == self.DIRTY:
                dirty_list.append(Model.contributive_info)
                break
            flag_map[Model.contributive_info] = contributive_info_flag

            # 清算信息
            liquidation_info_flag = self.get_liquidation_info(data_list.get(Model.liquidation_info))
            if liquidation_info_flag == self.DIRTY:
                dirty_list.append(Model.liquidation_info)
                break
            flag_map[Model.liquidation_info] = liquidation_info_flag

            # 年报信息
            annual_info_flag = self.get_annual_info(data_list.get(Model.annual_info))
            if annual_info_flag == self.DIRTY:
                dirty_list.append(Model.annual_info)
                break
            flag_map[Model.annual_info] = annual_info_flag

            # 判断是否有不完整数据
            check_success = self.CORRECT
            for key, flag in flag_map.iteritems():
                if flag != self.CORRECT:
                    check_success = flag
                    incomplete_list.append(key)

            # 退出while循环
            break

        # 脏数据
        if check_success == self.DIRTY:
            self.log.error('脏数据: province = {province} company = {detail_name} 属性: {field}'.format(
                province=self.province, detail_name=detail_name, field=','.join(dirty_list)))
        elif check_success == self.INCOMPLETE:
            self.log.error('属性不完整数据: province = {province} company = {detail_name} 属性: {field}'.format(
                province=self.province, detail_name=detail_name, field=','.join(incomplete_list)))

        return check_success

    def check_field(self, field_info, feature_list=None, classify=Model.type_list, data_type='html'):
        '''
        :param field_info: 属性信息 base_info or change_info ...
        :param feature_list: 特征值
        :param classify: 需要检查的分类 list or detail
        :param data_type: 样本数据类型 html xml json
        :return: CORRECT INCOMPLETE DIRTY
        '''
        if field_info is None:
            return self.INCOMPLETE

        field_item_list = field_info.get(classify)
        if field_item_list is None:
            return self.INCOMPLETE

        check_flag = self.CORRECT
        for item in field_item_list:

            text = item.get('text')
            status = item.get('status')
            if status is None or status == 'fail':
                check_flag = self.INCOMPLETE
                continue

            # 如果是字段不存在的 不进行检测
            if status == 'not exist':
                continue

            if data_type == 'html':
                if not isinstance(feature_list, list):
                    continue
                flag = self.DIRTY
                for f in feature_list:
                    if f in text:
                        flag = self.CORRECT
                        break

                if flag == self.DIRTY:
                    check_flag = self.DIRTY
                    break

            elif data_type == 'json':
                try:
                    json.loads(text)
                except ValueError:
                    return self.DIRTY
            elif data_type == 'xml':
                try:
                    PyQuery(text, parser='xml')
                except XMLSyntaxError:
                    return self.DIRTY

        return check_flag

    # 基本信息
    def get_base_info(self, base_info):
        '''
        :param base_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        基本信息一般存储在list列表中, 因为基本信息不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        '''
        return self.CORRECT

    # 股东信息
    def get_shareholder_info(self, shareholder_info):
        '''
        :param shareholder_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        股东信息一般存储在list列表中, 因为股东信息不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        '''
        return self.CORRECT

    # 变更信息
    def get_change_info(self, change_info):
        '''
        :param change_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        变更信息一般只包含list列表, 但是特殊情况下也会有detail详情页列表 比如 北京这个省份有发现过包含详情页的变更信息
        :return: 返回工商schema字典
        '''
        return self.CORRECT

    # 主要人员
    def get_key_person_info(self, key_person_info):
        '''
        :param key_person_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        主要人员一般存储在list列表中, 因为主要人员不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        '''
        return self.CORRECT

    # 分支机构
    def get_branch_info(self, branch_info):
        '''
        :param branch_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        分支机构一般存储在list列表中, 因为分支机构不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        '''
        return self.CORRECT

    # 出资信息
    def get_contributive_info(self, contributive_info):
        '''
        :param contributive_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        出资信息一般会由两个列表分别进行存储, 但部分省份也可能只包含list列表, 没有详情页信息
        :return: 返回工商schema字典
        '''
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
