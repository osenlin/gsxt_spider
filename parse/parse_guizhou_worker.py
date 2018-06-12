#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: parse_base_worker.py
@time: 2017/2/3 17:32
"""
import json

from base.parse_base_worker import ParseBaseWorker
from common import util
from common.annual_field import *
from common.global_field import Model
from common.gsxt_field import *


class GsxtParseGuiZhouWorker(ParseBaseWorker):
    def __init__(self, **kwargs):
        ParseBaseWorker.__init__(self, **kwargs)

    # 基本信息
    def get_base_info(self, base_info):
        page = self.get_crawl_page(base_info)
        if page is None or page == u'':
            return {}

        native_json = util.json_loads(page)
        if native_json is None:
            return {}

        json_data_arr = native_json.get('data', [])
        if json_data_arr is None or len(json_data_arr) == 0:
            return {}

        base_info_dict = self.parse_base_data(json_data_arr)
        base_info_dict = replace_none(base_info_dict)
        return base_info_dict

    # 股东信息
    def get_shareholder_info(self, shareholder_info):
        shareholder_info_dict = {}
        share_model = {}
        page_text = self.get_crawl_page(shareholder_info)
        lst_shareholder = []
        if page_text is None or page_text == u'':
            return {}

        native_json = util.json_loads(page_text)
        if native_json is None:
            return {}

        json_data_arr = native_json.get('data', [])
        if json_data_arr is None:
            return {}

        for data in json_data_arr:
            share_model[GsModel.ShareholderInformation.SHAREHOLDER_NAME] = data.get('tzrmc', '')
            share_model[GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT] = util.get_amount_with_unit(
                data.get('rjcze', ''))
            share_model[GsModel.ShareholderInformation.PAIED_AMOUNT] = util.get_amount_with_unit(
                data.get('sjcze', ''))

            share_model[GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL] = []

            # 认缴细节
            sub_detail_dict = {
                GsModel.ShareholderInformation.SUBSCRIPTION_TYPE: data.get('rjczfs', ''),
                GsModel.ShareholderInformation.SUBSCRIPTION_TIME: data.get('rjczrq', ''),
                GsModel.ShareholderInformation.SUBSCRIPTION_PUBLISH_TIME: data.get('rjgsrq', ''),
            }
            sub_detail_dict = replace_none(sub_detail_dict)
            share_model[GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL].append(sub_detail_dict)
            # 实缴细节
            share_model[GsModel.ShareholderInformation.PAIED_DETAIL] = []

            paid_dict = {
                GsModel.ShareholderInformation.PAIED_TYPE: data.get('sjczfs', ''),
                GsModel.ShareholderInformation.PAIED_TIME: data.get('sjczrq', ''),
                GsModel.ShareholderInformation.PAIED_PUBLISH_TIME: data.get('sjgsrq', ''),
            }
            paid_dict = replace_none(paid_dict)
            share_model[GsModel.ShareholderInformation.PAIED_DETAIL].append(paid_dict)

            # 股东补丁
            share_model = replace_none(share_model)
            lst_shareholder.append(share_model)
        shareholder_info_dict[GsModel.SHAREHOLDER_INFORMATION] = lst_shareholder

        return shareholder_info_dict

    # 变更信息
    def get_change_info(self, change_info):
        change_info_dict = {}
        page_text = self.get_crawl_page(change_info)
        lst_change_records = []
        if page_text is None or page_text == u'':
            return {}

        native_json = util.json_loads(page_text)
        if native_json is None:
            return {}

        json_data_arr = native_json.get('data', [])
        if json_data_arr is None:
            return {}

        for data in json_data_arr:
            change_model = {
                GsModel.ChangeRecords.CHANGE_ITEM: data.get('bcsxmc', ''),
                # 去除多余的字
                GsModel.ChangeRecords.BEFORE_CONTENT: util.format_content(data.get('bcnr', '')),
                GsModel.ChangeRecords.AFTER_CONTENT: util.format_content(data.get('bghnr', '')),
                # 日期格式化
                GsModel.ChangeRecords.CHANGE_DATE: data.get('hzrq', '')
            }
            change_model = replace_none(change_model)
            lst_change_records.append(change_model)
        change_info_dict[GsModel.CHANGERECORDS] = lst_change_records
        return change_info_dict

    # 主要人员
    def get_key_person_info(self, key_person_info):
        key_person_info_dict = {}
        lst_key_person = []
        page_text = self.get_crawl_page(key_person_info)
        if page_text is None or page_text == u'':
            return {}

        native_json = util.json_loads(page_text)
        if native_json is None:
            return {}

        json_data_arr = native_json.get('data', [])
        if json_data_arr is None:
            return {}

        for data in json_data_arr:
            key_person = {
                GsModel.KeyPerson.KEY_PERSON_NAME: data.get('xm', u''),
                GsModel.KeyPerson.KEY_PERSON_POSITION: data.get('zwmc', u'')}
            key_person = replace_none(key_person)
            lst_key_person.append(key_person)
        key_person_info_dict[GsModel.KEY_PERSON] = lst_key_person
        return key_person_info_dict

    # 分支机构
    def get_branch_info(self, branch_info):
        branch_info_dict = {}
        lst_branch = []
        page_text = self.get_crawl_page(branch_info)
        if page_text is None or page_text == u'':
            return {}

        native_json = util.json_loads(page_text)
        if native_json is None:
            return {}

        json_data_arr = native_json.get('data', [])
        if json_data_arr is None:
            return {}

        for data in json_data_arr:
            branch_model = {
                GsModel.Branch.COMPAY_NAME: data.get('fgsmc', '').strip(),
                GsModel.Branch.CODE: data.get('fgszch', '').strip(),
            }
            branch_model = replace_none(branch_model)
            lst_branch.append(branch_model)
        branch_info_dict[GsModel.BRANCH] = lst_branch
        return branch_info_dict

    # 出资信息
    def get_contributive_info(self, con_info):
        con_info_dict = {}
        part_a_con = {}
        part_b_con = {}
        con_list = []
        page_text = self.get_crawl_page(con_info)
        if page_text is None or page_text == u'':
            return {}

        native_json = util.json_loads(page_text)
        if native_json is None:
            return {}

        json_data_arr = native_json.get('data', [])
        if json_data_arr is None:
            return {}

        for data in json_data_arr:
            sub_model = {
                GsModel.ContributorInformation.SHAREHOLDER_NAME: data.get('czmc', ''),
                GsModel.ContributorInformation.SHAREHOLDER_TYPE: data.get('tzrlxmc', ''),
                GsModel.ContributorInformation.CERTIFICATE_TYPE: data.get('zzlxmc', ''),
                GsModel.ContributorInformation.CERTIFICATE_NO: data.get('zzbh', '')
            }
            sub_model = replace_none(sub_model)  # 打补丁
            part_a_con[data.get('czmc', '')] = sub_model
        pages_detail = self.get_crawl_page(con_info, True, Model.type_detail)

        if pages_detail is not None:
            for page_item in pages_detail:
                if page_item is None:
                    continue

                shareholder_name, sub_model = self.get_con_detail(page_item.get(u'text', u''))
                part_b_con[shareholder_name] = sub_model

        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            con_list.append(v_list)
        con_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = con_list

        return con_info_dict

    # 获取股东信息详细列表
    @staticmethod
    def get_con_detail(page_text):
        shareholder_name = ""
        sub_model = {}
        if page_text == u'' or page_text is None:
            return shareholder_name, sub_model

        native_json = util.json_loads(page_text)
        if native_json is None:
            return shareholder_name, sub_model

        json_data_arr = native_json.get('data', [])
        if json_data_arr is None:
            return shareholder_name, sub_model

        for json_item in json_data_arr:
            lst_sub_detail = []
            lst_paid_detail = []
            shareholder_name = json_item.get('czmc', '')
            sub_model[GsModel.ContributorInformation.SHAREHOLDER_NAME] = json_item.get('czmc', '')
            sub_model[GsModel.ContributorInformation.SUBSCRIPTION_AMOUNT] = json_item.get('rjcze', '')  # 自带单位
            sub_model[GsModel.ContributorInformation.PAIED_AMOUNT] = json_item.get('sjcze', '')  # 自带单位

            sub_model_detail = {
                GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TYPE: json_item.get('rjczfsmc', ''),
                GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT: json_item.get('rjcze1', ''),
                GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TIME:
                    json_item.get('rjczrq', ''),  # 自带单位
            }
            sub_model_detail = replace_none(sub_model_detail)  # 补丁

            lst_sub_detail.append(sub_model_detail)
            sub_model[GsModel.ContributorInformation.SUBSCRIPTION_DETAIL] = lst_sub_detail

            paid_model_detail = {
                GsModel.ContributorInformation.PaiedDetail.PAIED_TYPE: json_item.get('sjczfsmc', ''),
                GsModel.ContributorInformation.PaiedDetail.PAIED_AMOUNT: json_item.get('sjcze1', ''),  # 自带单位
                GsModel.ContributorInformation.PaiedDetail.PAIED_TIME:
                    json_item.get('sjczrq', ''),
            }

            paid_model_detail = replace_none(paid_model_detail)  # 补丁
            lst_paid_detail.append(paid_model_detail)
            sub_model[GsModel.ContributorInformation.PAIED_DETAIL] = lst_paid_detail
            sub_model = replace_none(sub_model)  # 补丁
        return shareholder_name, sub_model

    # 清算信息
    def get_liquidation_info(self, liquidation_info):
        return {}

    @staticmethod
    def parse_base_data(json_data_arr):
        json_data = json_data_arr[0]
        data = {GsModel.CODE: json_data.get('zch', ''),
                GsModel.HEZHUN_DATE: json_data.get('hzrq', ''),
                GsModel.COMPANY: json_data.get('qymc', ''),
                GsModel.BUSINESS_STATUS: json_data.get('mclxmc', ''),
                GsModel.REGISTERED_DATE: json_data.get('clrq', ''),
                GsModel.REGISTERED_ADDRESS: json_data.get('djjgmc', ''),
                GsModel.ENTERPRISE_TYPE: json_data.get('qylxmc', ''),
                GsModel.LEGAL_MAN: json_data.get('fddbr', ''),
                GsModel.REGISTERED_CAPITAL: json_data.get('zczb', ''),
                GsModel.ADDRESS: json_data.get('zs', ''),
                GsModel.BUSINESS_SCOPE: json_data.get('jyfw', ''),
                GsModel.PERIOD_FROM: json_data.get('yyrq1', ''),
                GsModel.PERIOD_TO: json_data.get('yyrq2', '')}
        data = replace_none(data)  # 补丁
        data[GsModel.PERIOD] = u"{0}至{1}".format(data.get(GsModel.PERIOD_FROM),
                                                 data.get(GsModel.PERIOD_TO))
        data = replace_none(data)  # 补丁
        return data

    # 年报信息
    def get_annual_info(self, annual_item_list):
        return ParseGuiZhouAnnual(annual_item_list, self.log).get_result()


class ParseGuiZhouAnnual(object):
    def __init__(self, annual_item_list, log):
        self.annual_info_dict = {}
        if not isinstance(annual_item_list, list) or len(annual_item_list) <= 0:
            return

        self.log = log
        self.annual_item_list = annual_item_list

        # 分发解析
        self.dispatch()

    def dispatch(self):
        if self.annual_item_list is None:
            raise IndexError("未抓取到相关网页,或者抓取网页失败")

        if len(self.annual_item_list) <= 0:
            return {}

        if len(self.annual_item_list) == 0:
            return {}

        for page_item in self.annual_item_list:
            page = page_item.get(u'text', u'')
            if page == u'':
                # 空数据
                continue

            page_json = util.json_loads(page)
            if page_json is None:
                continue

            json_data_arr = page_json.get('data', None)
            if json_data_arr is None or len(json_data_arr) == 0:
                continue

            # 基本信息
            if json_data_arr[0].get('qymc', '') != '':  # 企业名称
                annual_base_info = self.get_annual_base_info(json_data_arr)
                self.annual_info_dict.update(annual_base_info)

            # 年报 企业资产状况信息
            if json_data_arr[0].get('zcze', '') != '' and json_data_arr[0].get('nsze', '') != '':
                asset_model = self.get_annual_asset_info(json_data_arr)
                self.annual_info_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = asset_model

            # 网站或网店信息
            if json_data_arr[0].get('wz', '') != '':
                lst_websites = self.get_annual_out_website(json_data_arr)
                self.annual_info_dict[AnnualReports.WEBSITES] = lst_websites

            # 对外投资
            if json_data_arr[0].get('zch', '') != '' and json_data_arr[0].get('rownum', '') != '':
                self.annual_info_dict[AnnualReports.INVESTED_COMPANIES] = self.get_annual_inv_info(json_data_arr)

            # 修改信息
            if json_data_arr[0].get('bgsxmc', '') != '':
                lst_edit_change = self.get_annual_edit_change(json_data_arr)
                self.annual_info_dict[AnnualReports.EDIT_CHANGE_INFOS] = lst_edit_change

            # 股东出资信息
            if json_data_arr[0].get('tzr', '') != '':
                lst_share_hold = self.get_annual_share_hold_info(json_data_arr)
                self.annual_info_dict[AnnualReports.SHAREHOLDER_INFORMATION] = lst_share_hold

            # 股权变更
            if json_data_arr[0].get('gd', '') != '':
                lst_edit_shareholding_change = self.get_annual_edit_shareholding_change(json_data_arr)
                self.annual_info_dict[AnnualReports.EDIT_SHAREHOLDING_CHANGE_INFOS] = lst_edit_shareholding_change

            # 对外担保
            if json_data_arr[0].get('zqr', '') != '':
                lst_annual_out_guarantee = self.get_annual_out_guarantee_info(json_data_arr)
                self.annual_info_dict[AnnualReports.OUT_GUARANTEE_INFO] = lst_annual_out_guarantee

    # 发起人及出资信息
    @staticmethod
    def get_annual_share_hold_info(json_data_arr):
        lst = []
        for js_item in json_data_arr:
            share_model = {
                AnnualReports.ShareholderInformation.SHAREHOLDER_NAME: js_item.get('tzr', u''),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                    js_item.get('rjcze', u'')),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TIME: js_item.get('rjczrq', u''),  # 认缴时间
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TYPE: js_item.get('rjczfs', u''),  # 认缴类型 #有坑

                AnnualReports.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(js_item.get('sjcze', u'')),
                # 1实缴金额
                AnnualReports.ShareholderInformation.PAIED_TIME: js_item.get('sjczrq', u''),  # 实缴时间
                AnnualReports.ShareholderInformation.PAIED_TYPE: js_item.get('sjczfs', u''),  # 实缴类型 #有坑

            }
            share_model = replace_none(share_model)  # 补丁
            lst.append(share_model)
        return lst

    @staticmethod
    def get_annual_base_info(json_data_arr):
        basic_model = []
        for js_item in json_data_arr:
            basic_model = {
                AnnualReports.CODE: js_item.get('zch', ''),
                AnnualReports.ZIP_CODE: js_item.get('yzbm', ''),
                AnnualReports.CONTACT_NUMBER: js_item.get('lxdh', ''),
                AnnualReports.COMPANY_NAME: js_item.get('qymc', ''),
                AnnualReports.ADDRESS: js_item.get('dz', ''),
                AnnualReports.EMAIL: js_item.get('dzyx', ''),
                AnnualReports.EMPLOYED_POPULATION: js_item.get('cyrs', ''),
                AnnualReports.BUSINESS_STATUS: js_item.get('jyzt', ''),
                AnnualReports.IS_WEB: js_item.get('sfww', ''),
                AnnualReports.IS_TRANSFER: js_item.get('sfzr', ''),
                AnnualReports.IS_INVEST: js_item.get('sfdw', ''),
                AnnualReports.EMPLOYED_POPULATION_WOMAN: js_item.get('cyrsnx', ''),
                AnnualReports.ENTERPRISE_HOLDING: js_item.get('kgqk', ''),
                AnnualReports.BUSINESS_ACTIVITIES: js_item.get('zyyw', '')
            }
            basic_model = replace_none(basic_model)  # 补丁
        return basic_model

    @staticmethod
    def get_annual_asset_info(json_data_arr):
        asset_model = {}
        for js_item in json_data_arr:
            asset_model = {
                AnnualReports.EnterpriseAssetStatusInformation.GENERAL_ASSETS: js_item.get('zcze', '').strip(),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_EQUITY: js_item.get('qyhj', '').strip(),
                AnnualReports.EnterpriseAssetStatusInformation.GROSS_SALES: js_item.get('xsze', '').strip(),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_PROFIT: js_item.get('lrze', '').strip(),
                AnnualReports.EnterpriseAssetStatusInformation.INCOME_OF_TOTAL: js_item.get('zysr', '').strip(),
                AnnualReports.EnterpriseAssetStatusInformation.RETAINED_PROFITS: js_item.get('jlr', '').strip(),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_TAX: js_item.get('nsze', '').strip(),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_INDEBTEDNESS: js_item.get('fzze', '').strip()
            }
            asset_model = replace_none(asset_model)  # 补丁
        return asset_model

    @staticmethod
    def get_annual_out_website(json_data_arr):
        lst = []
        for js_item in json_data_arr:
            web_model = {
                AnnualReports.WebSites.TYPE: js_item.get(u'lx', u''),
                AnnualReports.WebSites.SITE: js_item.get(u'wz', u''),
                AnnualReports.WebSites.NAME: js_item.get(u'mc', u'')
            }
            web_model = replace_none(web_model)  # 补丁
            lst.append(web_model)
        return lst

    # 对外投资
    @staticmethod
    def get_annual_inv_info(json_data_arr):
        lst = []
        for js_item in json_data_arr:
            model = {AnnualReports.InvestedCompanies.COMPANY_NAME: js_item.get(u'mc', u''),
                     AnnualReports.InvestedCompanies.CODE: js_item.get(u'zch', u'')}
            model = replace_none(model)  # 补丁
            lst.append(model)
        return lst

    # 修改信息
    @staticmethod
    def get_annual_edit_change(json_data_arr):
        lst = []
        for js_item in json_data_arr:
            edit_model = {
                AnnualReports.EditChangeInfos.CHANGE_ITEM: js_item.get(u'bgsxmc', u''),
                AnnualReports.EditChangeInfos.BEFORE_CONTENT: js_item.get(u'bgq', u''),
                AnnualReports.EditChangeInfos.AFTER_CONTENT: js_item.get(u'bgh', u''),
                AnnualReports.EditChangeInfos.CHANGE_DATE: js_item.get(u'bgrq', u'')
            }
            edit_model = replace_none(edit_model)  # 补丁
            lst.append(edit_model)
        return lst

    # 股权变更
    @staticmethod
    def get_annual_edit_shareholding_change(json_data_arr):
        lst = []
        for js_item in json_data_arr:
            edit_model = {
                AnnualReports.EditShareholdingChangeInfos.SHAREHOLDER_NAME: js_item.get(u'inv', u''),
                AnnualReports.EditShareholdingChangeInfos.BEFORE_CONTENT: js_item.get(u'transbmpr', u''),
                AnnualReports.EditShareholdingChangeInfos.AFTER_CONTENT: js_item.get(u'transampr', u''),
                AnnualReports.EditShareholdingChangeInfos.CHANGE_DATE: js_item.get(u'altdatelabel', u'')
            }
            edit_model = replace_none(edit_model)  # 补丁
            lst.append(edit_model)
        return lst

    @staticmethod
    def get_annual_out_guarantee_info(json_data_arr):
        lst = []
        for js_item in json_data_arr:
            share_model = {
                AnnualReports.OutGuaranteeInfo.CREDITOR: js_item.get('zqr', u''),  #
                AnnualReports.OutGuaranteeInfo.OBLIGOR: js_item.get('zwr', u''),  #
                AnnualReports.OutGuaranteeInfo.DEBT_TYPE: js_item.get('zzqzl', u''),  #
                AnnualReports.OutGuaranteeInfo.DEBT_AMOUNT: util.get_amount_with_unit(js_item.get('zzqse', u''), '元'),
                AnnualReports.OutGuaranteeInfo.PERFORMANCE_PERIOD: js_item.get('zwqx', u''),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_PERIOD: js_item.get('bzfs', u''),  # 担保期限
                AnnualReports.OutGuaranteeInfo.GUARANTEE_TYPE: js_item.get('bzqj', u''),  # 担保方式
            }
            share_model = replace_none(share_model)  # 补丁
            lst.append(share_model)
        return lst

    def get_result(self):
        return self.annual_info_dict


def replace_none(temp_model):
    for k, v in temp_model.items():
        if v is None:
            v = ''
            temp_model[k] = v
    return temp_model
