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

from pyquery import PyQuery

from base.parse_base_worker import ParseBaseWorker
from common import util
from common.annual_field import *
from common.global_field import Model
from common.gsxt_field import *


class GsxtParseJiangSuWorker(ParseBaseWorker):
    def __init__(self, **kwargs):
        ParseBaseWorker.__init__(self, **kwargs)

    # 基本信息
    def get_base_info(self, base_info):
        """
            :param base_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
            其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
            具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
            基本信息一般存储在list列表中, 因为基本信息不包含列表结构不需要detail列表
            :return: 返回工商schema字典
            """
        page = self.get_crawl_page(base_info)
        page = json.loads(page)
        base_info_dict = {
            u'统一社会信用代码/注册号': page.get('REG_NO'),
            u'企业名称': page.get('CORP_NAME'),
            u'类型': page.get('ZJ_ECON_KIND'),
            u'法定代表人': page.get('OPER_MAN_NAME'),
            u'注册资本': page.get('REG_CAPI_WS'),
            u'成立日期': page.get('START_DATE'),
            u'经营期限自': page.get('FARE_TERM_START'),
            u'经营期限至': page.get('FARE_TERM_END'),
            u'登记机关': page.get('BELONG_ORG'),
            u'核准日期': page.get('CHECK_DATE'),
            u'登记状态': page.get('CORP_STATUS'),
            u'住所': page.get('ADDR'),
            u'经营范围': page.get('FARE_SCOPE')
        }
        base_info_dict = bu_ding(base_info_dict)
        new_base_info_dict = {}
        for k, v in base_info_dict.items():
            new_k = GsModel.format_base_model(k)
            new_base_info_dict[new_k] = v

        new_base_info_dict[GsModel.PERIOD] = u"{0}至{1}".format(
            new_base_info_dict.get(GsModel.PERIOD_FROM), new_base_info_dict.get(GsModel.PERIOD_TO))
        return new_base_info_dict

    # 股东信息
    def get_shareholder_info(self, shareholder_info):
        shareholder_info_dict = {}
        page_list = self.get_crawl_page(shareholder_info, True)
        if page_list is None:
            return shareholder_info_dict

        lst_shareholder = []
        for page in page_list:
            text = page.get('text')
            json_data = util.json_loads(text)
            if json_data is None:
                continue

            data_list = json_data.get('data', [])
            for data in data_list:
                if data is None or len(data) == 0:
                    continue

                share_model = {}
                py_page = data.get('D1')
                if py_page is None:
                    continue

                td = PyQuery(py_page).find('td')
                if len(td) < 2:
                    continue

                share_model[GsModel.ShareholderInformation.SHAREHOLDER_NAME] = td.eq(1).text()
                share_model[GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT] = util.get_amount_with_unit(
                    td.eq(2).text().replace(',', ''))
                share_model[GsModel.ShareholderInformation.PAIED_AMOUNT] = util.get_amount_with_unit(
                    td.eq(3).text().replace(',', ''))

                share_model[GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL] = [{
                    GsModel.ShareholderInformation.SUBSCRIPTION_TYPE: td.eq(4).text(),
                    GsModel.ShareholderInformation.SUBSCRIPTION_TIME: td.eq(6).text(),
                    GsModel.ShareholderInformation.SUBSCRIPTION_PUBLISH_TIME: td.eq(7).text()
                }]

                share_model[GsModel.ShareholderInformation.PAIED_DETAIL] = [{
                    GsModel.ShareholderInformation.PAIED_TYPE: td.eq(8).text(),
                    GsModel.ShareholderInformation.PAIED_TIME: td.eq(10).text(),
                    GsModel.ShareholderInformation.PAIED_PUBLISH_TIME: td.eq(11).text()
                }]
                share_model = bu_ding(share_model)
                lst_shareholder.append(share_model)

        if len(lst_shareholder) > 0:
            shareholder_info_dict[GsModel.SHAREHOLDER_INFORMATION] = lst_shareholder

        return shareholder_info_dict

    # 变更信息
    def get_change_info(self, change_info):
        """
        :param change_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        变更信息一般只包含list列表, 但是特殊情况下也会有detail详情页列表 比如 北京这个省份有发现过包含详情页的变更信息
        :return: 返回工商schema字典
        """
        change_info_dict = {}
        page_list = self.get_crawl_page(change_info, True)
        if page_list is None:
            return change_info_dict

        change_record_list = []
        for page in page_list:
            text = page.get('text')
            native_json = util.json_loads(text)
            if native_json is None:
                continue

            json_data_arr = native_json.get('data', [])
            for data in json_data_arr:
                change_model = {
                    GsModel.ChangeRecords.CHANGE_ITEM: data.get('CHANGE_ITEM_NAME'),
                    # 去除多余的字
                    GsModel.ChangeRecords.BEFORE_CONTENT: util.format_content(data.get('OLD_CONTENT')),
                    GsModel.ChangeRecords.AFTER_CONTENT: util.format_content(data.get('NEW_CONTENT')),
                    # 日期格式化
                    GsModel.ChangeRecords.CHANGE_DATE: data.get('CHANGE_DATE')
                }
                change_model = bu_ding(change_model)
                change_record_list.append(change_model)

        if len(change_record_list) > 0:
            change_info_dict[GsModel.CHANGERECORDS] = change_record_list

        return change_info_dict

    # 主要人员
    def get_key_person_info(self, key_person_info):
        """
        :param key_person_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        主要人员一般存储在list列表中, 因为主要人员不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        """
        key_person_info_dict = {}
        lst_key_person = []
        page_list = self.get_crawl_page(key_person_info)
        json_data_arr = util.json_loads(page_list)
        if json_data_arr is None:
            return key_person_info_dict

        for data in json_data_arr:
            key_person_model = {
                GsModel.KeyPerson.KEY_PERSON_NAME: data.get('PERSON_NAME'),
                GsModel.KeyPerson.KEY_PERSON_POSITION: data.get('POSITION_NAME')
            }
            key_person_model = bu_ding(key_person_model)
            lst_key_person.append(key_person_model)

        if len(lst_key_person) > 0:
            key_person_info_dict[GsModel.KEY_PERSON] = lst_key_person

        return key_person_info_dict

    # 分支机构
    def get_branch_info(self, branch_info):
        """
            :param branch_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
            其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
            具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
            分支机构一般存储在list列表中, 因为分支机构不包含列表结构不需要detail列表
            :return: 返回工商schema字典
            """
        branch_info_dict = {}
        lst_branch = []

        page_list = self.get_crawl_page(branch_info)
        json_data_arr = util.json_loads(page_list)
        if json_data_arr is None:
            return branch_info_dict

        for data in json_data_arr:
            branch_model = {
                GsModel.Branch.COMPAY_NAME: data.get('DIST_NAME'),
                GsModel.Branch.CODE: data.get('DIST_REG_NO'),
                GsModel.Branch.REGISTERED_ADDRESS: data.get('DIST_BELONG_ORG')
            }
            branch_model = bu_ding(branch_model)
            lst_branch.append(branch_model)

        if len(lst_branch) > 0:
            branch_info_dict[GsModel.BRANCH] = lst_branch

        return branch_info_dict

    # 出资信息
    def get_contributive_info(self, contributive_info):
        con_info_dict = {}
        part_a_con = {}
        part_b_con = {}
        page_list = self.get_crawl_page(contributive_info, True)
        if page_list is None:
            return con_info_dict

        for page in page_list:
            text = page.get('text')
            native_json = util.json_loads(text)
            if native_json is None:
                continue

            json_data_arr = native_json.get('data', [])
            for data in json_data_arr:
                sub_model = {
                    GsModel.ContributorInformation.SHAREHOLDER_NAME: data.get('STOCK_NAME'),
                    GsModel.ContributorInformation.SHAREHOLDER_TYPE: data.get('STOCK_TYPE'),
                    GsModel.ContributorInformation.CERTIFICATE_TYPE: data.get('IDENT_TYPE_NAME'),
                    GsModel.ContributorInformation.CERTIFICATE_NO: data.get('IDENT_NO')
                }
                sub_model = bu_ding(sub_model)
                part_a_con[sub_model[GsModel.ContributorInformation.SHAREHOLDER_NAME]] = sub_model

        pages_detail = self.get_crawl_page(contributive_info, True, Model.type_detail)
        if pages_detail is not None:
            # todo 江苏没有列表案例, 无法按列表解析
            shareholder_name, sub_model = self.get_share_hold_detail(pages_detail)
            part_b_con[shareholder_name] = sub_model

        con_list = []
        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            con_list.append(v_list)

        if len(con_list) > 0:
            con_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = con_list
        return con_info_dict

    # 从json中获取出资信息的股东信息详情页
    @staticmethod
    def get_share_hold_detail(page_list):
        shareholder_name = ""
        sub_model = {}
        if page_list is None:
            return shareholder_name, sub_model
        sub_model = {}
        items = json.loads(page_list.get('text'))
        sub_model[GsModel.ContributorInformation.SHAREHOLDER_NAME] = items.get('inv')
        sub_model[GsModel.ContributorInformation.SUBSCRIPTION_AMOUNT] = items.get('lisubconam')
        sub_model[GsModel.ContributorInformation.PAIED_AMOUNT] = items.get('liacconam')

        sub_model = bu_ding(sub_model)
        shareholder_name = sub_model[GsModel.ContributorInformation.SHAREHOLDER_NAME]
        return shareholder_name, sub_model

    # 清算信息
    def get_liquidation_info(self, liquidation_info):
        return {}

    # 年报信息
    def get_annual_info(self, annual_item_list):
        return ParseJiangSuAnnual(annual_item_list, self.log).get_result()


class ParseJiangSuAnnual(object):
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

        dict_annual = {}
        for lst_annual in self.annual_item_list:
            if 'nbxxJbxx' in lst_annual.get('url'):
                dict_annual['nbxxJbxx'] = util.json_loads(lst_annual.get('text'))
            elif 'nbxxWzwd' in lst_annual.get('url'):
                dict_annual['nbxxWzwd'] = util.json_loads(lst_annual.get('text'))
            elif 'nbxxGdcz' in lst_annual.get('url'):
                dict_annual['nbxxGdcz'] = util.json_loads(lst_annual.get('text'))
            elif 'nbxxDwtz' in lst_annual.get('url'):
                dict_annual['nbxxDwtz'] = util.json_loads(lst_annual.get('text'))
            elif 'nbxxDwdb' in lst_annual.get('url'):
                dict_annual['nbxxDwdb'] = util.json_loads(lst_annual.get('text'))
            elif 'nbxxGqbg' in lst_annual.get('url'):
                dict_annual['nbxxGqbg'] = util.json_loads(lst_annual.get('text'))
            elif 'nbxxXgxx' in lst_annual.get('url'):
                dict_annual['nbxxXgxx'] = util.json_loads(lst_annual.get('text'))

        # 基本信息
        base_info = dict_annual.get('nbxxJbxx')
        if base_info is not None:
            annual_base_info = self.get_annual_base_info(base_info)
            self.annual_info_dict.update(annual_base_info)

        # 年报 企业资产状况信息
        asset_info = dict_annual.get('nbxxJbxx')
        if asset_info is not None:
            asset_model = self.get_annual_asset_info(asset_info)
            self.annual_info_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = asset_model

        # 网站或网店信息
        web_info = dict_annual.get('nbxxWzwd')
        if web_info is not None:
            lst_websites = self.get_annual_web_site_info(web_info)
            self.annual_info_dict[AnnualReports.WEBSITES] = lst_websites

        # 股东出资信息
        share_hold_info = dict_annual.get('nbxxGdcz')
        if share_hold_info is not None:
            lst_share_hold = self.get_annual_share_hold_info(share_hold_info)
            self.annual_info_dict[AnnualReports.SHAREHOLDER_INFORMATION] = lst_share_hold

        # 对外投资
        inv_info = dict_annual.get('nbxxDwtz')
        if inv_info is not None:
            lst_inv = self.get_annual_inv_info(inv_info)
            self.annual_info_dict[AnnualReports.INVESTED_COMPANIES] = lst_inv

        # 对外担保
        out_guaranty_info = dict_annual.get('nbxxDwdb')
        if out_guaranty_info is not None:
            lst_out_guaranty = self.get_annual_out_guarantee_info(out_guaranty_info)
            self.annual_info_dict[AnnualReports.OUT_GUARANTEE_INFO] = lst_out_guaranty

        # 股权变更
        edit_shareholding_change_info = dict_annual.get('nbxxGqbg')
        if edit_shareholding_change_info is not None:
            lst_edit_shareholding_change = self.get_annual_edit_shareholding_change(edit_shareholding_change_info)
            self.annual_info_dict[AnnualReports.EDIT_SHAREHOLDING_CHANGE_INFOS] = lst_edit_shareholding_change

        # 修改记录
        edit_change_info = dict_annual.get('nbxxXgxx')
        if edit_change_info is not None:
            lst_edit_change = self.get_annual_edit_change(edit_change_info)
            self.annual_info_dict[AnnualReports.EDIT_CHANGE_INFOS] = lst_edit_change

    # 年报基本信息
    @staticmethod
    def get_annual_base_info(info):
        annual_base_info_dict = {
            AnnualReports.CODE: info.get('REG_NO'),
            AnnualReports.COMPANY_NAME: info.get('CORP_NAME'),
            AnnualReports.ADDRESS: info.get('ADDR'),
            AnnualReports.ZIP_CODE: info.get('ZIP'),
            AnnualReports.CONTACT_NUMBER: info.get('TEL'),
            AnnualReports.EMAIL: info.get('E_MAIL'),
            AnnualReports.EMPLOYED_POPULATION: info.get('PRAC_PERSON_NUM'),
            AnnualReports.BUSINESS_STATUS: info.get('PRODUCE_STATUS'),
            AnnualReports.IS_INVEST: info.get('IF_INVEST'),
            AnnualReports.IS_WEB: info.get('IF_WEBSITE'),
            AnnualReports.IS_OUT_GUARANTEE: info.get('IF_DWBZDB'),
            AnnualReports.IS_TRANSFER: info.get('IF_EQUITY'),
            AnnualReports.EMPLOYED_POPULATION_WOMAN: info.get('WOM_EMP_NUM'),
            AnnualReports.ENTERPRISE_HOLDING: info.get('HOLDINGS_MSG'),
            AnnualReports.BUSINESS_ACTIVITIES: info.get('MAIN_BUSIACT'),
            AnnualReports.EMPLOYED_POPULATION_INCREASED: info.get('ADD_NUM'),
            AnnualReports.EMPLOYED_POPULATION_QUIT: info.get('EXIT_NUM'),
            AnnualReports.EMPLOYED_POPULATION_FARMER: info.get('FARMER_NUM'),
            AnnualReports.LEGAL_MAN: info.get('OPER_MAN_NAME'),
        }
        if 'PARENT_CORP_NAME' in info.keys():
            annual_base_info_dict[AnnualReports.SUPER_COMPANY] = info.get('PARENT_CORP_NAME')
        if 'PARENT_CORP_NAME' in info.keys():
            annual_base_info_dict[AnnualReports.SUPER_CODE] = info.get('PARENT_REG_NO')
        annual_base_info_dict = bu_ding(annual_base_info_dict)
        return annual_base_info_dict

    # 年报 企业资产状况信息 纳税总额 RATGRO  获得政府扶持资金、补助 PRIYEASUB  营业额或营业收入 PRIYEASALES  盈余总额 PRIYEAPROFIT  金融贷款 PRIYEALOAN
    # todo 需要具体例子检查下字段是否相对应
    @staticmethod
    def get_annual_asset_info(info):
        if 'SUBSIDY' in info.keys():
            annual_asset_info_dict = {
                AnnualReports.EnterpriseAssetStatusInformation.GROSS_SALES: info.get('SALE_INCOME'),
                AnnualReports.EnterpriseAssetStatusInformation.RETAINED_PROFITS: info.get('PROFIT_TOTAL'),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_INDEBTEDNESS: info.get('LOAN'),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_TAX: info.get('TAX_TOTAL'),
                AnnualReports.EnterpriseAssetStatusInformation.FUND_SUBSIDY: info.get('SUBSIDY')
            }
        else:
            annual_asset_info_dict = {
                AnnualReports.EnterpriseAssetStatusInformation.GENERAL_ASSETS: info.get('NET_AMOUNT'),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_EQUITY: info.get('TOTAL_EQUITY'),
                AnnualReports.EnterpriseAssetStatusInformation.GROSS_SALES: info.get('SALE_INCOME'),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_PROFIT: info.get('PROFIT_TOTAL'),
                AnnualReports.EnterpriseAssetStatusInformation.INCOME_OF_TOTAL: info.get('SERV_FARE_INCOME'),
                AnnualReports.EnterpriseAssetStatusInformation.RETAINED_PROFITS: info.get('PROFIT_RETA'),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_TAX: info.get('TAX_TOTAL'),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_INDEBTEDNESS: info.get('DEBT_AMOUNT')
            }
        annual_asset_info_dict = bu_ding(annual_asset_info_dict)
        return annual_asset_info_dict

    # 年报网站信息
    @staticmethod
    def get_annual_web_site_info(web_info):
        lst_websites = []
        for web in web_info:
            web_item = {
                AnnualReports.WebSites.NAME: web.get('WEB_NAME'),
                AnnualReports.WebSites.TYPE: web.get('WEB_TYPE'),
                AnnualReports.WebSites.SITE: web.get('WEB_URL')
            }
            web_item = bu_ding(web_item)
            lst_websites.append(web_item)
        return lst_websites

    # 年报 股东出资信息(客户端分页)
    @staticmethod
    def get_annual_share_hold_info(shareholder_info):
        s_info = shareholder_info.get('data', [])
        lst = []
        for s in s_info:
            share_model = {
                AnnualReports.ShareholderInformation.SHAREHOLDER_NAME: s.get('STOCK_NAME'),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_AMOUNT: s.get('SHOULD_CAPI'),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TIME: s.get('SHOULD_CAPI_DATE'),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TYPE: s.get('SHOULD_CAPI_TYPE'),
                AnnualReports.ShareholderInformation.PAIED_AMOUNT: s.get('REAL_CAPI'),
                AnnualReports.ShareholderInformation.PAIED_TIME: s.get('REAL_CAPI_DATE'),
                AnnualReports.ShareholderInformation.PAIED_TYPE: s.get('REAL_CAPI_TYPE'),
            }
            share_model = bu_ding(share_model)
            lst.append(share_model)
        return lst

    # 年报 对外投资信息
    @staticmethod
    def get_annual_inv_info(inv_info):
        lst_inv = []
        for inv in inv_info:
            inv_item = {
                AnnualReports.InvestedCompanies.COMPANY_NAME: inv.get('INVEST_NAME'),
                AnnualReports.InvestedCompanies.CODE: inv.get('INVEST_REG_NO'),
            }
            inv_item = bu_ding(inv_item)
            lst_inv.append(inv_item)
        return lst_inv

    # 年报 对外担保方法
    @staticmethod
    def get_annual_out_guarantee_info(out_guaranty_info):
        out_guaranty_data = out_guaranty_info.get('data', [])
        lst = []
        for out_guaranty in out_guaranty_data:
            out_guarantee_model = {
                AnnualReports.OutGuaranteeInfo.CREDITOR: out_guaranty.get('CRED_NAME'),
                AnnualReports.OutGuaranteeInfo.OBLIGOR: out_guaranty.get('DEBT_NAME'),
                AnnualReports.OutGuaranteeInfo.DEBT_TYPE: out_guaranty.get('CRED_TYPE'),
                AnnualReports.OutGuaranteeInfo.DEBT_AMOUNT: out_guaranty.get('CRED_AMOUNT'),
                AnnualReports.OutGuaranteeInfo.PERFORMANCE_PERIOD: out_guaranty.get('GUAR_DATE'),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_PERIOD: out_guaranty.get('GUAR_PERIOD'),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_TYPE: out_guaranty.get('GUAR_TYPE')
            }
            out_guarantee_model = bu_ding(out_guarantee_model)
            lst.append(out_guarantee_model)
        return lst

    # 年报 股权变更方法
    @staticmethod
    def get_annual_edit_shareholding_change(edit_shareholding_change_info):
        edit_shareholding_change_data = edit_shareholding_change_info.get('data', [])
        lst = []
        for edit_shareholding_change in edit_shareholding_change_data:
            change_model = {
                AnnualReports.EditShareholdingChangeInfos.SHAREHOLDER_NAME: edit_shareholding_change.get('STOCK_NAME'),
                AnnualReports.EditShareholdingChangeInfos.BEFORE_CONTENT: edit_shareholding_change.get('CHANGE_BEFORE'),
                AnnualReports.EditShareholdingChangeInfos.AFTER_CONTENT: edit_shareholding_change.get('CHANGE_AFTER'),
                AnnualReports.EditShareholdingChangeInfos.CHANGE_DATE: edit_shareholding_change.get('CHANGE_DATE')
            }
            change_model = bu_ding(change_model)
            lst.append(change_model)
        return lst

    # 年报 修改记录
    @staticmethod
    def get_annual_edit_change(edit_change_info):
        edit_change_data = edit_change_info.get('data', [])
        lst = []
        for edit_change in edit_change_data:
            edit_model = {
                AnnualReports.EditChangeInfos.CHANGE_ITEM: edit_change.get('CHANGE_ITEM_NAME'),
                AnnualReports.EditChangeInfos.BEFORE_CONTENT: edit_change.get('OLD_CONTENT'),
                AnnualReports.EditChangeInfos.AFTER_CONTENT: edit_change.get('NEW_CONTENT'),
                AnnualReports.EditChangeInfos.CHANGE_DATE: edit_change.get('CHANGE_DATE')
            }
            edit_model = bu_ding(edit_model)
            lst.append(edit_model)
        return lst

    def get_result(self):
        return self.annual_info_dict


def bu_ding(temp_model):
    for k, v in temp_model.items():
        if v is None:
            v = ''
            temp_model[k] = v
    return temp_model
