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
from common import util
from common.annual_field import *
from common.global_field import Model
from common.gsxt_field import *


class GsxtParseJiLinWorker(ParseBaseWorker):
    def __init__(self, **kwargs):
        ParseBaseWorker.__init__(self, **kwargs)

    # 基本信息
    def get_base_info(self, base_info):
        base_info_dict = {}
        new_base_info_dict = {}
        page = self.get_crawl_page(base_info)
        json_data = util.json_loads(page)
        if json_data is None:
            return {}

        base_info_dict[unicode(json_data.get('uniscIdName', ''))] = json_data.get('uniscId')
        base_info_dict[u'企业名称'] = json_data.get('entName')
        base_info_dict[u'类型'] = json_data.get('entType_CN')
        base_info_dict[u'法定代表人'] = json_data.get('leRep')
        base_info_dict[u'注册资本'] = json_data.get('regCap')
        base_info_dict[u'成立日期'] = json_data.get('estDate')
        base_info_dict[u'经营期限自'] = json_data.get('opFrom')
        base_info_dict[u'经营期限至'] = json_data.get('opTo')
        base_info_dict[u'登记机关'] = json_data.get('regOrg_CN')
        base_info_dict[u'核准日期'] = json_data.get('apprDate')
        base_info_dict[u'登记状态'] = json_data.get('regState_CN')
        base_info_dict[u'住所'] = json_data.get('dom')
        base_info_dict[u'经营范围'] = json_data.get('opScope')
        if 'compForm_CN' in json_data.keys():
            base_info_dict[u'组成形式'] = json_data.get('compForm_CN')
        for k, v in base_info_dict.items():
            if v is None:
                v = ''
            new_k = GsModel.format_base_model(k)
            new_base_info_dict[new_k] = v
        new_base_info_dict[GsModel.PERIOD] = u"{0}至{1}". \
            format(new_base_info_dict.get(GsModel.PERIOD_FROM), new_base_info_dict.get(GsModel.PERIOD_TO))
        return new_base_info_dict

    # 股东信息
    def get_shareholder_info(self, shareholder_info):
        shareholder_info_dict = {}
        pages = self.get_crawl_page(shareholder_info, True)
        lst_shareholder = []
        for page in pages:
            text = page.get('text')
            json_data = util.json_loads(text)
            if json_data is None:
                continue

            data_list = json_data.get('data', [])
            if data_list is None:
                continue

            for data in data_list:
                sub_dict = {
                    GsModel.ShareholderInformation.SUBSCRIPTION_TYPE: data.get('subConForm_CN'),
                    GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT: data.get('subConAm'),
                    GsModel.ShareholderInformation.SUBSCRIPTION_TIME: data.get('currency'),
                    GsModel.ShareholderInformation.SUBSCRIPTION_PUBLISH_TIME: data.get('shouldPublicDate')
                }
                sub_dict = replace_none(sub_dict)

                paid_dict = {
                    GsModel.ShareholderInformation.PAIED_TYPE: data.get('acConForm_CN'),
                    GsModel.ShareholderInformation.PAIED_AMOUNT: data.get('acConAm'),
                    GsModel.ShareholderInformation.PAIED_TIME: data.get('conDate'),
                    GsModel.ShareholderInformation.PAIED_PUBLISH_TIME: data.get('factPublicDate')
                }
                paid_dict = replace_none(paid_dict)

                share_model = {
                    GsModel.ShareholderInformation.SHAREHOLDER_NAME: data.get('inv'),
                    GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT: data.get('totalSubConAm'),
                    GsModel.ShareholderInformation.PAIED_AMOUNT: data.get('totalAcConAm'),

                    GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL: [sub_dict],
                    GsModel.ShareholderInformation.PAIED_DETAIL: [paid_dict]
                }
                share_model = replace_none(share_model)

                lst_shareholder.append(share_model)
        shareholder_info_dict[GsModel.SHAREHOLDER_INFORMATION] = lst_shareholder
        return shareholder_info_dict

    # 变更信息
    def get_change_info(self, change_info):
        change_info_dict = {}
        pages = self.get_crawl_page(change_info, True)
        lst_change_records = []
        for page in pages:
            text = page.get('text')
            json_data = util.json_loads(text)
            if json_data is None:
                continue

            data_list = json_data.get('data', [])
            if data_list is not None:
                for data in data_list:
                    change_model = {
                        GsModel.ChangeRecords.CHANGE_ITEM: data.get('altItem_CN'),
                        # 去除多余的字
                        GsModel.ChangeRecords.BEFORE_CONTENT: util.format_content(data.get('altBe')),
                        GsModel.ChangeRecords.AFTER_CONTENT: util.format_content(data.get('altAf')),
                        # 日期格式化
                        GsModel.ChangeRecords.CHANGE_DATE: data.get('altDate')
                    }
                    change_model = replace_none(change_model)

                    lst_change_records.append(change_model)
        change_info_dict[GsModel.CHANGERECORDS] = lst_change_records
        return change_info_dict

    # 主要人员
    def get_key_person_info(self, key_person_info):
        key_person_info_dict = {}
        pages = self.get_crawl_page(key_person_info)
        lst_key_person = []
        json_arr = util.json_loads(pages)
        if json_arr is None:
            return {}

        data_arr = json_arr.get('data', [])
        if data_arr is None:
            return {}

        for data in data_arr:
            key_person_model = {
                GsModel.KeyPerson.KEY_PERSON_NAME: data.get('name'),
                GsModel.KeyPerson.KEY_PERSON_POSITION: data.get('position_CN')
            }
            key_person_model = replace_none(key_person_model)
            lst_key_person.append(key_person_model)

        key_person_info_dict[GsModel.KEY_PERSON] = lst_key_person
        return key_person_info_dict

    # 分支机构
    def get_branch_info(self, branch_info):
        branch_info_dict = {}
        pages = self.get_crawl_page(branch_info)
        lst_branch = []
        json_arr = util.json_loads(pages)
        if json_arr is None:
            return {}

        data_arr = json_arr.get('data', [])
        if data_arr is None:
            return {}

        for data in data_arr:
            branch_model = {
                GsModel.Branch.COMPAY_NAME: data.get('barname'),
                GsModel.Branch.CODE: data.get('uniscId'),
                GsModel.Branch.REGISTERED_ADDRESS: data.get('regOrg_CN')
            }
            branch_model = replace_none(branch_model)
            lst_branch.append(branch_model)
        branch_info_dict[GsModel.BRANCH] = lst_branch
        return branch_info_dict

    # 出资信息
    def get_contributive_info(self, con_info):
        con_info_dict = {}
        part_b_con = {}
        part_a_con = {}
        pages_list = self.get_crawl_page(con_info, True)
        for page in pages_list:
            text = page.get('text')
            json_arr = util.json_loads(text)
            if json_arr is None:
                continue

            data_arr = json_arr.get('data', [])
            if data_arr is None:
                continue

            for data in data_arr:
                con_model = {
                    GsModel.ContributorInformation.SHAREHOLDER_NAME: data.get('inv'),
                    GsModel.ContributorInformation.SHAREHOLDER_TYPE: data.get('invType_CN'),
                    GsModel.ContributorInformation.CERTIFICATE_TYPE: data.get('blicType_CN'),
                    GsModel.ContributorInformation.CERTIFICATE_NO: data.get('blicNO')
                }
                con_model = replace_none(con_model)
                part_a_con[con_model[GsModel.ContributorInformation.SHAREHOLDER_NAME]] = con_model

        pages_detail = self.get_crawl_page(con_info, True, Model.type_detail)
        if pages_detail is not None:
            for page in pages_detail:
                shareholder_name, sub_model = self.get_share_hold_detail(page)
                part_b_con[shareholder_name] = sub_model
        lst_con = []
        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            lst_con.append(v_list)
        con_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = lst_con
        return con_info_dict

    # 从json中获取出资信息的股东信息详情页
    @staticmethod
    def get_share_hold_detail(pages):
        shareholder_name = ""
        sub_model = {}
        if pages is None:
            return shareholder_name, sub_model

        sub_model = {}
        items = util.json_loads(pages.get('text'))
        sub_model[GsModel.ContributorInformation.SHAREHOLDER_NAME] = items.get('inv')
        sub_model[GsModel.ContributorInformation.SUBSCRIPTION_AMOUNT] = items.get('liAcConAM')
        sub_model[GsModel.ContributorInformation.PAIED_AMOUNT] = items.get('liSubConAm')
        sub_model = replace_none(sub_model)

        shareholder_name = sub_model[GsModel.ContributorInformation.SHAREHOLDER_NAME]
        return shareholder_name, sub_model

    # 清算信息
    def get_liquidation_info(self, liquidation_info):
        return {}

    # 年报信息
    def get_annual_info(self, annual_item_list):
        return ParseJiLinAnnual(annual_item_list, self.log).get_result()


class ParseJiLinAnnual(object):
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

        dict_annual = {}
        for lst_annual in self.annual_item_list:
            if 'PubAnnualInfo/Annual' in lst_annual.get('url'):
                dict_annual['Annual'] = util.json_loads(lst_annual.get('text'))
            elif 'AnWebSites' in lst_annual.get('url'):
                dict_annual['AnWebSites'] = util.json_loads(lst_annual.get('text'))
            elif 'AnSubCapitals' in lst_annual.get('url'):
                dict_annual['AnSubCapitals'] = util.json_loads(lst_annual.get('text'))
            elif 'AnForInvestments' in lst_annual.get('url'):
                dict_annual['AnForInvestments'] = util.json_loads(lst_annual.get('text'))
            elif 'AnAsset' in lst_annual.get('url'):
                dict_annual['AnAsset'] = util.json_loads(lst_annual.get('text'))
            elif 'AnForGuarantees' in lst_annual.get('url'):
                dict_annual['AnForGuarantees'] = util.json_loads(lst_annual.get('text'))
            elif 'AnAlterStocks' in lst_annual.get('url'):
                dict_annual['AnAlterStocks'] = util.json_loads(lst_annual.get('text'))
            elif 'AnUpdates' in lst_annual.get('url'):
                dict_annual['AnUpdates'] = util.json_loads(lst_annual.get('text'))

        # 基本信息
        base_info = dict_annual.get('Annual')
        if base_info is not None:
            annual_base_info = self.get_annual_base_info(base_info)
            self.annual_info_dict.update(annual_base_info)

        # 网站或网店信息
        web_info = dict_annual.get('AnWebSites')
        if web_info is not None:
            lst_websites = self.get_annual_web_site_info(web_info)
            self.annual_info_dict[AnnualReports.WEBSITES] = lst_websites

        # 股东出资信息
        share_hold_info = dict_annual.get('AnSubCapitals')
        if share_hold_info is not None:
            lst_share_hold = self.get_annual_share_hold_info(share_hold_info)
            self.annual_info_dict[AnnualReports.SHAREHOLDER_INFORMATION] = lst_share_hold

        # 对外投资
        inv_info = dict_annual.get('AnForInvestments')
        if inv_info is not None:
            lst_inv = self.get_annual_inv_info(inv_info)
            self.annual_info_dict[AnnualReports.INVESTED_COMPANIES] = lst_inv

        # 年报 企业资产状况信息
        asset_info = dict_annual.get('AnAsset')
        if asset_info is not None:
            asset_model = self.get_annual_asset_info(asset_info)
            self.annual_info_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = asset_model

        # 对外担保
        out_guaranty_info = dict_annual.get('AnForGuarantees')
        if out_guaranty_info is not None:
            lst_out_guaranty = self.get_annual_out_guarantee_info(out_guaranty_info)
            self.annual_info_dict[AnnualReports.OUT_GUARANTEE_INFO] = lst_out_guaranty

        # 股权变更
        edit_shareholding_change_info = dict_annual.get('AnAlterStocks')
        if edit_shareholding_change_info is not None:
            lst_edit_shareholding_change = self.get_annual_edit_shareholding_change(edit_shareholding_change_info)
            self.annual_info_dict[AnnualReports.EDIT_SHAREHOLDING_CHANGE_INFOS] = lst_edit_shareholding_change

        # 修改记录
        edit_change_info = dict_annual.get('AnUpdates')
        if edit_change_info is not None:
            lst_edit_change = self.get_annual_edit_change(edit_change_info)
            self.annual_info_dict[AnnualReports.EDIT_CHANGE_INFOS] = lst_edit_change

    # 年报基本信息
    @staticmethod
    def get_annual_base_info(info):
        annual_base_info_dict = {
            AnnualReports.CODE: info.get('uniscId'),
            AnnualReports.COMPANY_NAME: info.get('entName'),
            AnnualReports.ADDRESS: info.get('addr'),
            AnnualReports.ZIP_CODE: info.get('postalCode'),
            AnnualReports.CONTACT_NUMBER: info.get('tel'),
            AnnualReports.EMAIL: info.get('email'),
            AnnualReports.EMPLOYED_POPULATION: info.get('empNum'),
            AnnualReports.BUSINESS_STATUS: info.get('busst_CN'),
            AnnualReports.IS_INVEST: info.get('hasForInvestment'),
            AnnualReports.IS_WEB: info.get('hasWebSite'),
            AnnualReports.IS_OUT_GUARANTEE: info.get('hasForguarantee'),
            AnnualReports.IS_TRANSFER: info.get('hasAlterStock'),
            AnnualReports.EMPLOYED_POPULATION_WOMAN: info.get('womEmpNum'),
            AnnualReports.EMPLOYED_POPULATION_FARMER: info.get('farNum'),
            AnnualReports.EMPLOYED_POPULATION_INCREASED: info.get('annNewMemNum'),
            AnnualReports.EMPLOYED_POPULATION_QUIT: info.get('annRedMemNum'),
            AnnualReports.ENTERPRISE_HOLDING: info.get('holdingsMsg'),
            AnnualReports.BUSINESS_ACTIVITIES: info.get('mainBusiact'),
            AnnualReports.SUPER_COMPANY: info.get('dependentEntName'),
            AnnualReports.SUPER_CODE: info.get('dependentEntUniscId'),
            AnnualReports.REGISTERED_CAPITAL: info.get('FundAm'),
            AnnualReports.LEGAL_MAN: info.get('ownerName'),
        }
        annual_base_info_dict = replace_none(annual_base_info_dict)
        return annual_base_info_dict

    # 年报 企业资产状况信息
    @staticmethod
    def get_annual_asset_info(info):
        if 'priYeaSub' in info.keys():
            annual_asset_info_dict = {
                AnnualReports.EnterpriseAssetStatusInformation.GROSS_SALES: info.get('maiBusInc'),  # 销售额或营业收入
                AnnualReports.EnterpriseAssetStatusInformation.RETAINED_PROFITS: info.get('priYeaProfit'),  # 盈余总额
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_INDEBTEDNESS: info.get('priYeaLoan'),  # 金融贷款
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_TAX: info.get('ratGro'),  # 纳税总额
                AnnualReports.EnterpriseAssetStatusInformation.FUND_SUBSIDY: info.get('priYeaSub')  # 获得政府扶持资金、补助
            }
        else:
            annual_asset_info_dict = {
                AnnualReports.EnterpriseAssetStatusInformation.GENERAL_ASSETS: info.get('assGro'),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_EQUITY: info.get('totEqu'),
                AnnualReports.EnterpriseAssetStatusInformation.GROSS_SALES: info.get('vendInc'),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_PROFIT: info.get('proGro'),
                AnnualReports.EnterpriseAssetStatusInformation.INCOME_OF_TOTAL: info.get('maiBusInc'),
                AnnualReports.EnterpriseAssetStatusInformation.RETAINED_PROFITS: info.get('netInc'),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_TAX: info.get('ratGro'),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_INDEBTEDNESS: info.get('liaGro')
            }
            annual_asset_info_dict = replace_none(annual_asset_info_dict)
        return annual_asset_info_dict

    # 年报网站信息
    @staticmethod
    def get_annual_web_site_info(web_info):
        web_data = web_info.get('data', [])
        lst_websites = []
        for web in web_data:
            web_item = {
                AnnualReports.WebSites.NAME: web.get('webSitName'),
                AnnualReports.WebSites.TYPE: web.get('webType'),
                AnnualReports.WebSites.SITE: web.get('domain')
            }
            web_item = replace_none(web_item)
            lst_websites.append(web_item)
        return lst_websites

    # 年报 股东出资信息(客户端分页)
    @staticmethod
    def get_annual_share_hold_info(shareholder_info):
        s_info = shareholder_info.get('data', [])
        lst = []
        for s in s_info:
            share_model = {
                AnnualReports.ShareholderInformation.SHAREHOLDER_NAME: s.get('invName'),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                    s.get('liSubConAm')),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TIME: s.get('subConDate'),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TYPE: s.get('subConForm_CN'),
                AnnualReports.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(s.get('liAcConAm')),
                AnnualReports.ShareholderInformation.PAIED_TIME: s.get('acConDate'),
                AnnualReports.ShareholderInformation.PAIED_TYPE: s.get('acConForm_CN'),
            }
            share_model = replace_none(share_model)
            lst.append(share_model)
        return lst

    # 年报 对外投资信息
    @staticmethod
    def get_annual_inv_info(inv_info):
        inv_data = inv_info.get('data', [])
        lst_inv = []
        for inv in inv_data:
            inv_item = {
                AnnualReports.InvestedCompanies.COMPANY_NAME: inv.get('entName'),
                AnnualReports.InvestedCompanies.CODE: inv.get('uniscId'),
            }
            inv_item = replace_none(inv_item)
            lst_inv.append(inv_item)
        return lst_inv

    # 年报 对外担保方法
    @staticmethod
    def get_annual_out_guarantee_info(out_guaranty_info):
        out_guaranty_data = out_guaranty_info.get('data', [])
        lst = []
        for out_guaranty in out_guaranty_data:
            out_guarantee_model = {
                AnnualReports.OutGuaranteeInfo.CREDITOR: out_guaranty.get('more'),
                AnnualReports.OutGuaranteeInfo.OBLIGOR: out_guaranty.get('mortgagor'),
                AnnualReports.OutGuaranteeInfo.DEBT_TYPE: out_guaranty.get('priClaSecKind'),
                AnnualReports.OutGuaranteeInfo.DEBT_AMOUNT: out_guaranty.get('priClaSecAm'),
                AnnualReports.OutGuaranteeInfo.PERFORMANCE_PERIOD: u'{0}-{1}'.format(out_guaranty.get('pefPerForm'),
                                                                                     out_guaranty.get('pefPerTo')),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_PERIOD: out_guaranty.get('guaranperiod'),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_TYPE: out_guaranty.get('gaType')
            }
            out_guarantee_model = replace_none(out_guarantee_model)
            lst.append(out_guarantee_model)
        return lst

    # 年报 股权变更方法
    @staticmethod
    def get_annual_edit_shareholding_change(edit_shareholding_change_info):
        edit_shareholding_change_data = edit_shareholding_change_info.get('data', [])
        lst = []
        for edit_shareholding_change in edit_shareholding_change_data:
            change_model = {
                AnnualReports.EditShareholdingChangeInfos.SHAREHOLDER_NAME: edit_shareholding_change.get('inv'),
                AnnualReports.EditShareholdingChangeInfos.BEFORE_CONTENT: edit_shareholding_change.get('transAmPrBf'),
                AnnualReports.EditShareholdingChangeInfos.AFTER_CONTENT: edit_shareholding_change.get('transAmPrAf'),
                AnnualReports.EditShareholdingChangeInfos.CHANGE_DATE: edit_shareholding_change.get('altDate')
            }
            change_model = replace_none(change_model)
            lst.append(change_model)
        return lst

    # 年报 修改记录
    @staticmethod
    def get_annual_edit_change(edit_change_info):
        edit_change_data = edit_change_info.get('data', [])
        lst = []
        for edit_change in edit_change_data:
            edit_model = {
                AnnualReports.EditChangeInfos.CHANGE_ITEM: edit_change.get('alitem'),
                AnnualReports.EditChangeInfos.BEFORE_CONTENT: edit_change.get('altBe'),
                AnnualReports.EditChangeInfos.AFTER_CONTENT: edit_change.get('altAf'),
                AnnualReports.EditChangeInfos.CHANGE_DATE: edit_change.get('altDate')
            }
            edit_model = replace_none(edit_model)
            lst.append(edit_model)
        return lst

    def get_result(self):
        return self.annual_info_dict


def replace_none(item_dict):
    for k, v in item_dict.items():
        if v is None:
            item_dict[k] = ''
    return item_dict
