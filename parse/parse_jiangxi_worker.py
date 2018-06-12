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


class GsxtParseJiangXiWorker(ParseBaseWorker):
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
        page = util.json_loads(page)
        if page is None:
            return {}

        base_info_dict = {}
        new_base_info_dict = {}
        base_info_dict[u'统一社会信用代码/注册号'] = page.get('UNISCID') if 'UNISCID' in page.keys() else page.get('REGNO')
        base_info_dict[u'企业名称'] = page.get('ENTNAME')
        base_info_dict[u'类型'] = page.get('ENTTYPE_CN')
        base_info_dict[unicode(page.get('namelike').replace('：', ''))] = page.get('NAME')
        base_info_dict[u'注册资本'] = u'{0}万元{1}'.format(
            page.get('REGCAP'), page.get('REGCAPCUR_CN'))
        base_info_dict[u'成立日期'] = page.get('ESTDATE')
        base_info_dict[u'经营期限自'] = page.get('OPFROM')
        base_info_dict[u'经营期限至'] = page.get('OPTO')
        base_info_dict[u'登记机关'] = page.get('REGORG_CN')
        base_info_dict[u'核准日期'] = page.get('APPRDATE')
        base_info_dict[u'登记状态'] = page.get('REGSTATE_CN')
        base_info_dict[u'住所'] = page.get('DOM')
        base_info_dict[u'经营范围'] = page.get('OPSCOPE')
        if 'COMPFORM_CN' in page.keys():
            base_info_dict[u'组成形式'] = page.get('COMPFORM_CN')

        for k, v in base_info_dict.items():
            if v is None:
                v = ''
            new_k = GsModel.format_base_model(k)
            new_base_info_dict[new_k] = v
        new_base_info_dict[GsModel.PERIOD] = u"{0}至{1}".format(
            new_base_info_dict.get(GsModel.PERIOD_FROM), new_base_info_dict.get(GsModel.PERIOD_TO))
        return new_base_info_dict

    # 股东信息
    def get_shareholder_info(self, shareholder_info):
        """
            :param shareholder_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
            其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
            具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
            股东信息一般存储在list列表中, 因为股东信息不包含列表结构不需要detail列表
            :return: 返回工商schema字典
            """
        shareholder_info_dict = {}
        pages = self.get_crawl_page(shareholder_info, True)

        lst_shareholder = []
        for page in pages:
            text = page.get('text')
            data_arr = util.json_loads(text).get('data', [])
            if data_arr is None:
                return {}

            for data in data_arr:
                if data is None or len(data) < 2:
                    continue

                share_model = {
                    GsModel.ShareholderInformation.SHAREHOLDER_NAME: data.get('INV'),
                    GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                        data.get('RJSSUM')),
                    GsModel.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(
                        data.get('SJSUM'))
                }
                lst_sub = []
                sjf_info = util.json_loads(data['SJFINFO'])
                if sjf_info is None:
                    continue

                for SubscriptionDetail in sjf_info:
                    if SubscriptionDetail is not None:
                        sub_detail = {
                            GsModel.ShareholderInformation.SUBSCRIPTION_TYPE: SubscriptionDetail.get('CONFORM_CN'),
                            GsModel.ShareholderInformation.SUBSCRIPTION_TIME: SubscriptionDetail.get('CONDATE'),
                            GsModel.ShareholderInformation.SUBSCRIPTION_PUBLISH_TIME: SubscriptionDetail.get(
                                'PUBLICDATE')
                        }
                        sub_detail = replace_none(sub_detail)
                        lst_sub.append(sub_detail)
                    share_model[GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL] = lst_sub

                paid_lst = []
                rjf_info = util.json_loads(data['RJFINFO'])
                if rjf_info is None:
                    continue

                for json_paid_detail in rjf_info:
                    if json_paid_detail is not None:
                        paid_detail = {
                            GsModel.ShareholderInformation.PAIED_TYPE: json_paid_detail.get('CONFORM_CN'),
                            GsModel.ShareholderInformation.PAIED_TIME: json_paid_detail.get('CONDATE'),
                            GsModel.ShareholderInformation.PAIED_PUBLISH_TIME: json_paid_detail.get('PUBLICDATE')
                        }
                        paid_detail = replace_none(paid_detail)
                        paid_lst.append(paid_detail)
                    share_model[GsModel.ShareholderInformation.PAIED_DETAIL] = paid_lst

                share_model = replace_none(share_model)
                lst_shareholder.append(share_model)
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
        pages = self.get_crawl_page(change_info, True)
        lst_change_records = []
        for page in pages:
            text = page.get('text')
            data_arr = util.json_loads(text).get('data', [])
            if data_arr is None:
                return {}

            for data in data_arr:
                change_model = {
                    GsModel.ChangeRecords.CHANGE_ITEM: data.get('ALTITEM_CN'),
                    # 去除多余的字
                    GsModel.ChangeRecords.BEFORE_CONTENT: util.format_content(data.get('ALTBE')),
                    GsModel.ChangeRecords.AFTER_CONTENT: util.format_content(data.get('ALTAF')),
                    # 日期格式化
                    GsModel.ChangeRecords.CHANGE_DATE: data.get('ALTDATE')
                }
                change_model = replace_none(change_model)
                lst_change_records.append(change_model)
        change_info_dict[GsModel.CHANGERECORDS] = lst_change_records
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
        pages = self.get_crawl_page(key_person_info)
        lst_key_person = []
        pages = util.json_loads(pages)
        if pages is None:
            return {}

        for page in pages:
            key_person_dict = {
                GsModel.KeyPerson.KEY_PERSON_NAME: page.get('NAME'),
                GsModel.KeyPerson.KEY_PERSON_POSITION: page.get('POSITION_CN')
            }
            key_person_dict = replace_none(key_person_dict)
            lst_key_person.append(key_person_dict)

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
        pages = self.get_crawl_page(branch_info)
        lst_branch = []
        pages = util.json_loads(pages)
        if pages is None:
            return {}

        for page in pages:
            branch_model = {
                GsModel.Branch.COMPAY_NAME: page.get('BRNAME'),
                GsModel.Branch.CODE: page.get('REGNO'),
                GsModel.Branch.REGISTERED_ADDRESS: page.get('REGORG_CN')
            }
            branch_model = replace_none(branch_model)
            lst_branch.append(branch_model)
        branch_info_dict[GsModel.BRANCH] = lst_branch
        return branch_info_dict

    # 出资信息
    def get_contributive_info(self, con_info):
        """
            :param con_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
            其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
            具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
            出资信息一般会由两个列表分别进行存储, 但部分省份也可能只包含list列表, 没有详情页信息
            :return: 返回工商schema字典
            """
        con_info_dict = {}
        pages_list = self.get_crawl_page(con_info, True)
        part_a_con = {}
        part_b_con = {}
        for page in pages_list:
            text = page.get('text')
            data = util.json_loads(text).get('data', [])
            if data is None:
                return {}

            for data in data:
                sub_model = {
                    GsModel.ContributorInformation.SHAREHOLDER_NAME: data.get('INV'),
                    GsModel.ContributorInformation.SHAREHOLDER_TYPE: data.get('INVTYPE_CN'),
                    GsModel.ContributorInformation.CERTIFICATE_TYPE: data.get('CERTYPE_CN'),
                    GsModel.ContributorInformation.CERTIFICATE_NO: data.get('CERNO')
                }
                sub_model = replace_none(sub_model)
                part_a_con[sub_model[GsModel.ContributorInformation.SHAREHOLDER_NAME]] = sub_model

        pages_detail = self.get_crawl_page(con_info, True, Model.type_detail)
        if pages_detail is not None:
            for page in pages_detail:
                tables = PyQuery(page.get('text'), parser='html').find('table').items()
                shareholder_name, sub_model = self._get_sharehold_detail(tables)
                part_b_con[shareholder_name] = sub_model
        lst_con = []
        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            lst_con.append(v_list)
        con_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = lst_con
        return con_info_dict

    # 清算信息
    def get_liquidation_info(self, liquidation_info):
        return {}

    # 年报信息
    def get_annual_info(self, annual_item_list):
        return ParseJiangXiAnnual(annual_item_list, self.log).get_result()


# 年报解析类
class ParseJiangXiAnnual(object):
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

        if self.annual_item_list is None or self.annual_item_list[0].get(u'status', u'fail') != u'success':
            raise IndexError("为抓取到相关网页,或者抓取网页失败")

        if not isinstance(self.annual_item_list, list) or len(self.annual_item_list) == 0:
            return {}

        dict_annual = {}
        for lst_annual in self.annual_item_list:
            if 'baseinfo' in lst_annual.get('url'):
                dict_annual['baseinfo'] = util.json_loads(lst_annual.get('text'))
            elif 'websiteinfo' in lst_annual.get('url'):
                dict_annual['websiteinfo'] = util.json_loads(lst_annual.get('text'))
            elif 'subcapital' in lst_annual.get('url'):
                dict_annual['subcapital'] = util.json_loads(lst_annual.get('text'))
            elif 'anforinvestment' in lst_annual.get('url'):
                dict_annual['forinvestment'] = util.json_loads(lst_annual.get('text'))
            elif 'forguaranteeinfo' in lst_annual.get('url'):
                dict_annual['forguaranteeinfo'] = util.json_loads(lst_annual.get('text'))
            elif 'alterstockinfo' in lst_annual.get('url'):
                dict_annual['alterstockinfo'] = util.json_loads(lst_annual.get('text'))
            elif 'updateinfo' in lst_annual.get('url'):
                dict_annual['updateinfo'] = util.json_loads(lst_annual.get('text'))
        # 基本信息
        base_info_data = dict_annual.get('baseinfo')
        if base_info_data is not None:
            for base_info_item in base_info_data:
                annual_base_info = self.get_annual_base_info(base_info_item)
                self.annual_info_dict.update(annual_base_info)

                # 年报 企业资产状况信息
                asset_model = self.get_annual_asset_info(base_info_item)
                self.annual_info_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = asset_model

        # 网站或网店信息
        web_info = dict_annual.get('websiteinfo')
        if web_info is not None:
            lst_websites = self.get_annual_web_site_info(web_info)
            self.annual_info_dict[AnnualReports.WEBSITES] = lst_websites

        # 股东出资信息
        share_hold_info = dict_annual.get('subcapital')
        if share_hold_info is not None:
            lst_share_hold = self.get_annual_share_hold_info(share_hold_info)
            self.annual_info_dict[AnnualReports.SHAREHOLDER_INFORMATION] = lst_share_hold

        # 对外投资
        inv_info = dict_annual.get('forinvestment')
        if inv_info is not None:
            lst_inv = self.get_annual_inv_info(inv_info)
            self.annual_info_dict[AnnualReports.INVESTED_COMPANIES] = lst_inv

        # 对外担保
        out_guaranty_info = dict_annual.get('forguaranteeinfo')
        if out_guaranty_info is not None:
            lst_out_guaranty = self.get_annual_out_guarantee_info(out_guaranty_info)
            self.annual_info_dict[AnnualReports.OUT_GUARANTEE_INFO] = lst_out_guaranty

        # 股权变更
        edit_shareholding_change_info = dict_annual.get('alterstockinfo')
        if edit_shareholding_change_info is not None:
            lst_edit_shareholding_change = self.get_annual_edit_shareholding_change(edit_shareholding_change_info)
            self.annual_info_dict[AnnualReports.EDIT_SHAREHOLDING_CHANGE_INFOS] = lst_edit_shareholding_change

        # 修改记录
        edit_change_info = dict_annual.get('updateinfo')
        if edit_change_info is not None:
            lst_edit_change = self.get_annual_edit_change(edit_change_info)
            self.annual_info_dict[AnnualReports.EDIT_CHANGE_INFOS] = lst_edit_change

    # 年报基本信息
    @staticmethod
    def get_annual_base_info(info):
        annual_base_info_dict = {}
        enterprise_holding = {
            '1': u'国有控股',
            '2': u'集体控股',
            '3': u'私人控股',
            '4': u'港澳台商控股',
            '5': u'外商控股',
            '6': u'其他'
        }
        annual_base_info_dict[AnnualReports.CODE] = info.get('REGNO')
        annual_base_info_dict[AnnualReports.COMPANY_NAME] = info.get(
            'ENTNAME') if 'ENTNAME' in info.keys() else info.get(
            'FARSPEARTNAME')
        annual_base_info_dict[AnnualReports.ADDRESS] = info.get('ADDR')
        annual_base_info_dict[AnnualReports.ZIP_CODE] = info.get('POSTALCODE')
        annual_base_info_dict[AnnualReports.CONTACT_NUMBER] = info.get('TEL')
        annual_base_info_dict[AnnualReports.EMAIL] = info.get('EMAIL')
        annual_base_info_dict[AnnualReports.EMPLOYED_POPULATION] = unicode(info.get('EMPNUM')) + u'人' if info.get(
            'EMPNUMDIS') == '1' else u'企业选择不公示'
        annual_base_info_dict[AnnualReports.BUSINESS_STATUS] = info.get('BUSST_CN')
        annual_base_info_dict[AnnualReports.IS_WEB] = u'是' if info.get('ISWEB') == '1' else u'否'
        annual_base_info_dict[AnnualReports.IS_TRANSFER] = u'是' if info.get('ISCHANGE') == '1' else u'否'
        annual_base_info_dict[AnnualReports.IS_INVEST] = u'是' if info.get('ISLETTER') == '1' else u'否'
        annual_base_info_dict[AnnualReports.IS_OUT_GUARANTEE] = u'是' if info.get('ISFORGUARANTEE') == '1' else u'否'

        if 'MEMNUM' in info.keys() and 'EMPNUM' not in info.keys():
            annual_base_info_dict[AnnualReports.EMPLOYED_POPULATION] = unicode(info.get('MEMNUM')) + u'人'
        if 'WOMEMPNUMIS' in info.keys():
            annual_base_info_dict[AnnualReports.EMPLOYED_POPULATION_WOMAN] = unicode(
                info.get('WOMEMPNUM')) + u'人' if info.get(
                'WOMEMPNUMIS') == '1' else u'企业选择不公示'
        if 'HOLDINGSMSGIS' in info.keys():
            annual_base_info_dict[AnnualReports.ENTERPRISE_HOLDING] = enterprise_holding.get(
                info.get('HOLDINGSMSG')) if info.get(
                'HOLDINGSMSGIS') == '1' else u'企业选择不公示'
        if 'FARNUM' in info.keys():
            annual_base_info_dict[AnnualReports.EMPLOYED_POPULATION_FARMER] = unicode(info.get('FARNUM')) + u'人'
        if 'ANNNEWMEMNUM' in info.keys():
            annual_base_info_dict[AnnualReports.EMPLOYED_POPULATION_INCREASED] = unicode(
                info.get('ANNNEWMEMNUM')) + u'人'
        if 'ANNREDMEMNUM' in info.keys():
            annual_base_info_dict[AnnualReports.EMPLOYED_POPULATION_QUIT] = unicode(info.get('ANNREDMEMNUM')) + u'人'
        annual_base_info_dict = replace_none(annual_base_info_dict)
        return annual_base_info_dict

    # 年报 企业资产状况信息 纳税总额 RATGRO  获得政府扶持资金、补助 PRIYEASUB  营业额或营业收入 PRIYEASALES  盈余总额 PRIYEAPROFIT  金融贷款 PRIYEALOAN
    @staticmethod
    def get_annual_asset_info(info):
        if 'PRIYEASUB' in info.keys():
            annual_asset_info_dict = {
                AnnualReports.EnterpriseAssetStatusInformation.GROSS_SALES: util.get_amount_with_unit(
                    info.get('PRIYEASALES')),
                AnnualReports.EnterpriseAssetStatusInformation.RETAINED_PROFITS: util.get_amount_with_unit(
                    info.get('PRIYEAPROFIT')),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_INDEBTEDNESS: util.get_amount_with_unit(
                    info.get('PRIYEALOAN')),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_TAX: util.get_amount_with_unit(info.get('RATGRO')),
                AnnualReports.EnterpriseAssetStatusInformation.FUND_SUBSIDY: util.get_amount_with_unit(
                    info.get('PRIYEASUB')),
            }
        else:
            annual_asset_info_dict = {
                AnnualReports.EnterpriseAssetStatusInformation.GENERAL_ASSETS: util.get_amount_with_unit(
                    info.get('ASSGRO')) if info.get('ASSGRODIS') == '1' else u'企业选择不公示',
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_EQUITY: util.get_amount_with_unit(
                    info.get('TOTEQU')) if info.get('TOTEQUDIS') == '1' else u'企业选择不公示',
                AnnualReports.EnterpriseAssetStatusInformation.GROSS_SALES: util.get_amount_with_unit(
                    info.get('VENDINC')) if info.get('VENDINCDIS') == '1' else u'企业选择不公示',
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_PROFIT: util.get_amount_with_unit(
                    info.get('PROGRO')) if info.get('PROGRODIS') == '1' else u'企业选择不公示',
                AnnualReports.EnterpriseAssetStatusInformation.INCOME_OF_TOTAL: util.get_amount_with_unit(
                    info.get('MAIBUSINC')) if info.get('MAIBUSINCDIS') == '1' else u'企业选择不公示',
                AnnualReports.EnterpriseAssetStatusInformation.RETAINED_PROFITS: util.get_amount_with_unit(
                    info.get('NETINC')) if info.get('NETINCDIS') == '1' else u'企业选择不公示',
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_TAX: util.get_amount_with_unit(
                    info.get('RATGRO')) if info.get('RATGRODIS') == '1' else u'企业选择不公示',
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_INDEBTEDNESS: util.get_amount_with_unit(
                    info.get('LIAGRO')) if info.get('LIAGRODIS') == '1' else u'企业选择不公示'
            }
            annual_asset_info_dict = replace_none(annual_asset_info_dict)
        return annual_asset_info_dict

    # 年报网站信息
    @staticmethod
    def get_annual_web_site_info(web_info):
        web_type = {
            '1': u'网站',
            '2': u'网店'
        }
        lst_websites = []
        for web in web_info:
            web_item = {
                AnnualReports.WebSites.NAME: web.get('WEBSITNAME'),
                AnnualReports.WebSites.TYPE: web_type.get(web.get('WEBTYPE')),
                AnnualReports.WebSites.SITE: web.get('DOMAIN')
            }
            web_item = replace_none(web_item)
            lst_websites.append(web_item)
        return lst_websites

    # 年报 股东出资信息(客户端分页)
    @staticmethod
    def get_annual_share_hold_info(shareholder_info):
        s_info = shareholder_info.get('data')
        lst = []
        for s in s_info:
            share_model = {
                AnnualReports.ShareholderInformation.SHAREHOLDER_NAME: s.get('INVNAME'),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                    s.get('LISUBCONAM')),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TIME: s.get('SUBCONDATE'),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TYPE: s.get('SUBCONFORM_CN'),
                AnnualReports.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(s.get('LIACCONAM')),
                AnnualReports.ShareholderInformation.PAIED_TIME: s.get('ACCONDATE'),
                AnnualReports.ShareholderInformation.PAIED_TYPE: s.get('ACCONFORM_CN'),
            }
            share_model = replace_none(share_model)
            lst.append(share_model)
        return lst

    # 年报 对外投资信息
    @staticmethod
    def get_annual_inv_info(inv_info):
        lst_inv = []
        for inv in inv_info:
            inv_item = {
                AnnualReports.InvestedCompanies.COMPANY_NAME: inv.get('ENTNAME'),
                AnnualReports.InvestedCompanies.CODE: inv.get('UNISCID'),
            }
            inv_item = replace_none(inv_item)
            lst_inv.append(inv_item)
        return lst_inv

    # 年报 对外担保方法
    # todo 目前没有企业信息可以确定out_guaranty_info的格式,暂时已两种可能去解析
    @staticmethod
    def get_annual_out_guarantee_info(out_guaranty_info):
        try:
            out_guaranty_data = out_guaranty_info.get('data')
        except AttributeError:
            out_guaranty_data = out_guaranty_info
        lst = []
        for out_guaranty in out_guaranty_data:
            out_guarantee_model = {
                AnnualReports.OutGuaranteeInfo.CREDITOR: out_guaranty.get('MORE'),
                AnnualReports.OutGuaranteeInfo.OBLIGOR: out_guaranty.get('MORTGAGOR'),
                AnnualReports.OutGuaranteeInfo.DEBT_TYPE: out_guaranty.get('PRICLASECKIND'),
                AnnualReports.OutGuaranteeInfo.DEBT_AMOUNT: out_guaranty.get('PRICLASECAM'),
                AnnualReports.OutGuaranteeInfo.PERFORMANCE_PERIOD: out_guaranty.get('PEFPER'),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_PERIOD: out_guaranty.get('GUARANPERIOD'),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_TYPE: out_guaranty.get('GATYPE')
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
                AnnualReports.EditShareholdingChangeInfos.SHAREHOLDER_NAME: edit_shareholding_change.get('INV'),
                AnnualReports.EditShareholdingChangeInfos.BEFORE_CONTENT: edit_shareholding_change.get('TRANSAMPR'),
                AnnualReports.EditShareholdingChangeInfos.AFTER_CONTENT: edit_shareholding_change.get('TRANSAMAFT'),
                AnnualReports.EditShareholdingChangeInfos.CHANGE_DATE: edit_shareholding_change.get('ALTDATE')
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
                AnnualReports.EditChangeInfos.CHANGE_ITEM: edit_change.get('AITEMS'),
                AnnualReports.EditChangeInfos.BEFORE_CONTENT: edit_change.get('ALTBE'),
                AnnualReports.EditChangeInfos.AFTER_CONTENT: edit_change.get('ALTAF'),
                AnnualReports.EditChangeInfos.CHANGE_DATE: edit_change.get('ALTDATE')
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
