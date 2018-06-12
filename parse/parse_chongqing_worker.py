# !/usr/bin/env python
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


class GsxtParseChongQingWorker(ParseBaseWorker):
    def __init__(self, **kwargs):
        ParseBaseWorker.__init__(self, **kwargs)
        # 必须反馈抓取情况到种子列表
        self.report_status = self.REPORT_SEED

    # 基本信息
    def get_base_info(self, base_info):
        new_base_info_dict = {}
        page_list = util.json_loads(self.get_crawl_page(base_info))
        if page_list is None:
            return new_base_info_dict

        for page in page_list:
            form = page.get('form') if 'form' in page.keys() else page.get('form1')
            base_info_dict = dict()
            base_info_dict[unicode(form.get('title').replace('：', ''))] = form.get('uniscid')
            base_info_dict[u'企业名称'] = form.get('entname') if 'entname' in form.keys() else form.get(
                'traname')
            base_info_dict[u'类型'] = form.get('enttype_cn')
            base_info_dict[unicode(form.get('name_title').replace(':', ''))] = form.get('name')
            base_info_dict[u'成立日期'] = form.get('estdate')
            base_info_dict[u'经营期限自'] = form.get('opfrom')
            base_info_dict[u'经营期限至'] = form.get('opto')
            base_info_dict[u'登记机关'] = form.get('regorg_cn')
            base_info_dict[u'核准日期'] = form.get('apprdate')
            base_info_dict[u'登记状态'] = form.get('regstate_cn')
            base_info_dict[u'住所'] = form.get('dom')
            base_info_dict[u'经营范围'] = form.get('opscope')
            if 'compform_cn' in form.keys():
                base_info_dict[u'组成形式'] = form.get('compform_cn')
            if form.get('regcap') is not None:
                base_info_dict[u'注册资本'] = u'{0}万{1}'.format(form.get('regcap'), form.get('regcapcur_cn'))

            base_info_dict = self.replace_none(base_info_dict)

            for k, v in base_info_dict.items():
                new_k = GsModel.format_base_model(k)
                new_base_info_dict[new_k] = v

            new_base_info_dict[GsModel.PERIOD] = u"{0}至{1}". \
                format(new_base_info_dict.get(GsModel.PERIOD_FROM), new_base_info_dict.get(GsModel.PERIOD_TO))
        return new_base_info_dict

    # 股东信息
    def get_shareholder_info(self, shareholder_info):
        shareholder_info_dict = {}
        shareholder_list = []
        page_items = self.get_crawl_page(shareholder_info, True)
        for page in page_items:
            text = page.get('text')
            page_lists = json.loads(text)
            for page_list in page_lists:
                page_data_list = page_list.get('list', [])
                for data in page_data_list:
                    if data is None or len(data) == 0:
                        continue

                    share_model = {
                        GsModel.ShareholderInformation.SHAREHOLDER_NAME: data.get('inv'),
                        GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT:
                            util.get_amount_with_unit(data.get('lisubconam')),
                        GsModel.ShareholderInformation.PAIED_AMOUNT:
                            util.get_amount_with_unit(data.get('liacconam')),
                    }
                    for sub_and_paid_detail in data.get('subList'):
                        sub_dict = {
                            GsModel.ShareholderInformation.SUBSCRIPTION_TYPE: sub_and_paid_detail.get('e_conform_cn'),
                            GsModel.ShareholderInformation.SUBSCRIPTION_TIME: sub_and_paid_detail.get('e_condate'),
                            GsModel.ShareholderInformation.SUBSCRIPTION_PUBLISH_TIME:
                                sub_and_paid_detail.get('e_publicdate')
                        }

                        sub_dict = self.replace_none(sub_dict)
                        share_model[GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL] = [sub_dict]
                        paid_dict = {
                            GsModel.ShareholderInformation.PAIED_TYPE:
                                sub_and_paid_detail.get('p_conform_cn'),
                            GsModel.ShareholderInformation.PAIED_TIME:
                                sub_and_paid_detail.get('p_condate'),
                            GsModel.ShareholderInformation.PAIED_PUBLISH_TIME:
                                sub_and_paid_detail.get('p_publicdate')
                        }

                        paid_dict = self.replace_none(paid_dict)
                        share_model[GsModel.ShareholderInformation.PAIED_DETAIL] = [paid_dict]
                    share_model = self.replace_none(share_model)
                    shareholder_list.append(share_model)

        if len(shareholder_list) > 0:
            shareholder_info_dict[GsModel.SHAREHOLDER_INFORMATION] = shareholder_list
        return shareholder_info_dict

    # 变更信息
    def get_change_info(self, change_info):
        change_info_dict = {}
        change_records_list = []
        page_items = self.get_crawl_page(change_info, True)
        for page in page_items:
            text = page.get('text')
            page_lists = json.loads(text)
            for page_list in page_lists:
                page_data_list = page_list.get('list', [])
                for pl in page_data_list:
                    change_model = {
                        GsModel.ChangeRecords.CHANGE_ITEM: pl.get('altitem_cn'),
                        # 去除多余的字
                        GsModel.ChangeRecords.BEFORE_CONTENT: util.format_content(pl.get('altbe')),
                        GsModel.ChangeRecords.AFTER_CONTENT: util.format_content(pl.get('altaf')),
                        # 日期格式化
                        GsModel.ChangeRecords.CHANGE_DATE: pl.get('altdate')
                    }
                    change_model = self.replace_none(change_model)
                    change_records_list.append(change_model)

        if len(change_records_list) > 0:
            change_info_dict[GsModel.CHANGERECORDS] = change_records_list

        return change_info_dict

    # 主要人员
    def get_key_person_info(self, key_person_info):
        key_person_info_dict = {}
        key_person_list = []
        page_items = json.loads(self.get_crawl_page(key_person_info))
        for page in page_items:
            page_lists = page.get('list', [])
            for page_list in page_lists:
                for pl in page_list:
                    key_person = {
                        GsModel.KeyPerson.KEY_PERSON_NAME: pl.get('name'),
                        GsModel.KeyPerson.KEY_PERSON_POSITION: pl.get('position_cn')
                    }
                    key_person = self.replace_none(key_person)
                    key_person_list.append(key_person)
        if len(key_person_list) > 0:
            key_person_info_dict[GsModel.KEY_PERSON] = key_person_list
        return key_person_info_dict

    # 分支机构
    def get_branch_info(self, branch_info):
        branch_info_dict = {}
        lst_branch = []
        page_items = json.loads(self.get_crawl_page(branch_info))
        for page in page_items:
            page_lists = page.get('list', [])
            for page_list in page_lists:
                for pl in page_list:
                    branch_model = {
                        GsModel.Branch.COMPAY_NAME: pl.get('brname'),
                        GsModel.Branch.CODE: page.get('uniscid'),
                        GsModel.Branch.REGISTERED_ADDRESS: page.get('regorg_cn')
                    }
                    branch_model = self.replace_none(branch_model)
                    lst_branch.append(branch_model)
        if len(lst_branch) > 0:
            branch_info_dict[GsModel.BRANCH] = lst_branch
        return branch_info_dict

    # 出资信息
    def get_contributive_info(self, con_info):
        con_info_dict = {}
        part_a_con = {}
        part_b_con = {}
        pages_list = self.get_crawl_page(con_info, True)

        for pages in pages_list:
            pages = json.loads(pages.get('text'))
            for page in pages:
                data_list = page.get('list', [])
                for data in data_list:
                    sub_model = {
                        GsModel.ContributorInformation.SHAREHOLDER_NAME: data.get('inv'),
                        GsModel.ContributorInformation.SHAREHOLDER_TYPE: data.get('invtype_cn'),
                        GsModel.ContributorInformation.CERTIFICATE_TYPE: data.get('blictype_cn'),
                        GsModel.ContributorInformation.CERTIFICATE_NO: data.get('blicno')
                    }
                    sub_model = self.replace_none(sub_model)
                    part_a_con[sub_model[GsModel.ContributorInformation.SHAREHOLDER_NAME]] = sub_model

        pages_detail = self.get_crawl_page(con_info, True, Model.type_detail)
        if pages_detail is not None:

            # 每3项合为一项,每一项有相同页面的股东信息、认缴、实缴
            new_detail = []
            detail_num = len(pages_detail) / 3
            for i in xrange(detail_num):
                lst_detail = []
                for page in pages_detail:
                    page_url = page.get('url')
                    page_id = page_url.split('?')[0].split('/')[-1]
                    if len(lst_detail) == 0:
                        lst_detail.append(page)
                    elif page_id == lst_detail[0].get('url').split('?')[0].split('/')[-1]:
                        lst_detail.append(page)
                    elif len(lst_detail) == 3:
                        continue

                for done_page in lst_detail:
                    pages_detail.remove(done_page)

                new_detail.append(lst_detail)

            for pages in new_detail:
                shareholder_name, sub_model = self.get_share_hold_detail(pages)
                sub_model = self.replace_none(sub_model)
                part_b_con[shareholder_name] = sub_model

        con_list = []
        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            con_list.append(v_list)

        if len(con_list) > 0:
            con_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = con_list
        return con_info_dict

    # 从json中获取出资信息的股东信息详情页
    def get_share_hold_detail(self, pages):
        shareholder_name = ""
        sub_model = {}
        if pages is None:
            return shareholder_name, sub_model

        for page in pages:
            page_text = page.get('text')
            items = json.loads(page_text)
            for item in items:
                if 'form' in item.keys():
                    form = item.get('form')
                    shareholder_name = form.get('inv')
                    sub_model[GsModel.ContributorInformation.SHAREHOLDER_NAME] = form.get('inv')
                    sub_model[GsModel.ContributorInformation.SUBSCRIPTION_AMOUNT] = util.get_amount_with_unit(
                        form.get('lisubconam'))
                    sub_model[GsModel.ContributorInformation.PAIED_AMOUNT] = util.get_amount_with_unit(
                        form.get('liacconam'))

                elif 'subconam' in page_text:
                    lst_sub_details = []
                    lst_sub_list = item.get('list')
                    for sub_item in lst_sub_list:
                        sub_model_detail = {
                            GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TYPE: sub_item.get(
                                'conform_cn'),
                            GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                                sub_item.get('subconam')),
                            GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TIME: sub_item.get('condate')
                        }
                        sub_model_detail = self.replace_none(sub_model_detail)
                        lst_sub_details.append(sub_model_detail)
                    sub_model[GsModel.ContributorInformation.SUBSCRIPTION_DETAIL] = lst_sub_details

                elif 'acconam' in page_text:
                    lst_paid_details = []
                    lst_paid_list = item.get('list')
                    for paid_item in lst_paid_list:
                        paid_model_detail = {
                            GsModel.ContributorInformation.PaiedDetail.PAIED_TYPE: paid_item.get('conform_cn'),
                            GsModel.ContributorInformation.PaiedDetail.PAIED_AMOUNT: util.get_amount_with_unit(
                                paid_item.get('acconam')),
                            GsModel.ContributorInformation.PaiedDetail.PAIED_TIME: paid_item.get('condate')
                        }
                        paid_model_detail = self.replace_none(paid_model_detail)
                        lst_paid_details.append(paid_model_detail)
                    sub_model[GsModel.ContributorInformation.PAIED_DETAIL] = lst_paid_details
        sub_model = self.replace_none(sub_model)
        return shareholder_name, sub_model

    @staticmethod
    def replace_none(item_dict):
        for k, v in item_dict.items():
            if v is None:
                item_dict[k] = ''
        return item_dict

    # 清算信息
    def get_liquidation_info(self, liquidation_info):
        return {}

    # 年报信息
    def get_annual_info(self, annual_item_list):
        return ParseChongQingAnnual(annual_item_list, self.log).get_result()


class ParseChongQingAnnual(object):
    def __init__(self, annual_item_list, log):
        self.annual_info_dict = {}
        if not isinstance(annual_item_list, list) or len(annual_item_list) <= 0:
            return

        self.log = log
        self.annual_item_list = annual_item_list

        # 分发解析
        self.dispatch()

    # 年报信息
    def dispatch(self):
        if self.annual_item_list is None:
            raise IndexError("未抓取到相关网页,或者抓取网页失败")

        if len(self.annual_item_list) <= 0:
            return {}

        dict_annual = {}
        for lst_annual in self.annual_item_list:
            if 'baseinfo' in lst_annual.get('url'):
                dict_annual['baseinfo'] = util.json_loads(lst_annual.get('text'))
            elif 'websiteinfo' in lst_annual.get('url'):
                dict_annual['websiteinfo'] = util.json_loads(lst_annual.get('text'))
            elif 'subcapital' in lst_annual.get('url'):
                dict_annual['subcapital'] = util.json_loads(lst_annual.get('text'))
            elif 'forinvestment' in lst_annual.get('url'):
                dict_annual['forinvestment'] = util.json_loads(lst_annual.get('text'))
            elif 'forguaranteeinfo' in lst_annual.get('url'):
                dict_annual['forguaranteeinfo'] = util.json_loads(lst_annual.get('text'))
            elif 'alterstockinfo' in lst_annual.get('url'):
                dict_annual['alterstockinfo'] = util.json_loads(lst_annual.get('text'))
            elif 'updateinfo' in lst_annual.get('url'):
                dict_annual['updateinfo'] = util.json_loads(lst_annual.get('text'))

        # 基本信息
        base_info = dict_annual.get('baseinfo')
        if base_info is not None:
            annual_base_info = self.get_annual_base_info(base_info)
            self.annual_info_dict.update(annual_base_info)

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

        # 年报 企业资产状况信息
        asset_info = dict_annual.get('baseinfo')
        if asset_info is not None:
            asset_model = self.get_annual_asset_info(asset_info)
            self.annual_info_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = asset_model

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
    def get_annual_base_info(self, info):
        base_form = info[0].get('form')
        annual_base_info_dict = {
            AnnualReports.CODE: base_form.get('uniscid') if 'uniscid' in base_form.keys() else base_form.get('regno'),
            # 库中两条记录都有,网页只显示一个
            AnnualReports.COMPANY_NAME: base_form.get('entname') if 'entname' in base_form.keys() else base_form.get(
                'farspeartname'),  # 农专社名称
            AnnualReports.ADDRESS: base_form.get('addr'),
            AnnualReports.ZIP_CODE: base_form.get('postalCode'),
            AnnualReports.CONTACT_NUMBER: base_form.get('tel'),
            AnnualReports.EMAIL: base_form.get('email'),
            AnnualReports.EMPLOYED_POPULATION: base_form.get(
                'empnum') if 'empnum' in base_form.keys() else base_form.get(
                'memnum'),  # 成员人数
            AnnualReports.BUSINESS_STATUS: base_form.get('busst_cn'),
            AnnualReports.IS_INVEST: u'是' if base_form.get('hasbrothers') == '1' else u'否',
            AnnualReports.IS_WEB: u'是' if base_form.get('haswebsite') == '1' else u'否',
            AnnualReports.IS_OUT_GUARANTEE: u'是' if base_form.get('hasexternalsecurity') == '1' else u'否',
            AnnualReports.IS_TRANSFER: u'是' if base_form.get('istransfer') == '1' else u'否',
            AnnualReports.EMPLOYED_POPULATION_WOMAN: base_form.get('womempnum'),
            AnnualReports.ENTERPRISE_HOLDING: base_form.get('holdingsmsg'),
            AnnualReports.BUSINESS_ACTIVITIES: base_form.get('mainBusiact'),
            AnnualReports.SUPER_COMPANY: base_form.get('dependentEntName'),
            AnnualReports.SUPER_CODE: base_form.get('dependentEntUniscId'),
        }
        if 'memnum' in base_form.keys():
            annual_base_info_dict[AnnualReports.EMPLOYED_POPULATION_FARMER] = base_form.get(
                'memnum') if 'memnum' in base_form.keys() else base_form.get('sfcempnum')
        if 'farnum' in base_form.keys():
            annual_base_info_dict[AnnualReports.EMPLOYED_POPULATION_FARMER] = base_form.get('farnum')
        if 'annnewmemnum' in base_form.keys():
            annual_base_info_dict[AnnualReports.EMPLOYED_POPULATION_INCREASED] = base_form.get('annnewmemnum')
        if 'annredmemnum' in base_form.keys():
            annual_base_info_dict[AnnualReports.EMPLOYED_POPULATION_QUIT] = base_form.get('annredmemnum')
        if 'ownername' in base_form.keys():
            annual_base_info_dict[AnnualReports.LEGAL_MAN] = base_form.get('ownername')
        annual_base_info_dict = self.replace_none(annual_base_info_dict)
        return annual_base_info_dict

    # 年报 企业资产状况信息
    def get_annual_asset_info(self, info):
        base_form = info[0].get('form')
        if 'priyeasub' in base_form.keys():
            annual_asset_info_dict = {
                AnnualReports.EnterpriseAssetStatusInformation.GROSS_SALES: util.get_amount_with_unit(
                    base_form.get('priyeasales')),  # 销售额或营业收入
                AnnualReports.EnterpriseAssetStatusInformation.RETAINED_PROFITS: util.get_amount_with_unit(
                    base_form.get('priyeaprofit')),  # 盈余总额
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_INDEBTEDNESS: util.get_amount_with_unit(
                    base_form.get('priyealoan')),  # 金融贷款
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_TAX: util.get_amount_with_unit(
                    base_form.get('ratgro')),  # 纳税总额
                AnnualReports.EnterpriseAssetStatusInformation.FUND_SUBSIDY: util.get_amount_with_unit(
                    base_form.get('priyeasub'))  # 获得政府扶持资金、补助
            }
        else:
            annual_asset_info_dict = {
                AnnualReports.EnterpriseAssetStatusInformation.GENERAL_ASSETS: util.get_amount_with_unit(
                    base_form.get('assgro')),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_EQUITY: util.get_amount_with_unit(
                    base_form.get('totequ')),
                AnnualReports.EnterpriseAssetStatusInformation.GROSS_SALES: util.get_amount_with_unit(
                    base_form.get('vendinc')),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_PROFIT: util.get_amount_with_unit(
                    base_form.get('progro')),
                AnnualReports.EnterpriseAssetStatusInformation.INCOME_OF_TOTAL: util.get_amount_with_unit(
                    base_form.get('maibusinc')),
                AnnualReports.EnterpriseAssetStatusInformation.RETAINED_PROFITS: util.get_amount_with_unit(
                    base_form.get('netinc')),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_TAX: util.get_amount_with_unit(
                    base_form.get('ratgro')),
                AnnualReports.EnterpriseAssetStatusInformation.TOTAL_INDEBTEDNESS: util.get_amount_with_unit(
                    base_form.get('liagro'))
            }
        annual_asset_info_dict = self.replace_none(annual_asset_info_dict)
        return annual_asset_info_dict

    # 年报网站信息
    def get_annual_web_site_info(self, web_info):
        web_data = web_info[0].get('list', [])
        lst_websites = []
        for web_data in web_data:
            for web in web_data:
                web_item = {
                    AnnualReports.WebSites.NAME: web.get('websitname'),
                    AnnualReports.WebSites.TYPE: web.get('webtype'),
                    AnnualReports.WebSites.SITE: web.get('website')
                }
                web_item = self.replace_none(web_item)
                lst_websites.append(web_item)
        return lst_websites

    # 年报 股东出资信息(客户端分页)
    def get_annual_share_hold_info(self, shareholder_info):
        s_info = shareholder_info[0].get('list', [])
        lst = []
        for s in s_info:
            share_model = {
                AnnualReports.ShareholderInformation.SHAREHOLDER_NAME: s.get('invname'),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                    s.get('lisubconam')),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TIME: s.get('subcondate'),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TYPE: s.get('subconform_cn'),
                AnnualReports.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(s.get('liacconam')),
                AnnualReports.ShareholderInformation.PAIED_TIME: s.get('accondate'),
                AnnualReports.ShareholderInformation.PAIED_TYPE: s.get('acconform_cn')
            }
            share_model = self.replace_none(share_model)
            lst.append(share_model)
        return lst

    # 年报 对外投资信息
    def get_annual_inv_info(self, inv_info):
        inv_data = inv_info[0].get('list', [])
        lst_inv = []
        for inv_data in inv_data:
            for inv in inv_data:
                inv_item = {
                    AnnualReports.InvestedCompanies.COMPANY_NAME: inv.get('entname'),
                    AnnualReports.InvestedCompanies.CODE: inv.get('uniscid'),
                }
                inv_item = self.replace_none(inv_item)
                lst_inv.append(inv_item)
        return lst_inv

    # 年报 对外担保方法
    def get_annual_out_guarantee_info(self, out_guaranty_info):
        out_guaranty_data = out_guaranty_info[0].get('list', [])
        lst = []
        for out_guaranty in out_guaranty_data:
            out_guarantee_model = {
                AnnualReports.OutGuaranteeInfo.CREDITOR: out_guaranty.get('more'),
                AnnualReports.OutGuaranteeInfo.OBLIGOR: out_guaranty.get('mortgagor'),
                AnnualReports.OutGuaranteeInfo.DEBT_TYPE: out_guaranty.get('priclaseckind'),
                AnnualReports.OutGuaranteeInfo.DEBT_AMOUNT: out_guaranty.get('priClaSecAm'),
                AnnualReports.OutGuaranteeInfo.PERFORMANCE_PERIOD: out_guaranty.get('pefper'),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_PERIOD: out_guaranty.get('guaranperiod'),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_TYPE: out_guaranty.get('gatype')
            }
            out_guarantee_model = self.replace_none(out_guarantee_model)
            lst.append(out_guarantee_model)
        return lst

    # 年报 股权变更方法
    def get_annual_edit_shareholding_change(self, edit_shareholding_change_info):
        edit_shareholding_change_data = edit_shareholding_change_info[0].get('list', [])
        lst = []
        for edit_shareholding_change in edit_shareholding_change_data:
            change_model = {
                AnnualReports.EditShareholdingChangeInfos.SHAREHOLDER_NAME: edit_shareholding_change.get('inv'),
                AnnualReports.EditShareholdingChangeInfos.BEFORE_CONTENT: edit_shareholding_change.get('transampr'),
                AnnualReports.EditShareholdingChangeInfos.AFTER_CONTENT: edit_shareholding_change.get('transamaft'),
                AnnualReports.EditShareholdingChangeInfos.CHANGE_DATE: edit_shareholding_change.get('altdate')
            }
            change_model = self.replace_none(change_model)
            lst.append(change_model)
        return lst

    # 年报 修改记录
    def get_annual_edit_change(self, edit_change_info):
        edit_change_data = edit_change_info[0].get('list', [])
        lst = []
        for edit_change in edit_change_data:
            edit_model = {
                AnnualReports.EditChangeInfos.CHANGE_ITEM: edit_change.get('alitem'),
                AnnualReports.EditChangeInfos.BEFORE_CONTENT: edit_change.get('altbe'),
                AnnualReports.EditChangeInfos.AFTER_CONTENT: edit_change.get('altaf'),
                AnnualReports.EditChangeInfos.CHANGE_DATE: edit_change.get('altdate')
            }
            edit_model = self.replace_none(edit_model)
            lst.append(edit_model)
        return lst

    def get_result(self):
        return self.annual_info_dict

    @staticmethod
    def replace_none(item_dict):
        for k, v in item_dict.items():
            if v is None:
                item_dict[k] = ''
        return item_dict
