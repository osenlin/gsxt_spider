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


class GsxtParseZheJiangWorker(ParseBaseWorker):
    def __init__(self, **kwargs):
        ParseBaseWorker.__init__(self, **kwargs)
        # 必须反馈抓取情况到种子列表
        self.report_status = self.REPORT_SEED

    # 基本信息
    def get_base_info(self, base_info):
        page = self.get_crawl_page(base_info)
        res = PyQuery(page, parser='html').find('.encounter-info.clearfix').find('li').items()
        base_info_dict = self.zj_get_base_item_info(res)
        return base_info_dict

    # 出资信息 基本信息下面的模块
    def get_contributive_info(self, con_info):
        con_info_dict = {}
        part_a_con = {}
        pages_list = self.get_crawl_page(con_info, True)

        dict_inv_type = {'1': u'企业法人', '2': u'自然人', '3': u'其他股东'}
        dict_certificate_type = {'1': u'法人营业执照', '3': u'其他'}
        dict_no_type = {'2': u'非公示项'}
        for page in pages_list:
            text = page.get('text')
            if text is None:
                continue
            con_data = json.loads(text).get('data', [])
            for data in con_data:
                share_model = {
                    GsModel.ContributorInformation.SHAREHOLDER_NAME: data.get('inv'),
                    GsModel.ContributorInformation.SHAREHOLDER_TYPE: dict_inv_type.get(data.get('invType')),
                    GsModel.ContributorInformation.CERTIFICATE_TYPE: data.get('cerTypeName') if data.get(
                        'invType') == '2'
                    else dict_certificate_type.get(data.get('invType')),
                    GsModel.ContributorInformation.CERTIFICATE_NO: data.get('bLicNO')
                    if data.get('invType') == '1' or data.get('invType') == '3' else dict_no_type.get(
                        data.get('invType'))
                }
                share_model = self.replace_none(share_model)
                part_a_con[share_model[GsModel.ContributorInformation.SHAREHOLDER_NAME]] = share_model

        part_b_con = {}
        pages_detail = self.get_crawl_page(con_info, True, Model.type_detail)
        if pages_detail is not None:
            for page in pages_detail:
                if page.get(u'status', u'') != u'success':
                    continue

                tables = PyQuery(page.get(u'text', u''), parser='html').find('.table-common').items()
                shareholder_name, sub_model = self.zj_get_share_hold_detail(tables)  # todo 出资信息的详情被注释掉了,待解决
                part_b_con[shareholder_name] = sub_model
        lst_con = []
        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            lst_con.append(v_list)
        con_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = lst_con
        return con_info_dict

        # 变更信息

    def get_change_info(self, change_info):
        change_info_dict = {}
        pages = self.get_crawl_page(change_info, True)
        lst_change_records = []
        for page in pages:
            text = page.get('text')
            if text is None:
                continue

            native_json = util.json_loads(text)
            if native_json is None:
                continue

            data_json_arr = native_json.get('data', [])
            if data_json_arr is None:
                continue

            for data in data_json_arr:
                change_model = {
                    GsModel.ChangeRecords.CHANGE_ITEM: data.get('altContent'),
                    # 去除多余的字
                    GsModel.ChangeRecords.BEFORE_CONTENT: util.format_content(data.get('altBeContent')),
                    GsModel.ChangeRecords.AFTER_CONTENT: util.format_content(data.get('altAfContent')),
                    GsModel.ChangeRecords.CHANGE_DATE: data.get('altDate')
                }
                change_model = self.replace_none(change_model)
                lst_change_records.append(change_model)
        change_info_dict[GsModel.CHANGERECORDS] = lst_change_records
        return change_info_dict

    # 主要人员
    def get_key_person_info(self, key_person_info):
        key_person_info_dict = {}
        page = self.get_crawl_page(key_person_info)
        lst_key_person = []
        json_data_arr = util.json_loads(page)
        if json_data_arr is None:
            return {}

        for item in json_data_arr:
            key_person = {
                GsModel.KeyPerson.KEY_PERSON_NAME: item.get('name'),
                GsModel.KeyPerson.KEY_PERSON_POSITION: item.get('posiContent')
            }
            key_person = self.replace_none(key_person)
            lst_key_person.append(key_person)

        key_person_info_dict[GsModel.KEY_PERSON] = lst_key_person
        return key_person_info_dict

        # 分支机构

    def get_branch_info(self, branch_info):
        branch_info_dict = {GsModel.BRANCH: None}
        page = self.get_crawl_page(branch_info)
        data_json_arr = util.json_loads(page)
        if data_json_arr is None:
            return {}

        lst_branch = []
        for item in data_json_arr:
            branch_model = {
                GsModel.Branch.COMPAY_NAME: item.get('entName'),
                GsModel.Branch.CODE: item.get('regNO')
            }
            branch_model = self.replace_none(branch_model)
            lst_branch.append(branch_model)
        branch_info_dict[GsModel.BRANCH] = lst_branch
        return branch_info_dict

        # 股东信息 靠下面的是股东信息  安徽省高速石化有限公司
        # done 日期要格式化

    def get_shareholder_info(self, shareholder_info):
        shareholder_info_dict = {}
        pages_list = self.get_crawl_page(shareholder_info, True)
        for page in pages_list:
            share_text = page.get('text', '')
            if share_text is None or share_text == u'':
                continue

            shareholder_info_dict = self.get_xml_shareholder_info(share_text)
        return shareholder_info_dict

    # 清算信息
    def get_liquidation_info(self, liquidation_info):
        return {}

    # ======================================================分割线=================================================
    # 工商 基本信息
    @staticmethod
    def zj_get_base_item_info(items):
        base_info_dict = {}
        for item in items:
            item_content = item.text().replace(u'•', u'').strip()
            if len(item_content) == 0:
                continue
            part = item_content.split(u'：', 1)
            k = GsModel.format_base_model(part[0].strip())
            base_info_dict[k] = part[1].strip()

            # if part[0].strip() == u'负责人' or part[0].strip() == u'法人代表' \
            #         or part[0].strip() == u'经营者' or part[0].strip() == u'执行事务合伙人' \
            #         or part[0].strip() == u'投资人' or part[0].strip() == u'法定代表' \
            #         or part[0].strip() == u'首席代表' or part[0].srip() == u'法定代表人':
            if k == GsModel.LEGAL_MAN:
                base_info_dict[GsModel.LEGAL_MAN_TYPE] = part[0].strip()

        base_info_dict[GsModel.PERIOD] = u"{0}至{1}".format(
            base_info_dict.get(GsModel.PERIOD_FROM, u''), base_info_dict.get(GsModel.PERIOD_TO, u''))
        return base_info_dict

    @staticmethod
    def zj_get_share_hold_detail(tables):
        shareholder_name = ""
        sub_model = {}
        if tables is None:
            return shareholder_name, sub_model

        for table in tables:
            th_text = table.text()
            if u'发起人' in th_text \
                    or u'股东名称' in th_text \
                    or u'股东及出资人名称' in th_text \
                    or u'股东' in th_text:
                tds = table.find('td')
                if len(tds) == 6:
                    sub_model[GsModel.ContributorInformation.SHAREHOLDER_NAME] = tds.eq(1).text().strip().replace(u'.',
                                                                                                                  u'')
                    sub_model[GsModel.ContributorInformation.SUBSCRIPTION_AMOUNT] = util.get_amount_with_unit(
                        tds.eq(3).text())
                    sub_model[GsModel.ContributorInformation.PAIED_AMOUNT] = util.get_amount_with_unit(
                        tds.eq(5).text())
                    shareholder_name = sub_model[GsModel.ContributorInformation.SHAREHOLDER_NAME]

            if u'认缴出资方式' in th_text:
                lst_sub_detail = []
                tds = table.find('td')
                if len(tds) == 6:
                    sub_model_detail = {
                        GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TYPE: tds.eq(3).text(),
                        GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                            tds.eq(1).text()),
                        GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TIME: tds.eq(5).text(),
                    }
                    lst_sub_detail.append(sub_model_detail)
                sub_model[GsModel.ContributorInformation.SUBSCRIPTION_DETAIL] = lst_sub_detail

            if u'实缴出资方式' in th_text:
                lst_paid_detail = []
                tds = table.find('td')
                if len(tds) == 3:
                    paid_model_detail = {
                        GsModel.ContributorInformation.PaiedDetail.PAIED_TYPE: tds.eq(0).text(),
                        GsModel.ContributorInformation.PaiedDetail.PAIED_AMOUNT: util.get_amount_with_unit(
                            tds.eq(1).text()),
                        GsModel.ContributorInformation.PaiedDetail.PAIED_TIME: tds.eq(2).text(),
                    }
                    lst_paid_detail.append(paid_model_detail)
                sub_model[GsModel.ContributorInformation.PAIED_DETAIL] = lst_paid_detail

        return shareholder_name, sub_model

    def get_xml_shareholder_info(self, share_xml):
        shareholder_info_dict = {}
        str_index = share_xml.find('<data>')
        if str_index < 0:
            return shareholder_info_dict

        shareholder_data = PyQuery(share_xml, parser='xml').find('data').find('data').items()
        lst_shareholder = []
        for data in shareholder_data:
            share_model = {
                GsModel.ShareholderInformation.SHAREHOLDER_NAME: data.find('inv').text(),
                GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                    data.find('liSubConAm').text()),
                GsModel.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(
                    data.find('liAcConAm').text()),
            }

            lst_sub = []
            sub_data = data.find('imInvprodetailList').find('imInvprodetailList').items()
            for sub_detail in sub_data:
                sub_dict = {
                    GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TYPE:
                        sub_detail.find('conFormCN').text(),
                    GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT:
                        util.get_amount_with_unit(sub_detail.find('subConAm').text()),
                    GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TIME:
                        sub_detail.find('conDate').text(),
                    GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_PUBLISH_TIME: sub_detail.find(
                        'publicDate').text()
                }
                sub_dict = self.replace_none(sub_dict)
                lst_sub.append(sub_dict)
            share_model[GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL] = lst_sub

            lst_paid = []
            paid_data = data.find('imInvactdetailList').find('imInvactdetailList').items()
            for paid_detail in paid_data:
                paid_dict = {
                    GsModel.ShareholderInformation.PaiedDetail.PAIED_TYPE: paid_detail.find('acConFormCn').text(),
                    GsModel.ShareholderInformation.PaiedDetail.PAIED_AMOUNT:
                        util.get_amount_with_unit(paid_detail.find('acConAm').text()),
                    GsModel.ShareholderInformation.PaiedDetail.PAIED_TIME: paid_detail.find('conDate').text(),
                    GsModel.ShareholderInformation.PaiedDetail.PAIED_PUBLISH_TIME:
                        paid_detail.find('publicDate').text(),
                }
                paid_dict = self.replace_none(paid_dict)
                lst_paid.append(paid_dict)
            share_model[GsModel.ShareholderInformation.PAIED_DETAIL] = lst_paid

            share_model = self.replace_none(share_model)
            lst_shareholder.append(share_model)
        shareholder_info_dict[GsModel.SHAREHOLDER_INFORMATION] = lst_shareholder
        return shareholder_info_dict

    @staticmethod
    def replace_none(item_dict):
        for k, v in item_dict.items():
            if v is None:
                item_dict[k] = ''
        return item_dict

    # 年报信息
    def get_annual_info(self, annual_item_list):
        return ParseZheJiangAnnual(annual_item_list, self.log).get_result()


class ParseZheJiangAnnual(object):
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
            url = lst_annual.get('url')
            if 'entinfo' in url:
                dict_annual['baseinfo'] = lst_annual.get('text')
            elif 'WebsiteInfo' in url:
                dict_annual['websiteinfo'] = util.json_loads(lst_annual.get('text'))
            elif 'subcapitalInfo' in url:
                dict_annual['subcapital'] = util.json_loads(lst_annual.get('text'))
            elif 'forinvestMentInfo' in url:
                dict_annual['forinvestment'] = util.json_loads(lst_annual.get('text'))
            elif 'GuaranteeInfo' in url:
                dict_annual['forguaranteeinfo'] = util.json_loads(lst_annual.get('text'))
            elif 'alterStockInfo' in url:
                dict_annual['alterstockinfo'] = util.json_loads(lst_annual.get('text'))
            elif 'updateinfo' in url:
                dict_annual['updateinfo'] = util.json_loads(lst_annual.get('text'))

        # 基本信息
        base_info = dict_annual.get('baseinfo')
        if base_info is not None:
            info = PyQuery(base_info, parser='html').find('.encounter-info')
            annual_base_info = self.zj_get_annual_base_info(info)
            self.annual_info_dict.update(annual_base_info)

        # 网站或网店信息
        web_info = dict_annual.get('websiteinfo')
        if web_info is not None:
            lst_websites = self.zj_get_annual_web_site_info(web_info)
            self.annual_info_dict[AnnualReports.WEBSITES] = lst_websites

        # 股东出资信息
        share_hold_info = dict_annual.get('subcapital')
        if share_hold_info is not None:
            lst_share_hold = self.zj_get_annual_share_hold_info(share_hold_info)
            self.annual_info_dict[AnnualReports.SHAREHOLDER_INFORMATION] = lst_share_hold

        # 对外投资
        inv_info = dict_annual.get('forinvestment')
        if inv_info is not None:
            lst_inv = self.zj_get_annual_inv_info(inv_info)
            self.annual_info_dict[AnnualReports.INVESTED_COMPANIES] = lst_inv

        # 年报 企业资产状况信息
        base_info = dict_annual.get('baseinfo')
        if base_info is not None:
            tds = PyQuery(base_info, parser='html').find('.table-zichan').not_('.table-td-pd').find('td')
            asset_model = self.zj_get_annual_asset_info(tds)
            self.annual_info_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = asset_model

        # 对外担保
        out_guaranty_info = dict_annual.get('forguaranteeinfo')
        if out_guaranty_info is not None:
            lst_out_guaranty = self.zj_get_annual_out_guarantee_info(out_guaranty_info)
            self.annual_info_dict[AnnualReports.OUT_GUARANTEE_INFO] = lst_out_guaranty

        # 股权变更
        edit_shareholding_change_info = dict_annual.get('alterstockinfo')
        if edit_shareholding_change_info is not None:
            lst_edit_shareholding_change = self.zj_get_annual_edit_shareholding_change(edit_shareholding_change_info)
            self.annual_info_dict[AnnualReports.EDIT_SHAREHOLDING_CHANGE_INFOS] = lst_edit_shareholding_change

        # 修改记录
        edit_change_info = dict_annual.get('updateinfo')
        if edit_change_info is not None:
            lst_edit_change = self.zj_get_annual_edit_change(edit_change_info)
            self.annual_info_dict[AnnualReports.EDIT_CHANGE_INFOS] = lst_edit_change

    # 年报基本信息
    @staticmethod
    def zj_get_annual_base_info(py_items):
        li_items = py_items.find('li').items()
        annual_base_info_dict = {}
        for item in li_items:
            item_content = item.text().replace(u'•', u'').replace(u':', u'：')
            part = item_content.split(u'：', 2)
            k = AnnualReports.format_base_model(part[0].strip())
            annual_base_info_dict[k] = part[1].strip()
        return annual_base_info_dict

    # 年报网站信息
    def zj_get_annual_web_site_info(self, web_info):
        lst_websites = []
        for web in web_info:
            web_item = {
                AnnualReports.WebSites.NAME: web.get('webSitName'),
                AnnualReports.WebSites.TYPE: u'网站' if web.get('webType') == '1' else u'网店',
                AnnualReports.WebSites.SITE: web.get('webSite')
            }
            web_item = self.replace_none(web_item)
            lst_websites.append(web_item)
        return lst_websites

    # 年报 股东出资信息(客户端分页)
    def zj_get_annual_share_hold_info(self, shareholder_infos):
        lst = []
        shareholder_data = shareholder_infos.get('data', [])
        for s in shareholder_data:
            share_model = {
                AnnualReports.ShareholderInformation.SHAREHOLDER_NAME: s.get('inv'),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                    s.get('lisubconam')),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TIME: s.get('subConDate'),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TYPE: s.get('conFormCN'),
                AnnualReports.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(s.get('liacconam')),
                AnnualReports.ShareholderInformation.PAIED_TIME: s.get('acConDate'),
                AnnualReports.ShareholderInformation.PAIED_TYPE: s.get('acConFormCn')
            }
            share_model = self.replace_none(share_model)
            lst.append(share_model)
        return lst

    # 年报 对外投资信息
    def zj_get_annual_inv_info(self, inv_infos):
        lst_inv = []
        for inv in inv_infos:
            inv_item = {
                AnnualReports.InvestedCompanies.COMPANY_NAME: inv.get('entName'),
                AnnualReports.InvestedCompanies.CODE: inv.get('uniCode'),
            }
            inv_item = self.replace_none(inv_item)
            lst_inv.append(inv_item)
        return lst_inv

    # 年报 企业资产状况信息
    @staticmethod
    def zj_get_annual_asset_info(tds):
        lst_value = tds.filter(lambda i: i % 2 == 1).map(lambda i, e: PyQuery(e).text())
        lst_title = tds.filter(lambda i: i % 2 == 0).map(lambda i, e: PyQuery(e).text())
        map_title_value = zip(lst_title, lst_value)
        model = {}
        for k_title, v_value in map_title_value:
            if k_title.strip() == u'':
                continue
            k = AnnualReports.format_asset_model(k_title)
            model[k] = v_value
        return model

    # 年报 对外担保方法
    def zj_get_annual_out_guarantee_info(self, out_guarante_infos):
        ga_type = {'1': u'一般保证', '2': u'连带保证'}
        out_guaranty_data = out_guarante_infos.get('data', [])
        lst = []
        for out_guaranty in out_guaranty_data:
            out_guarantee_model = {
                AnnualReports.OutGuaranteeInfo.CREDITOR: out_guaranty.get('more'),
                AnnualReports.OutGuaranteeInfo.OBLIGOR: out_guaranty.get('mortgagor'),
                AnnualReports.OutGuaranteeInfo.DEBT_TYPE:
                    U'合同' if out_guaranty.get('priClaSecKind') == '1' else u'其他',
                AnnualReports.OutGuaranteeInfo.DEBT_AMOUNT: out_guaranty.get('priClaSecAm'),
                AnnualReports.OutGuaranteeInfo.PERFORMANCE_PERIOD:
                    u'{0}-{1}'.format(out_guaranty.get('pefPerForm'), out_guaranty.get('pefPerTo')),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_PERIOD:
                    u'期间' if out_guaranty.get('guaPeriod') == '1' else u'未约定',
                AnnualReports.OutGuaranteeInfo.GUARANTEE_TYPE: ga_type.get(out_guaranty.get('gaType'), u'未约定')
            }
            out_guarantee_model = self.replace_none(out_guarantee_model)
            lst.append(out_guarantee_model)
        return lst

    # 年报 股权变更方法
    def zj_get_annual_edit_shareholding_change(self, edit_shareholding_change_infos):
        edit_shareholding_change_data = edit_shareholding_change_infos.get('data', [])
        lst = []
        for edit_shareholding_change in edit_shareholding_change_data:
            change_model = {
                AnnualReports.EditShareholdingChangeInfos.SHAREHOLDER_NAME: edit_shareholding_change.get('inv'),
                AnnualReports.EditShareholdingChangeInfos.BEFORE_CONTENT: edit_shareholding_change.get('beTransAmPr'),
                AnnualReports.EditShareholdingChangeInfos.AFTER_CONTENT: edit_shareholding_change.get('afTransAmPr'),
                AnnualReports.EditShareholdingChangeInfos.CHANGE_DATE: edit_shareholding_change.get('altDate')
            }
            change_model = self.replace_none(change_model)
            lst.append(change_model)
        return lst

    # 年报 修改记录
    def zj_get_annual_edit_change(self, edit_change_infos):
        edit_change_data = edit_change_infos.get('data', [])
        lst = []
        for edit_change in edit_change_data:
            edit_model = {
                AnnualReports.EditChangeInfos.CHANGE_ITEM: edit_change.get('altItemName'),
                AnnualReports.EditChangeInfos.BEFORE_CONTENT: edit_change.get('altBe'),
                AnnualReports.EditChangeInfos.AFTER_CONTENT: edit_change.get('altAf'),
                AnnualReports.EditChangeInfos.CHANGE_DATE: edit_change.get('altDate')
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
