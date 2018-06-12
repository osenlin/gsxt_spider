#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: parse_base_worker.py
@time: 2017/2/3 17:32
"""

from pyquery import PyQuery as py

from base.parse_base_worker import ParseBaseWorker
from common import util
from common.annual_field import *
from common.global_field import Model
from common.gsxt_field import *


class GsxtParseNingXiaWorker(ParseBaseWorker):
    def __init__(self, **kwargs):
        ParseBaseWorker.__init__(self, **kwargs)

    # 基本信息
    def get_base_info(self, base_info):
        base_info_dict = {}
        page = self.get_crawl_page(base_info)
        if page is None or page == u'':
            return {}

        res = py(page, parser='html').find('.info_table').find('table').find('td').items()
        for item in res:
            item_content = item.text().replace(' ', '')
            part = item_content.split('：', 1)
            k = GsModel.format_base_model(part[0].replace(' ', ''))
            base_info_dict[k] = part[1].strip()
            base_info_dict[GsModel.PERIOD] = u"{0}至{1}".format(base_info_dict.get(GsModel.PERIOD_FROM),
                                                               base_info_dict.get(GsModel.PERIOD_TO))
        return base_info_dict

    # 股东信息
    def get_shareholder_info(self, shareholder_info):
        shareholder_info_dict = {}
        lst_shareholder = []
        pages = self.get_crawl_page(shareholder_info, True)
        if pages is None:
            return {}

        for page in pages:
            trs = py(page.get('text', u''), parser='html').find('.partner_com').find('tr').not_(
                '.partner_com_top').items()
            for tr in trs:
                tds = tr.find('td')
                if tds is None or len(tds) < 2:
                    continue

                share_model = {
                    GsModel.ShareholderInformation.SHAREHOLDER_NAME: tds.eq(1).text().replace(u'\\t', u''),
                    GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(tds.eq(2).text()),

                    GsModel.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(tds.eq(3).text()),  # 实缴

                    # 认缴细节
                    GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL: [{
                        GsModel.ShareholderInformation.SUBSCRIPTION_TYPE: tds.eq(4).text(),  # 认缴方式
                        GsModel.ShareholderInformation.SUBSCRIPTION_TIME: tds.eq(6).text(),  # 认缴时间
                        GsModel.ShareholderInformation.SUBSCRIPTION_PUBLISH_TIME: tds.eq(10).text(),  # 认缴公式时间
                    }],

                    # 实缴细节
                    GsModel.ShareholderInformation.PAIED_DETAIL: [{
                        GsModel.ShareholderInformation.PAIED_TYPE: tds.eq(7).text(),  # 实缴类型
                        GsModel.ShareholderInformation.PAIED_TIME: tds.eq(9).text(),  # 实缴 时间
                        GsModel.ShareholderInformation.PAIED_PUBLISH_TIME: tds.eq(10).text(),  # 实缴公式时间
                    }]
                }
                lst_shareholder.append(share_model)

        shareholder_info_dict[GsModel.SHAREHOLDER_INFORMATION] = lst_shareholder
        return shareholder_info_dict

    # 变更信息
    def get_change_info(self, change_info):
        change_info_dict = {}
        lst_change_records = []
        pages = self.get_crawl_page(change_info, True)
        if pages is None:
            return {}

        for page in pages:
            trs = py(page.get(u'text', u''), parser='html').find('.partner_com').find('tr').not_(
                '.partner_com_top').items()

            for tr in trs:
                tds = tr.find('td')
                change_model = {
                    GsModel.ChangeRecords.CHANGE_ITEM: tds.eq(1).text(),
                    # 去除多余的字
                    GsModel.ChangeRecords.BEFORE_CONTENT: util.format_content(tds.eq(2).text()),
                    GsModel.ChangeRecords.AFTER_CONTENT: util.format_content(tds.eq(3).text()),
                    # 日期格式化
                    GsModel.ChangeRecords.CHANGE_DATE: tds.eq(4).text()
                }
                lst_change_records.append(change_model)
        change_info_dict[GsModel.CHANGERECORDS] = lst_change_records
        return change_info_dict

    # 主要人员
    def get_key_person_info(self, key_person_info):
        key_person_info_dict = {}
        page = self.get_crawl_page(key_person_info)
        if page is None or page == u'':
            return key_person_info_dict

        items = py(page, parser='html').find('.info_name').find('li').items()
        lst_key_person = []
        for item in items:
            item_content = item.text()
            part = item_content.split(' ', 1)
            if len(part) >= 2:
                name = part[0].strip()
                position = part[1].strip()
            elif len(part) == 1:
                name = part[0].strip()
                position = u''
            else:
                continue

            key_person = {
                GsModel.KeyPerson.KEY_PERSON_NAME: name,
                GsModel.KeyPerson.KEY_PERSON_POSITION: position}
            lst_key_person.append(key_person)

        key_person_info_dict[GsModel.KEY_PERSON] = lst_key_person
        return key_person_info_dict

    # 分支机构
    def get_branch_info(self, branch_info):
        branch_info_dict = {}
        page = self.get_crawl_page(branch_info)
        lst_branch = []
        if page is None or page == u'':
            return branch_info_dict

        items = py(page, parser='html').find('.info_name').find('li').items()
        for item in items:
            item_content = item.find('p').eq(0).text()
            part = item_content.split('：', 1)
            branch_model = {
                GsModel.Branch.COMPAY_NAME: item.find('h3').eq(0).text(),
                GsModel.Branch.CODE: part[1].strip()
            }
            lst_branch.append(branch_model)
        branch_info_dict[GsModel.BRANCH] = lst_branch
        return branch_info_dict

    # 出资信息
    def get_contributive_info(self, con_info):
        con_info_dict = {}
        part_b_con = {}
        part_a_con = {}
        pages_list = self.get_crawl_page(con_info, True)
        if pages_list is None:
            return {}

        for page in pages_list:
            if page == u'':
                continue

            trs = py(page.get(u'text', u''), parser='html').find('.partner_com').find('tr').not_(
                '.partner_com_top').items()
            for tr in trs:
                tds = tr.find('td')
                sub_model = {
                    GsModel.ContributorInformation.SHAREHOLDER_NAME: tds.eq(1).text(),
                    GsModel.ContributorInformation.SHAREHOLDER_TYPE: tds.eq(2).text(),
                    GsModel.ContributorInformation.CERTIFICATE_TYPE: tds.eq(3).text(),
                    GsModel.ContributorInformation.CERTIFICATE_NO: tds.eq(4).text()
                }
                part_a_con[tds.eq(1).text().strip()] = sub_model

        pages_detail = self.get_crawl_page(con_info, True, Model.type_detail)

        if pages_detail is not None:
            for page in pages_detail:
                if page.get(u'text', u'') != u'' and page.get(u'', '') is not None:
                    shareholder_name, sub_model = self.get_con_detail(page.get(u'text', u''))
                    part_b_con[shareholder_name] = sub_model
        lst_con = []
        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            lst_con.append(v_list)
        con_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = lst_con
        return con_info_dict

    # 获取股东信息详细列表
    @staticmethod
    def get_con_detail(page):
        shareholder_name = ""
        sub_model = {}
        if page is None or page == u'':
            return shareholder_name, sub_model

        tables = py(page, parser='html').find('.partner_com').items()
        for table in tables:
            if u'发起人' in table.find('.info_table_h3').text() or u'股东' in table.find('.info_table_h3').text():  # 股东信息
                tds = table.find('td')
                shareholder_name = tds.eq(1).text().strip()
                sub_model[GsModel.ContributorInformation.SHAREHOLDER_NAME] = tds.eq(1).text()
                sub_model[GsModel.ContributorInformation.SUBSCRIPTION_AMOUNT] = util.get_amount_with_unit(
                    tds.eq(3).text())
                sub_model[GsModel.ContributorInformation.PAIED_AMOUNT] = util.get_amount_with_unit(tds.eq(5).text())

            if u'认缴' in table.find('.info_table_h3').text():  # 认缴明细信息
                trs = table.find('tr')
                lst_sub_detail = []
                for tr_i in xrange(1, len(trs)):
                    tds = trs.eq(tr_i).find('td')
                    sub_model_detail = {
                        GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TYPE: tds.eq(0).text(),
                        GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                            tds.eq(1).text()),
                        GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TIME:
                            tds.eq(2).text()
                    }
                    sub_model_detail = replace_none(sub_model_detail)
                    lst_sub_detail.append(sub_model_detail)
                sub_model[GsModel.ContributorInformation.SUBSCRIPTION_DETAIL] = lst_sub_detail

            if u'实缴' in table.find('.info_table_h3').text():  # 实缴明细信息
                trs = table.find('tr')
                lst_paid_detail = []
                for tr_i in xrange(1, len(trs)):
                    tds = trs.eq(tr_i).find('td')
                    paid_model_detail = {
                        GsModel.ContributorInformation.PaiedDetail.PAIED_TYPE: tds.eq(0).text(),
                        GsModel.ContributorInformation.PaiedDetail.PAIED_AMOUNT: util.get_amount_with_unit(
                            tds.eq(1).text()),
                        GsModel.ContributorInformation.PaiedDetail.PAIED_TIME:
                            tds.eq(2).text()
                    }
                    paid_model_detail = replace_none(paid_model_detail)  # 补丁2
                    lst_paid_detail.append(paid_model_detail)
                sub_model[GsModel.ContributorInformation.PAIED_DETAIL] = lst_paid_detail
                sub_model = replace_none(sub_model)

        return shareholder_name, sub_model

    # 清算信息
    def get_liquidation_info(self, liquidation_info):
        return {}

    # 年报信息
    def get_annual_info(self, annual_item_list):
        return ParseNingXiaAnnual(annual_item_list, self.log).get_result()


class ParseNingXiaAnnual(object):
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

        page_list_share_hold = []
        page_list_inv_company = []
        page_list_out_guaranty = []
        page_list_edit_shareholding_change = []
        page_list_edit_change = []
        if self.annual_item_list is None or self.annual_item_list[0].get(u'status', u'fail') != u'success':
            raise IndexError("为抓取到相关网页,或者抓取网页失败")

        for item in self.annual_item_list:
            url = item.get(u'url', u'')
            page_web = item.get(u'text', u'')
            if page_web is None or page_web.strip() == u'':
                continue

            # 基本信息
            if u'gsQynbAction_qynbBaseInfo' in url:
                annual_base_info = self.get_annual_base_info(page_web)
                self.annual_info_dict.update(annual_base_info)

            # 网站或网店信息
            if u'gsQynbAction_wzxxInfos' in url:
                lst_websites = self.get_annual_out_website(page_web)
                self.annual_info_dict[AnnualReports.WEBSITES] = lst_websites

            # 年报 企业资产状况信息
            if u'gsQynbAction_qyzcInfos' in url:
                asset_model = self.get_annual_asset_info(page_web)
                self.annual_info_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = asset_model

            # 股东出资信息
            if u'gsQynbAction_gdczInfos' in url:
                page_list_share_hold.append(page_web)

            # 对外投资
            if u'gsQynbAction_dwtzInfos' in url:
                page_list_inv_company.append(page_web)

            # 对外担保
            if u'gsQynbAction_dwdbInfo' in url:
                page_list_out_guaranty.append(page_web)

            # 股权变更
            if u'gsQynbAction_gqbgInfo' in url:
                page_list_edit_shareholding_change.append(page_web)

            # 修改信息
            if u'gsQynbAction_nbxgInfos' in url:
                page_list_edit_change.append(page_web)

        # 股东出资信息
        lst_share_hold = self.get_annual_share_hold_info(page_list_share_hold)
        self.annual_info_dict[AnnualReports.SHAREHOLDER_INFORMATION] = lst_share_hold
        # 对外投资
        self.annual_info_dict[AnnualReports.INVESTED_COMPANIES] = self.get_annual_inv_info(page_list_inv_company)

        # 对外担保
        lst_out_guaranty = self.get_annual_out_guarantee_info(page_list_out_guaranty)
        self.annual_info_dict[AnnualReports.OUT_GUARANTEE_INFO] = lst_out_guaranty

        # 股权变更
        lst_edit_shareholding_change = self.get_annual_edit_shareholding_change(page_list_edit_shareholding_change)
        self.annual_info_dict[AnnualReports.EDIT_SHAREHOLDING_CHANGE_INFOS] = lst_edit_shareholding_change

        # 修改信息
        lst_edit_change = self.get_annual_edit_change(page_list_edit_change)
        self.annual_info_dict[AnnualReports.EDIT_CHANGE_INFOS] = lst_edit_change

    @staticmethod
    def get_annual_base_info(page):
        py_all = py(page, parser='html')
        tds = py_all.find('.info_table').find('td').items()
        annual_base_info = {}
        for td in tds:
            part = td.text().split(u'：', 1)
            k = AnnualReports.format_base_model(part[0])
            annual_base_info[k] = part[1]
        return annual_base_info

    @staticmethod
    def get_annual_out_website(page):
        py_all = py(page, parser='html')
        lis = py_all.find('ul').find('li').items()
        lst = []
        for li in lis:
            web_model = {
                AnnualReports.WebSites.TYPE: li.find('span').eq(0).text(),
                AnnualReports.WebSites.SITE: li.find('span').eq(1).text(),
                AnnualReports.WebSites.NAME: li.find('h3').text()
            }
            lst.append(web_model)
        return lst

    @staticmethod
    def get_annual_share_hold_info(page_list):
        lst = []
        for page in page_list:
            py_all = py(page, parser='html')
            trs = py_all.find('table').find('tr').not_('.partner_com_top').items()
            for tr in trs:
                tds = tr.find('td')
                if len(tds) < 2:
                    continue

                share_model = {
                    AnnualReports.ShareholderInformation.SHAREHOLDER_NAME: tds.eq(1).text().strip(),
                    AnnualReports.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                        tds.eq(2).text().strip()),
                    AnnualReports.ShareholderInformation.SUBSCRIPTION_TIME: tds.eq(3).text().strip(),  # 认缴时间
                    AnnualReports.ShareholderInformation.SUBSCRIPTION_TYPE: tds.eq(4).text().strip(),  # 认缴类型
                    AnnualReports.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(
                        tds.eq(5).text().strip()),  # 1实缴金额
                    AnnualReports.ShareholderInformation.PAIED_TIME: tds.eq(6).text().strip(),  # 实缴时间
                    AnnualReports.ShareholderInformation.PAIED_TYPE: tds.eq(7).text().strip(),  # 实缴类型

                }
                lst.append(share_model)
        return lst

    @staticmethod
    def get_annual_inv_info(page_list):
        lst = []
        for page in page_list:
            py_all = py(page, parser='html')
            lis = py_all.find('ul').find('li').items()

            for li in lis:
                model = {AnnualReports.InvestedCompanies.COMPANY_NAME: li.find('h3').text(),
                         AnnualReports.InvestedCompanies.CODE: li.find('p').find('span').text()}
                lst.append(model)
        return lst

    @staticmethod
    def get_annual_asset_info(page):
        py_all = py(page, parser='html')
        trs = py_all.find('table').find('tr').items()
        asset_model = {}
        for tr in trs:
            k1 = AnnualReports.format_asset_model(tr.find('.table_h3').eq(0).text().strip())
            asset_model[k1] = tr.find('.table_left').eq(0).text().strip().replace(' ', '')
            k2 = AnnualReports.format_asset_model(tr.find('.table_h3').eq(1).text().strip())
            asset_model[k2] = tr.find('.table_left').eq(1).text().strip().replace(' ', '')
        return asset_model

    # 年报 对外担保
    def get_annual_out_guarantee_info(self, page_list):
        lst = []
        for page in page_list:
            py_all = py(page, parser='html')
            trs = py_all.find('table').find('tr').not_('.partner_com_top').items()

            for tr in trs:
                tds = tr.find('td')
                if len(tds) < 2:
                    continue

                performance = tds.eq(5).text().strip()
                performance_period = self.trans_for(performance)
                share_model = {
                    AnnualReports.OutGuaranteeInfo.CREDITOR: tds.eq(1).text().strip(),  #
                    AnnualReports.OutGuaranteeInfo.OBLIGOR: tds.eq(2).text().strip(),  #
                    AnnualReports.OutGuaranteeInfo.DEBT_TYPE: tds.eq(3).text().strip(),  #
                    AnnualReports.OutGuaranteeInfo.DEBT_AMOUNT: util.get_amount_with_unit(
                        tds.eq(4).text().strip()),
                    AnnualReports.OutGuaranteeInfo.PERFORMANCE_PERIOD: performance_period,
                    AnnualReports.OutGuaranteeInfo.GUARANTEE_PERIOD: tds.eq(6).text().strip(),  # 担保期限
                    AnnualReports.OutGuaranteeInfo.GUARANTEE_TYPE: tds.eq(7).text().strip(),  # 担保方式
                }
                lst.append(share_model)
        return lst

    @staticmethod
    def trans_for(pef_per):
        if pef_per is None or pef_per == '':
            return pef_per

        part = pef_per.split('-', 1)
        part[0] = part[0]
        part[1] = part[1]
        return "{0}-{1}".format(part[0], part[1])

    @staticmethod
    def get_annual_edit_shareholding_change(pagelist):
        lst = []
        for page in pagelist:
            py_all = py(page, parser='html')
            trs = py_all.find('table').find('tr').not_('.partner_com_top').items()

            for tr in trs:
                tds = tr.find('td')
                if len(tds) < 2:
                    continue

                edit_model = {
                    AnnualReports.EditShareholdingChangeInfos.SHAREHOLDER_NAME: tds.eq(1).text().strip(),
                    AnnualReports.EditShareholdingChangeInfos.BEFORE_CONTENT: tds.eq(2).text().strip(),
                    AnnualReports.EditShareholdingChangeInfos.AFTER_CONTENT: tds.eq(3).text().strip(),
                    AnnualReports.EditShareholdingChangeInfos.CHANGE_DATE: tds.eq(4).text().strip()
                }
                lst.append(edit_model)
        return lst

    @staticmethod
    def get_annual_edit_change(page_list):
        lst = []
        for page in page_list:
            py_all = py(page, parser='html')
            trs = py_all.find('table').find('tr').not_('.partner_com_top').items()

            for tr in trs:
                tds = tr.find('td')
                if len(tds) < 2:
                    continue

                edit_model = {
                    AnnualReports.EditChangeInfos.CHANGE_ITEM: tds.eq(1).text().strip(),
                    AnnualReports.EditChangeInfos.BEFORE_CONTENT: tds.eq(2).text().strip(),
                    AnnualReports.EditChangeInfos.AFTER_CONTENT: tds.eq(3).text().strip(),
                    AnnualReports.EditChangeInfos.CHANGE_DATE: tds.eq(4).text().strip()
                }
                lst.append(edit_model)
        return lst

    def get_result(self):
        return self.annual_info_dict


def replace_none(temp_model):
    for k, v in temp_model.items():
        if v is None:
            v = ''
            temp_model[k] = v
    return temp_model
