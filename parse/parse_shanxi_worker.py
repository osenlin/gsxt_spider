#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: parse_base_worker.py
@time: 2017/2/3 17:32
"""
import traceback

from pyquery import PyQuery as py

from base.parse_base_worker import ParseBaseWorker
from common import util
from common.annual_field import *
from common.global_field import Model
from common.gsxt_field import *


class GsxtParseShanXiWorker(ParseBaseWorker):
    def __init__(self, **kwargs):
        ParseBaseWorker.__init__(self, **kwargs)

    # 基本信息
    def get_base_info(self, base_info):
        page = self.get_crawl_page(base_info)
        base_info_dict = {}
        if page is None or page == u'':
            return base_info_dict

        res = py(page, parser='html').find('.detail_info').find('.part').eq(0)
        res = res.find('tr').find('td').not_('.dot').items()
        for item in res:
            item_content = item.text()
            part = item_content.split('：', 1)
            k = GsModel.format_base_model(part[0].replace(' ', ''))
            base_info_dict[k] = part[1].strip()
            base_info_dict[GsModel.PERIOD] = u"{0}至{1}".format(base_info_dict.get(GsModel.PERIOD_FROM),
                                                               base_info_dict.get(GsModel.PERIOD_TO))
        base_info_dict = self.bu_ding(base_info_dict)  # 补丁

        # 变更信息
        try:
            change_info_dict = self.get_change_info(page)
        except:
            self.log.error(
                'company:{0},error-part:changerecords_info,error-info:{1}'.format(base_info_dict.get('company', ''),
                                                                                  traceback.format_exc()))
            raise IndexError(
                'company:{0},error-part:changerecords_info,error-info:{1}'.format(base_info_dict.get('company', ''),
                                                                                  traceback.format_exc()))
        base_info_dict.update(change_info_dict)

        # 股东信息
        try:
            shareholder_info_dict = self.get_shareholder_info(page)
        except:
            self.log.error(
                'company:{0},error-part:contributive_info,error-info:{1}'.format(base_info_dict.get('company', ''),
                                                                                 traceback.format_exc()))
            raise IndexError(
                'company:{0},error-part:contributive_info,error-info:{1}'.format(base_info_dict.get('company', ''),
                                                                                 traceback.format_exc()))
        base_info_dict.update(shareholder_info_dict)

        return base_info_dict

    # 股东信息
    def get_shareholder_info(self, page):
        shareholder_info_dict = {}
        lst_shareholder = []
        if isinstance(page, dict) or page is None:
            return {}

        trs = py(page, parser='html').find('#table_qytzr').find('tr').items()
        for tr in trs:
            tds = tr.find('td')
            if tds is None or len(tds) < 2:
                continue

            share_model = {
                GsModel.ShareholderInformation.SHAREHOLDER_NAME: tds.eq(0).text().replace(u'\\t', u''),
                GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(tds.eq(1).text()),  # 认缴
                GsModel.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(tds.eq(2).text()),  # 实缴

                # 认缴细节
                GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL: [{
                    GsModel.ShareholderInformation.SUBSCRIPTION_TYPE: tds.eq(3).text(),  # 认缴方式
                    GsModel.ShareholderInformation.SUBSCRIPTION_TIME: tds.eq(5).text(),  # 认缴时间
                    GsModel.ShareholderInformation.SUBSCRIPTION_PUBLISH_TIME: tds.eq(6).text(),  # 认缴公式时间
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
    def get_change_info(self, page):
        change_info_dict = {}
        lst_change_records = []
        if isinstance(page, dict) or page is None:
            return {}

        trs = py(page, parser='html').find('#table_bgxx').find('tr').items()
        for tr in trs:
            tds = tr.find('td')
            if tds is None or len(tds) < 2:
                continue

            change_model = {
                GsModel.ChangeRecords.CHANGE_ITEM: tds.eq(1).text(),
                # 去除多余的字
                GsModel.ChangeRecords.BEFORE_CONTENT: util.format_content(tds.eq(2).text()),
                GsModel.ChangeRecords.AFTER_CONTENT: util.format_content(tds.eq(3).text()),
                # 日期格式化
                GsModel.ChangeRecords.CHANGE_DATE: tds.eq(4).text()
            }
            lst_change_records.append(change_model)
        change_info_dict[GsModel.CHANGERECORDS] = lst_change_records if len(lst_change_records) != 0 else None
        return change_info_dict

    # 主要人员
    def get_key_person_info(self, key_person_info):
        key_person_info_dict = {}
        page = self.get_crawl_page(key_person_info)
        if page is None:
            return key_person_info_dict

        items = py(page, parser='html').find('.detail_info').find('.part').find('#ul1').find('li').items()
        lst_key_person = []
        for item in items:
            key_person = {
                GsModel.KeyPerson.KEY_PERSON_NAME: item.find('.name').text().strip(),
                GsModel.KeyPerson.KEY_PERSON_POSITION: item.find('.position').text().strip()}
            lst_key_person.append(key_person)

        key_person_info_dict[GsModel.KEY_PERSON] = lst_key_person
        return key_person_info_dict

    # 分支机构
    def get_branch_info(self, branch_info):
        branch_info_dict = {}
        page = self.get_crawl_page(branch_info)
        if page is None:
            return branch_info_dict

        items = py(page, parser='html').find('.part').find('li').items()
        lst_branch = []
        for item in items:
            branch_model = {
                GsModel.Branch.COMPAY_NAME: item.find('.span1').text().strip(),
                GsModel.Branch.CODE: item.find('.span2').eq(0).attr('title')
            }
            lst_branch.append(branch_model)
        sort_branch = []
        for dic in lst_branch:
            if dic not in sort_branch:
                sort_branch.append(dic)
        branch_info_dict[GsModel.BRANCH] = sort_branch
        return branch_info_dict

    # 出资信息
    def get_contributive_info(self, con_info):
        con_info_dict = {}
        part_a_con = {}
        part_b_con = {}
        lst_con = []
        pages_list = self.get_crawl_page(con_info, True)
        if pages_list is None:
            return {}

        for page in pages_list:
            if page is None:
                continue

            status = page.get('status', 'fail')
            if status != 'success':
                continue

            text = page.get('text', None)
            if text is None:
                continue

            trs = py(text, parser='html').find('#table_fr').find('tr').items()
            for tr in trs:
                tds = tr.find('td')
                if tds is None or len(tds) < 2:
                    continue

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
                shareholder_name, sub_model = self.get_share_hold_detail(page.get(u'text', u''))
                if len(sub_model) <= 0:
                    continue

                part_b_con[shareholder_name] = sub_model

        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            lst_con.append(v_list)
        con_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = lst_con
        return con_info_dict

    def get_share_hold_detail(self, page):
        shareholder_name = ""
        sub_model = {}
        tables = py(page, parser='html').find('.table_list').items()
        for table in tables:
            if u'发起人' in table.find('tr').eq(0).find('th').text() or u'股东' in table.find('tr').eq(0).find(
                    'th').text():  # 股东信息
                tds = table.find('td')
                shareholder_name = tds.eq(0).text().strip()
                sub_model[GsModel.ContributorInformation.SHAREHOLDER_NAME] = tds.eq(0).text()
                sub_model[GsModel.ContributorInformation.SUBSCRIPTION_AMOUNT] = util.get_amount_with_unit(
                    tds.eq(1).text())
                sub_model[GsModel.ContributorInformation.PAIED_AMOUNT] = util.get_amount_with_unit(tds.eq(2).text())

            if u'认缴' in table.find('tr').eq(0).find('th').text():  # 认缴明细信息
                trs = table.find('tr')
                lst_sub_detail = []
                for tr_i in xrange(1, len(trs)):
                    tds = trs.eq(tr_i).find('td')
                    if tds is None or len(tds) < 2:
                        continue

                    sub_model_detail = {
                        GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TYPE: tds.eq(0).text(),  # 类型
                        GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                            tds.eq(1).text()),
                        # 数量
                        GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TIME:
                            tds.eq(2).text()
                    }
                    sub_model_detail = self.bu_ding(sub_model_detail)  # 补丁1
                    lst_sub_detail.append(sub_model_detail)
                sub_model[GsModel.ContributorInformation.SUBSCRIPTION_DETAIL] = lst_sub_detail

            if u'实缴' in table.find('tr').eq(0).find('th').text():  # 实缴明细信息
                trs = table.find('tr')
                lst_paid_detail = []
                for tr_i in xrange(1, len(trs)):
                    tds = trs.eq(tr_i).find('td')
                    if tds is None or len(tds) < 2:
                        continue

                    paid_model_detail = {
                        GsModel.ContributorInformation.PaiedDetail.PAIED_TYPE: tds.eq(0).text(),
                        GsModel.ContributorInformation.PaiedDetail.PAIED_AMOUNT: util.get_amount_with_unit(
                            tds.eq(1).text()),
                        GsModel.ContributorInformation.PaiedDetail.PAIED_TIME:
                            tds.eq(2).text()
                    }
                    paid_model_detail = self.bu_ding(paid_model_detail)  # 补丁2
                    lst_paid_detail.append(paid_model_detail)
                sub_model[GsModel.ContributorInformation.PAIED_DETAIL] = lst_paid_detail
                sub_model = self.bu_ding(sub_model)  # 补丁3
        return shareholder_name, sub_model

    # 清算信息
    def get_liquidation_info(self, liquidation_info):
        return {}

    # 年报信息
    def get_annual_info(self, annual_item):
        annual_info_dict = {}
        inv_info = {}
        lst_out_guaranty = {}
        lst_websites = {}
        asset_model = {}
        annual_item = annual_item[0]
        if annual_item is None or annual_item.get(u'status', u'fail') != u'success':
            raise IndexError("为抓取到相关网页,或者抓取网页失败")

        page = annual_item.get(u'text', u'')
        if page is None or page == '':
            return annual_info_dict

        py_all = py(page, parser='html')
        # 基本信息
        basic_info_table = py_all.find('.table_xq')
        annual_base_info = self.get_annual_base_info(basic_info_table)
        annual_info_dict.update(annual_base_info)
        # 年报 企业资产状况信息
        part_items = py_all.find('.part').items()
        for item in part_items:
            part_title = item.find('.part_title').text()
            if u'资产状况信息' in part_title or part_title == u'生产经营情况信息':
                trs = item.find('tr').items()
                asset_model = self.get_annual_asset_info(trs)

            # 对外投资,
            if u'对外投资信息' in part_title:
                lis = item.find('li').items()
                inv_info = self.get_annual_inv_info(lis)

            # 对外担保
            if part_title == u'对外提供保证担保信息':
                trs = item.find('tr').items()
                lst_out_guaranty = self.get_annual_out_guarantee_info(trs)

            # 网站或网店信息
            if u'网站或网店信息' in part_title:
                lis = item.find('li').items()
                lst_websites = self.get_annual_out_website(lis)

        # 股东出资信息 有id
        share_hold_table = py_all.find('#table_gdxx')
        lst_share_hold = self.get_annual_share_hold_info(share_hold_table)
        annual_info_dict[AnnualReports.SHAREHOLDER_INFORMATION] = lst_share_hold
        # 修改记录 有id
        edit_change_table = py_all.find('#table_xgxx')
        lst_edit_change = self.get_annual_edit_change(edit_change_table)
        annual_info_dict[AnnualReports.EDIT_CHANGE_INFOS] = lst_edit_change
        # 股权变更 有id
        edit_shareholding_change_table = py_all.find('#table_gqbg')
        lst_edit_shareholding_change = self.get_annual_edit_shareholding_change(edit_shareholding_change_table)
        annual_info_dict[AnnualReports.EDIT_SHAREHOLDING_CHANGE_INFOS] = lst_edit_shareholding_change

        # 企业资产状况信息
        annual_info_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = asset_model
        # 对外投资,
        annual_info_dict[AnnualReports.INVESTED_COMPANIES] = inv_info
        # 对外担保
        annual_info_dict[AnnualReports.OUT_GUARANTEE_INFO] = lst_out_guaranty
        # 网站或网店信息
        annual_info_dict[AnnualReports.WEBSITES] = lst_websites
        # 年报 对外担保

        return annual_info_dict

    @staticmethod
    def get_annual_base_info(table):
        annual_base_info = {}
        trs = table.find('tr').items()
        for tr in trs:
            tds = tr.find('td').not_('.dot')
            part = tds.eq(0).text().split(u'：', 1)
            if tds.eq(0).text() != '':
                k1 = AnnualReports.format_base_model(part[0])
                annual_base_info[k1] = part[1]
            if tds.eq(1).text() != '':
                part = tds.eq(1).text().split(u'：', 1)
                k2 = AnnualReports.format_base_model(part[0])
                annual_base_info[k2] = part[1]
        return annual_base_info

    @staticmethod
    def get_annual_asset_info(trs):
        asset_model = {}
        for tr in trs:
            if tr.find('th').eq(0).text().strip() != '':
                k1 = AnnualReports.format_asset_model(tr.find('th').eq(0).text().strip())
                asset_model[k1] = tr.find('td').eq(0).text().strip().replace(' ', '')
            if tr.find('th').eq(1).text().strip() != '':
                k2 = AnnualReports.format_asset_model(tr.find('th').eq(1).text().strip())
                asset_model[k2] = tr.find('td').eq(1).text().strip().replace(' ', '')
        return asset_model

    @staticmethod
    def get_annual_inv_info(lis):
        lst = []
        for li in lis:
            model = {AnnualReports.InvestedCompanies.COMPANY_NAME: li.find('.span1').text(),
                     AnnualReports.InvestedCompanies.CODE: li.find('.span2').attr('title')}
            lst.append(model)
        return lst

    def get_annual_out_guarantee_info(self, trs):
        lst = []
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
                AnnualReports.OutGuaranteeInfo.DEBT_AMOUNT: util.get_amount_with_unit(tds.eq(4).text().strip()),
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
    def get_annual_out_website(lis):
        lst = []
        for li in lis:
            part = li.find('.span2').eq(0).text().split("：", 1)
            web_model = {
                AnnualReports.WebSites.TYPE: part[1].strip(),
                AnnualReports.WebSites.SITE: li.find('.span2').eq(1).attr('title'),
                AnnualReports.WebSites.NAME: li.find('.span1').eq(0).text()
            }
            lst.append(web_model)
        return lst

    @staticmethod
    def get_annual_share_hold_info(table):
        trs = table.find('tr').items()
        lst = []
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
                # 1实缴金额
                AnnualReports.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(tds.eq(5).text().strip()),
                AnnualReports.ShareholderInformation.PAIED_TIME: tds.eq(6).text().strip(),  # 实缴时间
                AnnualReports.ShareholderInformation.PAIED_TYPE: tds.eq(7).text().strip(),  # 实缴类型

            }
            lst.append(share_model)
        return lst

    @staticmethod
    def get_annual_edit_change(table):
        trs = table.find('tr').items()
        lst = []
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

    @staticmethod
    def get_annual_edit_shareholding_change(table):
        trs = table.find('tr').items()
        lst = []
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
    def bu_ding(temp_model):
        for k, v in temp_model.items():
            if v is None:
                v = ''
                temp_model[k] = v
        return temp_model
