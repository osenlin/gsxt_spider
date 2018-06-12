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

from pyquery import PyQuery

from base.parse_base_worker import ParseBaseWorker
from common import util
from common.annual_field import *
from common.global_field import Model
from common.gsxt_field import *


# todo 甘肃亚盛股份实业（集团）有限公司兴盛分公司  解析逻辑有问题, 会漏掉属性解析

class GsxtParseGanSuWorker(ParseBaseWorker):
    def __init__(self, **kwargs):
        ParseBaseWorker.__init__(self, **kwargs)

    # 基本信息
    def get_base_info(self, base_info):
        page = self.get_crawl_page(base_info)
        res = PyQuery(page, parser='html').find('.info_name').items()
        base_info_dict = {}
        money = util.get_match_value("toDecimal6\('", "'\);", page)

        for item in res:
            item_content = item.text().replace('•', '')
            item_content = item_content.replace('：', ':')
            part = item_content.split(':', 1)
            k = GsModel.format_base_model(part[0].replace(' ', ''))
            base_info_dict[k] = part[1].strip()
            base_info_dict[GsModel.PERIOD] = u"{0}至{1}". \
                format(base_info_dict.get(GsModel.PERIOD_FROM), base_info_dict.get(GsModel.PERIOD_TO))
        reg_unit = base_info_dict.get(GsModel.REGISTERED_CAPITAL)
        if reg_unit is not None:
            base_info_dict[GsModel.REGISTERED_CAPITAL] = money + reg_unit

        # 股东信息
        try:
            shareholder_info_dict = self.get_inline_shareholder_info(page)
        except ValueError:
            self.log.error('company:{0},error-part:shareholder_info_dict,error-info:{1}'.format(
                base_info.get('company', u''), traceback.format_exc()))
            shareholder_info_dict = {}
        base_info_dict.update(shareholder_info_dict)

        # 变更信息
        try:
            change_info_dict = self.get_inline_change_info(page)
        except ValueError:
            self.log.error('company:{0},error-part:change_info_dict,error-info:{1}'.format(
                base_info.get('company', u''), traceback.format_exc()))
            change_info_dict = {}
        base_info_dict.update(change_info_dict)
        return base_info_dict

    # 股东信息
    @staticmethod
    def get_inline_shareholder_info(page):
        shareholder_info_dict = {}
        shareholder_list = []
        trs = PyQuery(page, parser='html').find('#gd_JSTab').find('tr').items()
        for tr in trs:
            tds = tr.find('td')
            if tds is None or len(tds) < 2:
                continue

            share_model = {
                GsModel.ShareholderInformation.SHAREHOLDER_NAME: tds.eq(1).text().replace(u'\\t', u''),
                GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(tds.eq(2).text()),
                GsModel.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(tds.eq(3).text()),
                GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL:
                    [{
                        GsModel.ShareholderInformation.SUBSCRIPTION_TYPE: tds.eq(4).text(),
                        GsModel.ShareholderInformation.SUBSCRIPTION_TIME: tds.eq(6).text(),
                        GsModel.ShareholderInformation.SUBSCRIPTION_PUBLISH_TIME: tds.eq(7).text(),
                    }],
                GsModel.ShareholderInformation.PAIED_DETAIL:
                    [{
                        GsModel.ShareholderInformation.PAIED_TYPE: tds.eq(8).text(),
                        GsModel.ShareholderInformation.PAIED_TIME: tds.eq(10).text(),
                        GsModel.ShareholderInformation.PAIED_PUBLISH_TIME: tds.eq(11).text()
                    }]
            }
            shareholder_list.append(share_model)

        if len(shareholder_list) > 0:
            shareholder_info_dict[GsModel.SHAREHOLDER_INFORMATION] = shareholder_list

        return shareholder_info_dict

    # 变更信息
    @staticmethod
    def get_inline_change_info(page):
        change_info_dict = {}
        change_records_list = []
        trs = PyQuery(page, parser='html').find('#changeTab').find('tr').items()
        for tr in trs:
            tds = tr.find('td')
            if len(tds) < 2:
                continue

            change_model = {
                GsModel.ChangeRecords.CHANGE_ITEM: tds.eq(1).text(),
                # 去除多余的字
                GsModel.ChangeRecords.BEFORE_CONTENT: util.format_content(tds.eq(2).text()),
                GsModel.ChangeRecords.AFTER_CONTENT: util.format_content(tds.eq(3).text()),
                # 日期格式化
                GsModel.ChangeRecords.CHANGE_DATE: tds.eq(4).text()
            }
            change_records_list.append(change_model)

        if len(change_records_list) > 0:
            change_info_dict[GsModel.CHANGERECORDS] = change_records_list

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
        page = self.get_crawl_page(key_person_info)

        items = PyQuery(page, parser='html').find('#per270').items()
        key_person_list = []
        for item in items:
            spans = item.find('span')
            if len(spans) < 2:
                continue

            key_person = {
                GsModel.KeyPerson.KEY_PERSON_NAME: spans.eq(0).text().strip(),
                GsModel.KeyPerson.KEY_PERSON_POSITION: spans.eq(1).text().strip()}
            key_person_list.append(key_person)

        if len(key_person_list) > 0:
            key_person_info_dict[GsModel.KEY_PERSON] = key_person_list
        return key_person_info_dict

    # 分支机构
    def get_branch_info(self, branch_info):
        branch_info_dict = {}
        page = self.get_crawl_page(branch_info)
        items = PyQuery(page, parser='html').find('#fzjg308').items()
        branch_list = []
        for item in items:
            spans = item.find('span')
            if len(spans) < 7:
                continue

            branch_model = {
                GsModel.Branch.COMPAY_NAME: spans.eq(0).text(),
                GsModel.Branch.CODE: spans.eq(3).text(),
                GsModel.Branch.REGISTERED_ADDRESS: spans.eq(6).text()  # 待定
            }
            branch_list.append(branch_model)
        if len(branch_list) > 0:
            branch_info_dict[GsModel.BRANCH] = branch_list
        return branch_info_dict

    # 出资信息
    def get_contributive_info(self, con_info):
        con_info_dict = {}
        part_a_con = {}
        part_b_con = {}
        pages_list = self.get_crawl_page(con_info, True)

        # for else 业务逻辑是什么?
        for page in pages_list:
            trs = PyQuery(page.get(u'text', u''), parser='html').find('#invTab').find('tr').items()
            '''
            注释: 查找id为invTab的<tr>集合,则进入循环(分支1)
            '''
            for tr in trs:
                tds = tr.find('td')
                if len(tds) < 2:
                    continue

                con_model = {
                    GsModel.ContributorInformation.SHAREHOLDER_NAME: tds.eq(1).text(),
                    GsModel.ContributorInformation.SHAREHOLDER_TYPE: tds.eq(2).text(),
                    GsModel.ContributorInformation.CERTIFICATE_TYPE: tds.eq(3).text(),
                    GsModel.ContributorInformation.CERTIFICATE_NO: tds.eq(4).text()
                }
                part_a_con[tds.eq(1).text().strip()] = con_model
            else:
                '''
                注释: 查找id为invTab的<tr>集合,没有数据集,则进入else分支,查找id为tzrPageTab的<tr>集合(分支2)
                '''
                trs = PyQuery(page.get('text', ''), parser='html').find('#tzrPageTab').find('tr').items()
                for tr in trs:
                    tds = tr.find('td')
                    if len(tds) < 2:
                        continue

                    con_model = {
                        GsModel.ContributorInformation.SHAREHOLDER_NAME: tds.eq(1).text(),
                        GsModel.ContributorInformation.SHAREHOLDER_TYPE: tds.eq(2).text()
                    }
                    part_a_con[con_model[GsModel.ContributorInformation.SHAREHOLDER_NAME]] = con_model

        pages_detail = self.get_crawl_page(con_info, True, Model.type_detail)
        if pages_detail is not None:
            for page in pages_detail:
                tables = PyQuery(page.get(u'text', u''), parser='html').find('.detailsList').items()
                shareholder_name, sub_model = self._get_sharehold_detail(tables)
                shareholder_name.replace(u'.', u'')
                part_b_con[shareholder_name] = sub_model

        con_list = []
        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            con_list.append(v_list)

        if len(con_list) > 0:
            con_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = con_list
        return con_info_dict

    # 清算信息
    def get_liquidation_info(self, liquidation_info):
        return {}

    def get_chattel_mortgage_info_detail(self, onclick, detail_list):

        result = dict()

        if onclick is None or onclick.strip() == '':
            return result

        temp_list = onclick.split(u'\'')
        if temp_list is None or len(temp_list) < 2:
            return result

        temp_list = temp_list[1].split(u'\'')
        if temp_list is None or len(temp_list) <= 0:
            return result

        morreg_id = temp_list[0]

        # 遍历所有页面
        for detail in detail_list:
            url = detail.get('url')
            if not isinstance(url, basestring):
                continue

            if morreg_id not in url:
                continue

            text = detail.get('text')
            if not isinstance(text, basestring) or text.strip() == u'':
                continue

            table_list = PyQuery(text, parser='html').find('.detailsList')
            if table_list is None or table_list.length < 5:
                raise FieldMissError

            # 动产抵押登记信息
            td_list = table_list.eq(0).find('td')
            cm_dict = dict()
            result[GsModel.ChattelMortgageInfo.ChattelDetail.CHATTEL_MORTGAGE] = cm_dict
            cm_dict[GsModel.ChattelMortgageInfo.ChattelDetail.ChattelMortgage.REGISTER_NUM] = td_list.eq(0).text()
            cm_dict[GsModel.ChattelMortgageInfo.ChattelDetail.ChattelMortgage.REGISTER_DATE] = td_list.eq(1).text()
            cm_dict[GsModel.ChattelMortgageInfo.ChattelDetail.ChattelMortgage.REGISTER_OFFICE] = td_list.eq(2).text()

            # 抵押权人概况信息
            tr_list = table_list.eq(1).find('tr').items()
            mps_list = list()
            result[GsModel.ChattelMortgageInfo.ChattelDetail.MORTGAGE_PERSON_STATUS] = mps_list
            for tr in tr_list:
                td_list = tr.find('td')
                if td_list is None or td_list.length < 5:
                    continue

                item = dict()
                item[GsModel.ChattelMortgageInfo.ChattelDetail.MortgagePersonStatus.MORTGAGE_PERSON_NAME] = td_list.eq(
                    1).text()
                item[GsModel.ChattelMortgageInfo.ChattelDetail.MortgagePersonStatus.CERTIFICATE_TYPE] = td_list.eq(
                    2).text()
                item[GsModel.ChattelMortgageInfo.ChattelDetail.MortgagePersonStatus.CERTIFICATE_NUM] = td_list.eq(
                    3).text()
                item[GsModel.ChattelMortgageInfo.ChattelDetail.MortgagePersonStatus.ADDRESS] = td_list.eq(4).text()
                mps_list.append(item)

            # 被担保债权概况信息
            td_list = table_list.eq(2).find('td')
            gps_dict = dict()
            result[GsModel.ChattelMortgageInfo.ChattelDetail.GUARANTEED_PERSON_STATUS] = gps_dict
            gps_dict[GsModel.ChattelMortgageInfo.ChattelDetail.GuaranteedPersonStatus.KIND] = td_list.eq(0).text()
            gps_dict[GsModel.ChattelMortgageInfo.ChattelDetail.GuaranteedPersonStatus.AMOUNT] = td_list.eq(1).text()
            gps_dict[GsModel.ChattelMortgageInfo.ChattelDetail.GuaranteedPersonStatus.SCOPE] = td_list.eq(2).text()
            gps_dict[GsModel.ChattelMortgageInfo.ChattelDetail.GuaranteedPersonStatus.PERIOD] = td_list.eq(3).text()
            gps_dict[GsModel.ChattelMortgageInfo.ChattelDetail.GuaranteedPersonStatus.REMARK] = td_list.eq(4).text()

            # 抵押物概况信息
            tr_list = table_list.eq(3).find('tr').items()
            gs_list = list()
            result[GsModel.ChattelMortgageInfo.ChattelDetail.GUARANTEE_STATUS] = gs_list
            for tr in tr_list:
                td_list = tr.find('td')
                if td_list is None or td_list.length < 5:
                    continue

                item = dict()
                item[GsModel.ChattelMortgageInfo.ChattelDetail.GuaranteeStatus.NAME] = td_list.eq(
                    1).text()
                item[GsModel.ChattelMortgageInfo.ChattelDetail.GuaranteeStatus.AFFILIATION] = td_list.eq(
                    2).text()
                item[GsModel.ChattelMortgageInfo.ChattelDetail.GuaranteeStatus.SITUATION] = td_list.eq(
                    3).text()
                item[GsModel.ChattelMortgageInfo.ChattelDetail.GuaranteeStatus.REMARK] = td_list.eq(4).text()
                gs_list.append(item)

            # 变更信息
            tr_list = table_list.eq(4).find('tr').items()
            change_list = list()
            result[GsModel.ChattelMortgageInfo.ChattelDetail.CHANGE_INFO] = change_list

            for tr in tr_list:
                td_list = tr.find('td')
                if td_list is None or td_list.length < 3:
                    continue

                item = dict()
                item[GsModel.ChattelMortgageInfo.ChattelDetail.ChangeInfo.CHANGE_DATE] = td_list.eq(
                    1).text()
                item[GsModel.ChattelMortgageInfo.ChattelDetail.ChangeInfo.CHANGE_CONTENT] = td_list.eq(
                    2).text()

            break

        return result

    # 动产抵押登记信息
    def get_chattel_mortgage_info(self, chattel_mortgage_info):
        chattel_mortgage_info_dict = dict()
        result_list = list()

        # 记录信息 空表也需要
        chattel_mortgage_info_dict[GsModel.CHATTEL_MORTGAGE_INFO] = result_list

        detail_list = self.get_crawl_page(chattel_mortgage_info, multi=True, part=Model.type_detail)

        page_text = self.get_crawl_page(chattel_mortgage_info)
        if page_text is None:
            return chattel_mortgage_info_dict

        jq = PyQuery(page_text, parser='html')
        move_tab = jq.find("#moveTab")
        tr_list = move_tab.find('tr').items()
        for tr in tr_list:
            td_list = tr.find('td')
            if td_list.length < 8:
                continue

            item = dict()

            item[GsModel.ChattelMortgageInfo.REGISTER_NUM] = td_list.eq(1).text()
            item[GsModel.ChattelMortgageInfo.REGISTER_DATE] = td_list.eq(2).text()
            item[GsModel.ChattelMortgageInfo.REGISTER_OFFICE] = td_list.eq(3).text()
            item[GsModel.ChattelMortgageInfo.CREDIT_AMOUNT] = util.get_amount_with_unit(td_list.eq(4).text())
            item[GsModel.ChattelMortgageInfo.STATUS] = td_list.eq(5).text()
            item[GsModel.ChattelMortgageInfo.PUBLISH_DATE] = td_list.eq(6).text()
            item[GsModel.ChattelMortgageInfo.CHATTEL_DETAIL] = self.get_chattel_mortgage_info_detail(
                td_list.eq(7).find('a').attr('onclick'), detail_list)

            if len(item) > 0:
                result_list.append(item)

        return chattel_mortgage_info_dict

    # 列入经营异常名录信息
    def get_abnormal_operation_info(self, abnormal_operation_info):
        abnormal_operation_info_dict = dict()

        result_list = list()

        # 记录信息 空表也需要
        abnormal_operation_info_dict[GsModel.ABNORMAL_OPERATION_INFO] = result_list

        page_text = self.get_crawl_page(abnormal_operation_info)
        if page_text is None:
            return abnormal_operation_info_dict

        jq = PyQuery(page_text, parser='html')
        move_tab = jq.find("#excpTab")
        tr_list = move_tab.find('tr').items()
        for tr in tr_list:
            td_list = tr.find('td')
            if td_list.length < 7:
                continue

            item = dict()

            item[GsModel.AbnormalOperationInfo.ENROL_REASON] = td_list.eq(1).text()
            item[GsModel.AbnormalOperationInfo.ENROL_DATE] = td_list.eq(2).text()
            item[GsModel.AbnormalOperationInfo.ENROL_DECIDE_OFFICE] = td_list.eq(3).text()
            item[GsModel.AbnormalOperationInfo.REMOVE_REASON] = td_list.eq(4).text()
            item[GsModel.AbnormalOperationInfo.REMOVE_DATE] = td_list.eq(5).text()
            item[GsModel.AbnormalOperationInfo.REMOVE_DECIDE_OFFICE] = td_list.eq(6).text()

            if len(item) > 0:
                result_list.append(item)

        return abnormal_operation_info_dict

    # 股权出质登记信息 详情
    def get_equity_pledged_info_detail(self, onclick, detail_list):
        return {}

    # 股权出质登记信息 股权出资登记
    def get_equity_pledged_info(self, equity_pledged_info):
        equity_pledged_info_dict = dict()

        result_list = list()

        # 记录信息 空表也需要
        equity_pledged_info_dict[GsModel.EQUITY_PLEDGED_INFO] = result_list

        detail_list = self.get_crawl_page(equity_pledged_info, multi=True, part=Model.type_detail)

        page_text = self.get_crawl_page(equity_pledged_info)
        if page_text is None:
            return equity_pledged_info_dict

        jq = PyQuery(page_text, parser='html')
        move_tab = jq.find("#stockTab")
        tr_list = move_tab.find('tr').items()
        for tr in tr_list:
            td_list = tr.find('td')
            if td_list.length < 11:
                continue

            item = dict()

            item[GsModel.EquityPledgedInfo.REGISTER_NUM] = td_list.eq(1).text()
            item[GsModel.EquityPledgedInfo.MORTGAGOR] = td_list.eq(2).text()
            item[GsModel.EquityPledgedInfo.MORTGAGOR_NUM] = td_list.eq(3).text()
            item[GsModel.EquityPledgedInfo.PLEDGE_STOCK_AMOUNT] = util.get_amount_with_unit(td_list.eq(4).text())
            item[GsModel.EquityPledgedInfo.PLEDGEE] = td_list.eq(5).text()
            item[GsModel.EquityPledgedInfo.PLEDGEE_NUM] = td_list.eq(6).text()
            item[GsModel.EquityPledgedInfo.REGISTER_DATE] = td_list.eq(7).text()
            item[GsModel.EquityPledgedInfo.STATUS] = td_list.eq(8).text()
            item[GsModel.EquityPledgedInfo.PUBLISH_DATE] = td_list.eq(9).text()
            item[GsModel.EquityPledgedInfo.EQUITY_PLEDGED_DETAIL] = self.get_equity_pledged_info_detail(
                td_list.eq(10).find('a').attr('onclick'), detail_list)

            if len(item) > 0:
                result_list.append(item)

        return equity_pledged_info_dict

    # 年报信息
    def get_annual_info(self, annual_item_list):
        return ParseGanSuAnnual(annual_item_list, self.log).get_result()


class ParseGanSuAnnual(object):
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

        annual_item = self.annual_item_list[0]

        page = annual_item.get(u'text', u'')
        if page is None or len(page) == 0:
            return {}

        py_all = PyQuery(page, parser='html')

        # 基本信息
        info = py_all.find('.info_name').items()
        annual_base_info = self.get_annual_base_info(info)
        self.annual_info_dict.update(annual_base_info)

        # 年报 企业资产状况信息
        tds = py_all.find('#zczkId')
        asset_model = self.get_annual_asset_info(tds, py_all)
        self.annual_info_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = asset_model

        divs = py_all.find('.webStyle.anchebotLine').items()
        for div in divs:

            # 网店
            if u'网址' in div.text():
                py_websites = div.find('#webInfo').items()
                lst_websites = self.get_annual_web_site_info(py_websites)
                self.annual_info_dict[AnnualReports.WEBSITES] = lst_websites

            # 对外投资
            elif u'注册号' in div.text():
                py_inv_company = div.find('#webInfo').items()
                lst_inv = self.get_annual_inv_info(py_inv_company)
                self.annual_info_dict[AnnualReports.INVESTED_COMPANIES] = lst_inv

        # 对外担保
        py_share_hold = py_all.find('#dBaoAnrepTab').find('tr').items()
        lst_out_guaranty = self.get_annual_out_guarantee_info(py_share_hold)
        self.annual_info_dict[AnnualReports.OUT_GUARANTEE_INFO] = lst_out_guaranty

        # 股东出资信息
        py_share_hold = py_all.find('#gdczAnrepTab').find('tr').items()
        lst_share_hold = self.get_annual_share_hold_info(py_share_hold)
        self.annual_info_dict[AnnualReports.SHAREHOLDER_INFORMATION] = lst_share_hold

        # 股权变更
        py_edit_shareholding_change = py_all.find('#gqAlertTab').find('tr').items()
        lst_edit_shareholding_change = self.get_annual_edit_shareholding_change(py_edit_shareholding_change)
        self.annual_info_dict[AnnualReports.EDIT_SHAREHOLDING_CHANGE_INFOS] = lst_edit_shareholding_change

        # 修改记录
        py_edit_change = py_all.find('#modifyTab').find('tr').items()
        lst_edit_change = self.get_annual_edit_change(py_edit_change)
        self.annual_info_dict[AnnualReports.EDIT_CHANGE_INFOS] = lst_edit_change

    # 年报基本信息
    @staticmethod
    def get_annual_base_info(py_items):
        annual_base_info_dict = {}
        for item in py_items:
            item_content = item.text().replace(u'•', u'').replace(u':', u'：')
            part = item_content.split(u'：', 2)
            k = AnnualReports.format_base_model(part[0].strip())
            annual_base_info_dict[k] = part[1].strip()
        return annual_base_info_dict

    # 年报网站信息
    @staticmethod
    def get_annual_web_site_info(py_websites):
        lst_web = []
        for py_web in py_websites:
            py_items = py_web.find('p').items()
            web_item = {}

            for item in py_items:
                if len(item.find('span')) == 1:
                    web_item[AnnualReports.WebSites.NAME] = item.text()
                else:
                    item_content = item.text().replace(u'·', u'')
                    part = item_content.split(u'：', 2)
                    k = AnnualReports.format_website_model(part[0].strip())
                    web_item[k] = part[1].strip()
            lst_web.append(web_item)
        return lst_web

    # 年报 企业资产状况信息
    @staticmethod
    def get_annual_asset_info(table_body, py_all):
        if len(table_body) <= 0:
            return {}

        model = {}
        lst_value = table_body.find('td').text().split(' ')
        lst_title = table_body.find('th').text().split(' ')
        if lst_title[0] == '':
            ent_body = py_all.find('#entZczk')
            lst_value = ent_body.find('td').text().split(' ')
            lst_title = ent_body.find('th').text().split(' ')
        map_title_value = zip(lst_title, lst_value)

        for k_title, v_value in map_title_value:
            k = AnnualReports.format_asset_model(k_title)
            model[k] = v_value
        return model

    # 年报 对外投资信息
    @staticmethod
    def get_annual_inv_info(py_inv_company):
        lst_inv = []
        for py_inv_item in py_inv_company:
            inv_item = {}
            ps_items = py_inv_item.find('p').items()

            for item in ps_items:
                if len(item.find('span')) == 1:
                    inv_item[AnnualReports.InvestedCompanies.COMPANY_NAME] = item.text()
                else:
                    item_content = item.text().replace(u'·', u'')
                    part = item_content.split(u'：', 2)
                    inv_item[AnnualReports.InvestedCompanies.CODE] = part[1].strip()
            lst_inv.append(inv_item)
        return lst_inv

    # 年报 对外担保方法
    @staticmethod
    def get_annual_out_guarantee_info(py_items):
        lst = []
        for trs in py_items:
            tds = trs.find('td')
            if tds.text() == '':
                continue

            out_guarantee_model = {
                AnnualReports.OutGuaranteeInfo.CREDITOR: tds.eq(1).text(),
                AnnualReports.OutGuaranteeInfo.OBLIGOR: tds.eq(2).text(),
                AnnualReports.OutGuaranteeInfo.DEBT_TYPE: tds.eq(3).text(),
                AnnualReports.OutGuaranteeInfo.DEBT_AMOUNT: tds.eq(4).text(),
                AnnualReports.OutGuaranteeInfo.PERFORMANCE_PERIOD: tds.eq(5).text(),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_PERIOD: tds.eq(6).text(),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_TYPE: tds.eq(7).text()
            }
            lst.append(out_guarantee_model)
        return lst

    # 年报 股东出资信息(客户端分页)
    @staticmethod
    def get_annual_share_hold_info(gdcz_item):
        lst = []
        for trs in gdcz_item:
            tds = trs.find('td')
            if tds.text() == '':
                continue

            share_model = {
                AnnualReports.ShareholderInformation.SHAREHOLDER_NAME: tds.eq(1).text(),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(tds.eq(2).text()),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TIME: tds.eq(3).text(),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TYPE: tds.eq(4).text(),
                AnnualReports.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(tds.eq(5).text()),
                AnnualReports.ShareholderInformation.PAIED_TIME: tds.eq(6).text(),
                AnnualReports.ShareholderInformation.PAIED_TYPE: tds.eq(7).text()
            }
            lst.append(share_model)
        return lst

    # 年报 股权变更方法
    @staticmethod
    def get_annual_edit_shareholding_change(py_items):
        lst = []
        for trs in py_items:
            tds = trs.find('td')
            if tds.text() == '':
                continue

            change_model = {
                AnnualReports.EditShareholdingChangeInfos.SHAREHOLDER_NAME: tds.eq(1).text(),
                AnnualReports.EditShareholdingChangeInfos.BEFORE_CONTENT: tds.eq(2).text(),
                AnnualReports.EditShareholdingChangeInfos.AFTER_CONTENT: tds.eq(3).text(),
                AnnualReports.EditShareholdingChangeInfos.CHANGE_DATE: tds.eq(4).text()
            }
            lst.append(change_model)
        return lst

    def get_result(self):
        return self.annual_info_dict

    # 年报 修改记录
    @staticmethod
    def get_annual_edit_change(py_items):
        lst = []
        for trs in py_items:
            tds = trs.find('td')
            if tds.text().strip() == u'':
                continue

            edit_model = {
                AnnualReports.EditChangeInfos.CHANGE_ITEM: tds.eq(1).text(),
                AnnualReports.EditChangeInfos.BEFORE_CONTENT: tds.eq(2).text(),
                AnnualReports.EditChangeInfos.AFTER_CONTENT: tds.eq(3).text(),
                AnnualReports.EditChangeInfos.CHANGE_DATE: tds.eq(4).text()
            }
            lst.append(edit_model)
        return lst
