#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: parse_base_worker.py
@time: 2017/2/3 17:32
"""
from pyquery import PyQuery

from base.parse_base_worker import ParseBaseWorker
from common import util
from common.annual_field import *
from common.global_field import *
from common.gsxt_field import *


class GsxtParseGuangXiWorker(ParseBaseWorker):
    def __init__(self, **kwargs):
        ParseBaseWorker.__init__(self, **kwargs)

    # 基本信息 done
    def get_base_info(self, base_info):
        page = self.get_crawl_page(base_info)
        res = PyQuery(page, parser='html').find('.qyqx-detail').find('tbody').find('td').items()
        base_info_dict = self._get_base_item_info(res)
        return base_info_dict

    # 股东信息 百度在线网络技术（北京）有限公司
    def get_shareholder_info(self, shareholder_info):
        shareholder_info_dict = {}
        shareholder_list = []
        page_list = self.get_crawl_page(shareholder_info, True)
        for page in page_list:
            table = PyQuery(page.get('text', u''), parser='html').find('.table-result')
            trs = table.find('tr').items()
            amount_unit = util.get_amount_unit(table)

            for tr in trs:
                tds = tr.find('td')
                if tds is None or len(tds) <= 2:
                    continue
                share_model = {
                    GsModel.ShareholderInformation.SHAREHOLDER_NAME: tds.eq(0).text().replace(u'\\t', u''),
                    GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(tds.eq(1).text()),
                    GsModel.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(tds.eq(2).text()),
                }
                tables = tds.find('table')
                if tables is None or len(tables) != 2:
                    continue

                trs_sub = tables.eq(0).find('tr').items()
                share_subs = self.get_sharehold_info_sub_detail(trs_sub, amount_unit=amount_unit)
                share_model.update(share_subs)
                trs_paied = tables.eq(1).find('tr').items()
                share_paied = self.get_sharehold_info_sub_detail(trs_paied, amount_unit=amount_unit,
                                                                 is_subs_or_paied='paied')
                share_model.update(share_paied)
                shareholder_list.append(share_model)
        if len(shareholder_list) > 0:
            shareholder_info_dict[GsModel.SHAREHOLDER_INFORMATION] = shareholder_list
        return shareholder_info_dict

    def get_change_info_detail(self, match_feature, detail):
        before = ''
        after = ''
        if detail is None:
            return before, after

        def get_change_list_detail(table):
            tr_list = table.find('tr').items()
            position_list = []
            for tr in tr_list:
                td_list = tr.find('td')
                if len(td_list) < 3:
                    continue
                position_list.append(td_list.eq(1).text().strip('*').strip() + ':' + td_list.eq(2).text())

            return ','.join(position_list)

        for item in detail:
            feature = item.get('match_feature')
            if feature is None:
                continue
            if feature.strip() not in match_feature and match_feature not in feature:
                continue
            text = item.get('text')
            if text is None or text.strip() == '':
                return before, after
            table_list = PyQuery(text, parser='html').find('.table-result')
            before = get_change_list_detail(table_list.eq(1))
            after = get_change_list_detail(table_list.eq(2))
            break

        return before, after

    # 变更信息tds的获取
    def get_change_info_page(self, tds, start_index=1, detail=None):
        if tds is None:
            return {}

        onclick = tds.find('a').attr('onclick')
        if onclick is None or onclick.strip() == '':
            change_model = {
                GsModel.ChangeRecords.CHANGE_ITEM: tds.eq(start_index).text(),
                GsModel.ChangeRecords.BEFORE_CONTENT: util.format_content(tds.eq(start_index + 1).text()),
                GsModel.ChangeRecords.AFTER_CONTENT: util.format_content(tds.eq(start_index + 2).text()),
                GsModel.ChangeRecords.CHANGE_DATE: tds.eq(start_index + 3).text(),
            }
        else:
            before, after = self.get_change_info_detail(onclick.strip(), detail)
            change_model = {
                GsModel.ChangeRecords.CHANGE_ITEM: tds.eq(start_index).text(),
                GsModel.ChangeRecords.BEFORE_CONTENT: before,
                GsModel.ChangeRecords.AFTER_CONTENT: after,
                GsModel.ChangeRecords.CHANGE_DATE: tds.eq(start_index + 2).text(),
            }
        return change_model

    # 变更信息 done
    def get_change_info(self, change_info):
        change_info_dict = {}
        page_list = self.get_crawl_page(change_info, True)
        page_detail = self.get_crawl_page(change_info, True, Model.type_detail)
        change_records_list = []
        for page in page_list:
            trs = PyQuery(page.get(u'text', u''), parser='html').find('#table2').find('tr[width!="95%"]').items()
            for tr in trs:
                tds = tr.find('td')
                if len(tds) < 4:
                    continue
                change_model = self.get_change_info_page(tds, detail=page_detail)
                if len(change_model) > 0:
                    change_records_list.append(change_model)

        if len(change_records_list) > 0:
            change_info_dict[GsModel.CHANGERECORDS] = change_records_list
        return change_info_dict

    # 主要人员 done
    def get_key_person_info(self, key_person_info):
        key_person_info_dict = {}
        page = self.get_crawl_page(key_person_info)

        items = PyQuery(page, parser='html').find('#table2').items()
        key_person_list = []
        for item in items:
            trs = item.find('tr')
            key_person = {
                GsModel.KeyPerson.KEY_PERSON_NAME: trs.eq(0).text().strip(),
                GsModel.KeyPerson.KEY_PERSON_POSITION: trs.eq(1).text().strip()}
            key_person_list.append(key_person)

        if len(key_person_list) > 0:
            key_person_info_dict[GsModel.KEY_PERSON] = key_person_list
        return key_person_info_dict

    # 分支机构 done
    def get_branch_info(self, branch_info):
        branch_info_dict = {}
        page = self.get_crawl_page(branch_info)
        items = PyQuery(page, parser='html').find('.detailsList').items()
        branch_list = []
        for item in items:
            trs = item.find('tr')
            code_array = trs.eq(1).text().split('：')
            code = code_array[1] if len(code_array) == 2 else None
            reg_addr_array = trs.eq(2).text().split('：')
            reg_addr = reg_addr_array[1] if len(reg_addr_array) == 2 else None
            branch_model = {
                GsModel.Branch.COMPAY_NAME: trs.eq(0).text(),
                GsModel.Branch.CODE: code,
                GsModel.Branch.REGISTERED_ADDRESS: reg_addr
            }
            branch_list.append(branch_model)

        if len(branch_list) > 0:
            branch_info_dict[GsModel.BRANCH] = branch_list

        return branch_info_dict

    # 出资信息
    def get_contributive_info(self, contributive_info):
        contributive_info_dict = {}
        pages_list = self.get_crawl_page(contributive_info, True)

        part_a_con = {}
        for page in pages_list:
            trs = PyQuery(page.get(u'text', u''), parser='html').find('#iframeFrame').find('table').find('tr').items()
            i = 1
            for tr in trs:
                # 略过第一行
                if i == 1:
                    i += 1
                    continue
                tds = tr.find('td')
                sub_model = self._get_sharehold_info_list_td_text(tds)
                if sub_model is None or len(sub_model) <= 0:
                    continue
                part_a_con[tds.eq(1).text().strip()] = sub_model

        # 有些企业没有详情信息
        pages_detail = self.get_crawl_page(contributive_info, True, Model.type_detail)
        part_b_con = {}
        if pages_detail is not None:
            for page in pages_detail:
                tables = PyQuery(page.get(u'text', u''), parser='html').find('.table-result').items()
                shareholder_name, sub_model = self._get_sharehold_detail(tables)
                part_b_con[shareholder_name] = sub_model

        con_list = []
        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            con_list.append(v_list)

        if len(con_list) > 0:
            contributive_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = con_list
        return contributive_info_dict

    # todo 年报抓取存储有一定问题 广西桂冠电力股份有限公司
    def get_annual_info(self, annual_item_list):
        return ParseGuangXiAnnual(annual_item_list, self.log).get_result()


# 年报解析类
class ParseGuangXiAnnual(object):
    def __init__(self, annual_item_list, log):
        self.annual_info_dict = {}
        if not isinstance(annual_item_list, list) or len(annual_item_list) <= 0:
            return

        self.log = log
        self.annual_item_list = annual_item_list

        # 分发解析
        self.dispatch()

    def dispatch(self):
        for item in self.annual_item_list:
            url = item.get('url', None)
            text = item.get('text', None)
            status = item.get('status', None)
            if url is None:
                self.log.error('url = null')
                continue
            if text is None:
                self.log.error('text = null')
                continue
            if status != 'success':
                self.log.error('status = {status}'.format(status=status))
                continue

            # 基本信息 字典
            if 'qynbxxList' in url or 'nznbxx' in url:
                self.annual_info_dict.update(self.get_base_info(text))

            # 企业资产状况 字典
            if 'qynbxxList' in url or 'nznbxx' in url:
                self.annual_info_dict.update(self.get_asset_info(text))

            # 股东出资 数组
            if 'gdcz_bj' in url:
                lst = self.get_gdcz_info(text)
                if len(lst) > 0:
                    if AnnualReports.SHAREHOLDER_INFORMATION not in self.annual_info_dict:
                        self.annual_info_dict[AnnualReports.SHAREHOLDER_INFORMATION] = lst
                    else:
                        self.annual_info_dict[AnnualReports.SHAREHOLDER_INFORMATION].extend(lst)

            # 对外担保 数组
            if 'qydwdb_bj' in url:
                lst = self.get_dwdb_info(text)
                if len(lst) > 0:
                    if AnnualReports.OUT_GUARANTEE_INFO not in self.annual_info_dict:
                        self.annual_info_dict[AnnualReports.OUT_GUARANTEE_INFO] = lst
                    else:
                        self.annual_info_dict[AnnualReports.OUT_GUARANTEE_INFO].extend(lst)

            # 对外投资 数组
            if 'dwtz_bj' in url:
                lst = self.get_dwtz_info(text)
                if len(lst) > 0:
                    if AnnualReports.INVESTED_COMPANIES not in self.annual_info_dict:
                        self.annual_info_dict[AnnualReports.INVESTED_COMPANIES] = lst
                    else:
                        self.annual_info_dict[AnnualReports.INVESTED_COMPANIES].extend(lst)

            # 网站网点 数组
            if 'wz_bj' in url:
                lst = self.get_wzwd_info(text)
                if len(lst) > 0:
                    if AnnualReports.WEBSITES not in self.annual_info_dict:
                        self.annual_info_dict[AnnualReports.WEBSITES] = lst
                    else:
                        self.annual_info_dict[AnnualReports.WEBSITES].extend(lst)

            # 修改信息 数组
            if 'qybg_bj' in url:
                lst = self.get_qybg_info(text)
                if len(lst) > 0:
                    if AnnualReports.EDIT_CHANGE_INFOS not in self.annual_info_dict:
                        self.annual_info_dict[AnnualReports.EDIT_CHANGE_INFOS] = lst
                    else:
                        self.annual_info_dict[AnnualReports.EDIT_CHANGE_INFOS].extend(lst)

            # 行政许可 数组
            if 'queryXzxk' in url:
                lst = self.get_xzxk_info(text)
                if len(lst) > 0:
                    if AnnualReports.ADMIN_LICENSE_INFO not in self.annual_info_dict:
                        self.annual_info_dict[AnnualReports.ADMIN_LICENSE_INFO] = lst
                    else:
                        self.annual_info_dict[AnnualReports.ADMIN_LICENSE_INFO].extend(lst)

            # 分支机构 数组
            if 'queryFzjg' in url:
                lst = self.get_fzjg_info(text)
                if len(lst) > 0:
                    if AnnualReports.BRANCH_INFO not in self.annual_info_dict:
                        self.annual_info_dict[AnnualReports.BRANCH_INFO] = lst
                    else:
                        self.annual_info_dict[AnnualReports.BRANCH_INFO].extend(lst)

    # 基本信息
    def get_base_info(self, page_text):

        base_dict = {}
        table_list = PyQuery(page_text, parser='html').find('.detail-list1.qy-list').items()
        for table in table_list:

            text = table.text()
            if '注册号' in text or '联系电话' in text or '社会信用代码' in text:
                py_items = table.find('td').items()

                for item in py_items:
                    item_content = item.text().replace(u'·', u'')
                    if item_content.find('：') == -1:
                        continue
                    part = item_content.split(u'：', 1)
                    k = AnnualReports.format_base_model(part[0].strip().replace(u' ', u''))
                    base_dict[k] = part[1].strip()
                break

        return base_dict

    # 行政许可
    def get_xzxk_info(self, page_text):
        lst = []

        trs = PyQuery(page_text, parser='html').find('tr').items()
        for tr in trs:
            tds = tr.find('td')
            if tds is None or len(tds) < 2:
                continue

            model = {
                AnnualReports.AdminLicenseInfo.LICENSE_NAME: tds.eq(0).text(),
                AnnualReports.AdminLicenseInfo.LICENSE_PERIOD_DATE: tds.eq(1).text(),
            }
            lst.append(model)
        return lst

    # 分支机构
    def get_fzjg_info(self, page_text):
        lst = []

        trs = PyQuery(page_text, parser='html').find('tr').items()
        for tr in trs:
            tds = tr.find('td')
            if tds is None or len(tds) < 2:
                continue

            model = {
                AnnualReports.BranchInfo.BRANCH_NAME: tds.eq(0).text(),
                AnnualReports.BranchInfo.BRANCH_CODE: tds.eq(1).text(),
            }
            lst.append(model)

        return lst

    # 企业资产状况
    def get_asset_info(self, page_text):
        asset_dict = {}
        jq = PyQuery(page_text, parser='html')
        table_result = jq.find('.table-result').items()
        for table in table_result:

            text = table.text()
            if '纳税' in text or '利润' in text or '销售' in text or '负债' in text:
                ths = table.find('th')
                tds = table.find('td')

                lst_value = tds.map(lambda i, e: PyQuery(e).text())
                lst_title = ths.map(lambda i, e: PyQuery(e).text())
                map_title_value = zip(lst_title, lst_value)
                model = {}
                for k_title, v_value in map_title_value:
                    if k_title.strip() == '':
                        continue
                    k = AnnualReports.format_asset_model(k_title)
                    model[k] = v_value

                if len(model) > 0:
                    asset_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = model

        return asset_dict

    # 修改信息
    def get_qybg_info(self, page_text):

        lst = []

        trs = PyQuery(page_text, parser='html').find('tr').items()
        for tr in trs:
            tds = tr.find('td')
            if tds is None or len(tds) < 5:
                continue

            edit_model = {
                AnnualReports.EditChangeInfos.CHANGE_ITEM: tds.eq(1).text(),
                AnnualReports.EditChangeInfos.BEFORE_CONTENT: tds.eq(2).text(),
                AnnualReports.EditChangeInfos.AFTER_CONTENT: tds.eq(3).text(),
                AnnualReports.EditChangeInfos.CHANGE_DATE: tds.eq(4).text(),
            }
            lst.append(edit_model)
        return lst

    # 股东出资
    def get_gdcz_info(self, page_text):
        lst = []

        trs = PyQuery(page_text, parser='html').find('tr').items()
        for tr in trs:
            tds = tr.find('td')
            if tds is None or len(tds) < 7:
                continue

            share_model = {
                AnnualReports.ShareholderInformation.SHAREHOLDER_NAME: tds.eq(1).text(),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(tds.eq(2).text()),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TIME: tds.eq(3).text(),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TYPE: tds.eq(4).text(),
                AnnualReports.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(tds.eq(5).text()),
                AnnualReports.ShareholderInformation.PAIED_TIME: tds.eq(6).text(),
                AnnualReports.ShareholderInformation.PAIED_TYPE: tds.eq(7).text(),
            }
            lst.append(share_model)
        return lst

    # 对外担保
    def get_dwdb_info(self, page_text):
        lst = []

        trs = PyQuery(page_text, parser='html').find('tr').items()
        for tr in trs:
            tds = tr.find('td')
            if tds is None or len(tds) < 7:
                continue

            model = {
                AnnualReports.OutGuaranteeInfo.CREDITOR: tds.eq(0).text(),
                AnnualReports.OutGuaranteeInfo.OBLIGOR: tds.eq(1).text(),
                AnnualReports.OutGuaranteeInfo.DEBT_TYPE: tds.eq(2).text(),
                AnnualReports.OutGuaranteeInfo.DEBT_AMOUNT: util.get_amount_with_unit(tds.eq(3).text()),
                AnnualReports.OutGuaranteeInfo.PERFORMANCE_PERIOD: tds.eq(4).text(),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_PERIOD: tds.eq(5).text(),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_TYPE: tds.eq(6).text(),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_PURVIEW: tds.eq(7).text(),
            }
            lst.append(model)
        return lst

    # 对外投资
    def get_dwtz_info(self, page_text):
        lst = []
        table_list = PyQuery(page_text, parser='html').find('#touziren').items()
        for table in table_list:
            tr_list = table.find('tr').items()
            model = {}
            for tr in tr_list:
                name_selector = tr.find('.ths')
                if len(name_selector) >= 1:
                    model[AnnualReports.InvestedCompanies.COMPANY_NAME] = name_selector.text()
                    continue
                text_array = tr.text().split(u'：')
                if len(text_array) >= 2:
                    model[AnnualReports.InvestedCompanies.CODE] = text_array[1]
            lst.append(model)
        return lst

    # 网站网点
    def get_wzwd_info(self, page_text):
        web_item = {}
        jq = PyQuery(page_text, parser='html')
        item_list = jq.find('#touziren').find('tr').items()
        for item in item_list:
            if len(item.find('.ths')) == 1:
                web_item[AnnualReports.WebSites.NAME] = item.text()
                continue
            item_content = item.text().replace(u'·', u'')
            part = item_content.split(u'：', 2)
            k = AnnualReports.format_website_model(part[0].strip().replace(u' ', u''))
            web_item[k] = part[1].strip()
        return [web_item]

    def get_result(self):
        return self.annual_info_dict
