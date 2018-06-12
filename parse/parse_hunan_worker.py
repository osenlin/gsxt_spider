#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: parse_base_worker.py
@time: 2017/2/3 17:32
"""
import re
import traceback

from pyquery import PyQuery

from base.parse_base_worker import ParseBaseWorker
from common import util
from common.annual_field import *
from common.global_field import Model
from common.gsxt_field import *


# todo 股东信息,年报
# todo 乐山市五通桥区芋香源芋头专业合作社 格式不一致 需要添加解析功能
class GsxtParseHuNanWorker(ParseBaseWorker):
    def __init__(self, **kwargs):
        ParseBaseWorker.__init__(self, **kwargs)
        self.chuzi_type = {
            "1": "货币",
            "2": "实物",
            "3": "知识产权",
            "4": "债权",
            "5": "高新技术成果",
            "6": "土地使用权",
            "7": "股权",
            "8": "劳务",
            "9": "其他"
        }

    # 基本信息
    def get_base_info(self, base_info):
        page = self.get_crawl_page(base_info)
        res = PyQuery(page, parser='html').find('.tableYyzz').find('td').items()
        base_info_dict = self._get_base_item_info(res)
        # 变更信息
        try:
            change_info_dict = self.get_inline_change_info(page)
        except:
            self.log.error(
                'company:{0},error-part:changerecords_info,error-info:{1}'.format(base_info_dict.get('company', ''),
                                                                                  traceback.format_exc()))
            change_info_dict = {}
        base_info_dict.update(change_info_dict)
        return base_info_dict

    # 股东信息
    def get_shareholder_info(self, shareholder_info):
        shareholder_list = []
        shareholder_info_dict = {}
        page = self.get_crawl_page(shareholder_info)
        inv_list = util.get_match_value('investor.inv = "', '";', page, True)
        sub_amount_list = util.get_match_value('invt.subConAm = "', '";', page, True)
        paid_amount_list = util.get_match_value('nvtActl.acConAm =  "', '";', page, True)
        sub_time_list = map(lambda e: e, util.get_match_value("invt.conDate = '", "';", page, True))
        sub_con_form = map(lambda e: self.chuzi_type.get(e, u''),
                           util.get_match_value('invt.conForm = "', '";', page, True))
        sub_pub_time = map(lambda e: e,
                           util.get_match_value("invt.noticeDate = '", "';", page, True))
        paid_time_list = map(lambda e: e,
                             util.get_match_value("invtActl.conDate = '", "';", page, True))
        paid_con_form = map(lambda e: self.chuzi_type.get(e, u''),
                            util.get_match_value('invtActl.conForm = "', '";', page, True))
        paid_pub_time = map(lambda e: e,
                            util.get_match_value("invtActl.noticeDate = '", "';", page, True))
        lst = zip(inv_list, sub_amount_list, paid_amount_list, sub_con_form, sub_amount_list, sub_time_list,
                  sub_pub_time, paid_con_form, paid_amount_list, paid_time_list, paid_pub_time)
        for item in lst:
            if item[0].strip() == '' and item[1].strip() == '' and item[2].strip() == '':
                continue
            share_model = {
                GsModel.ShareholderInformation.SHAREHOLDER_NAME: item[0],
                GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(item[1]),
                GsModel.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(item[2]),
                GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL: [
                    {
                        GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TYPE: item[3],
                        GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                            item[1]),
                        GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TIME: item[5],
                        GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_PUBLISH_TIME: item[6],
                    }
                ],
                GsModel.ShareholderInformation.PAIED_DETAIL: [
                    {
                        GsModel.ShareholderInformation.PaiedDetail.PAIED_TYPE: item[7],
                        GsModel.ShareholderInformation.PaiedDetail.PAIED_AMOUNT: util.get_amount_with_unit(item[2]),
                        GsModel.ShareholderInformation.PaiedDetail.PAIED_TIME: item[9],
                        GsModel.ShareholderInformation.PaiedDetail.PAIED_PUBLISH_TIME: item[10],
                    }
                ]
            }
            shareholder_list.append(share_model)
        # if len(shareholder_list) > 0:
        shareholder_info_dict[GsModel.SHAREHOLDER_INFORMATION] = shareholder_list
        return shareholder_info_dict

    # 变更信息
    # 客户端分页和基本信息同一页
    def get_inline_change_info(self, page):
        if isinstance(page, dict) or page is None:
            return {}
        change_info_dict = {}
        lst_change_records = []
        trs = PyQuery(page, parser='html').find('#a_alter_table').find('tr').items()
        i = 1
        for tr in trs:
            tds = tr.find('td')
            if i == 1 or len(tds) != 5:
                i += 1
                continue
            change_model = self._get_change_info_td_text(tds)
            if len(change_model) > 0:
                lst_change_records.append(change_model)
        change_info_dict[GsModel.CHANGERECORDS] = lst_change_records
        return change_info_dict

    # 主要人员
    def get_key_person_info(self, key_person_info):
        key_person_info_dict = {}
        page = self.get_crawl_page(key_person_info)
        items = PyQuery(page, parser='html').find('.dlPiece').find('td').items()
        lst_key_person = []
        for item in items:
            lis = item.find('li')
            key_person = {
                GsModel.KeyPerson.KEY_PERSON_NAME: lis.eq(0).text().strip(),
                GsModel.KeyPerson.KEY_PERSON_POSITION: lis.eq(1).text().strip()}
            lst_key_person.append(key_person)

        key_person_info_dict[GsModel.KEY_PERSON] = lst_key_person
        return key_person_info_dict

    # 分支机构
    def get_branch_info(self, branch_info):
        branch_info_dict = {}
        page = self.get_crawl_page(branch_info)
        items = PyQuery(page, parser='html').find('.dlPiece2').find('td').items()
        lst_branch = []
        for item in items:
            lis = item.find('li')
            code_array = lis.eq(1).text().split('：')
            reg_addr_array = lis.eq(2).text().split('：')
            code = code_array[1] if len(code_array) == 2 else None
            reg_addr = reg_addr_array[1] if len(reg_addr_array) == 2 else None
            branch_model = {
                GsModel.Branch.COMPAY_NAME: lis.eq(0).text(),
                GsModel.Branch.CODE: code,
                GsModel.Branch.REGISTERED_ADDRESS: reg_addr
            }
            lst_branch.append(branch_model)
        branch_info_dict[GsModel.BRANCH] = lst_branch
        return branch_info_dict

    # 获取出资 信息详细列表
    def _get_contributive_detail(self, page, name_dom="th"):
        shareholder_name = ""
        sub_model = {}
        text = page.get(u'text', u'')
        if text == u'':
            return shareholder_name, sub_model

        tables = PyQuery(text, parser='html').find('.content2').items()
        if tables is None:
            return shareholder_name, sub_model

        invtall_pattern = 'invtAll \+= parseFloat\(\'(.*?)\'\);'
        invtactlall_pattern = 'invtActlAll \+= parseFloat\(\'(.*?)\'\);'

        for table in tables:
            th_text = table.find(name_dom).text()
            if u'发起人' in th_text \
                    or u'股东名称' in th_text \
                    or u'股东及出资人名称' in th_text \
                    or u'股东' in th_text:
                tds = table.find('td')
                steps = xrange(3) if name_dom == "th" else [i * 2 + 1 for i in xrange(3)]
                shareholder_name = tds.eq(steps[0]).text().strip().replace(u'.', u'')
                sub_model[GsModel.ContributorInformation.SHAREHOLDER_NAME] = tds.eq(steps[0]).text()
                search_list = re.findall(invtall_pattern, text)
                if len(search_list) > 0:
                    sub_amount = search_list[0]
                else:
                    sub_amount = u''

                search_list = re.findall(invtactlall_pattern, text)
                if len(search_list) > 0:
                    paied_amount = search_list[0]
                else:
                    paied_amount = u''
                sub_model[GsModel.ContributorInformation.SUBSCRIPTION_AMOUNT] = util.get_amount_with_unit(
                    sub_amount)
                sub_model[GsModel.ContributorInformation.PAIED_AMOUNT] = util.get_amount_with_unit(
                    paied_amount)
            if u'认缴出资方式' in th_text:
                trs = table.find('tr')
                lst_sub_detail = []
                for tr_i in xrange(1, len(trs)):
                    tds = trs.eq(tr_i).find('td')
                    if len(tds) <= 2:
                        continue
                    sub_model_detail = {
                        GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TYPE: tds.eq(0).text(),
                        GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                            tds.eq(1).text()),
                        GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TIME: tds.eq(2).text(),
                    }
                    lst_sub_detail.append(sub_model_detail)
                sub_model[GsModel.ContributorInformation.SUBSCRIPTION_DETAIL] = lst_sub_detail
            if u'实缴出资方式' in th_text:
                trs = table.find('tr')
                lst_paied_detail = []
                for tr_i in xrange(1, len(trs)):
                    tds = trs.eq(tr_i).find('td')
                    if len(tds) <= 2:
                        continue
                    paied_model_detail = {
                        GsModel.ContributorInformation.PaiedDetail.PAIED_TYPE: tds.eq(0).text(),
                        GsModel.ContributorInformation.PaiedDetail.PAIED_AMOUNT: util.get_amount_with_unit(
                            tds.eq(1).text()),
                        GsModel.ContributorInformation.PaiedDetail.PAIED_TIME: tds.eq(2).text(),
                    }
                    lst_paied_detail.append(paied_model_detail)
                sub_model[GsModel.ContributorInformation.PAIED_DETAIL] = lst_paied_detail
        return shareholder_name, sub_model

    # 出资信息工具类 合伙人信息补丁 参考 湖南国开铁路建设私募基金合伙企业(有限合伙)
    def _get_sharehold_info_patch(self, tds, start_index=1):
        if tds is None:
            return {}
        if len(tds) <= start_index + 3:
            return {}
        sub_model = {
            GsModel.ContributorInformation.SHAREHOLDER_TYPE: tds.eq(start_index).text(),
            GsModel.ContributorInformation.SHAREHOLDER_NAME: tds.eq(start_index + 1).text(),
            GsModel.ContributorInformation.CERTIFICATE_TYPE: tds.eq(start_index + 2).text(),
            GsModel.ContributorInformation.CERTIFICATE_NO: tds.eq(start_index + 3).text()
        }
        return sub_model

    # 出资信息
    # 客户端分页和基本信息同一页,缺少详情页
    def get_contributive_info(self, contributive_info):
        contributive_info_dict = {}
        pages_list = self.get_crawl_page(contributive_info, True)
        part_a_con = {}
        is_patch = False
        for page in pages_list:

            label = PyQuery(page.get(u'text', u''), parser='html').find('#layout-01_01_02')
            if label is None or len(label) <= 0:
                label = PyQuery(page.get(u'text', u''), parser='html').find('#layout-01_01_07')
                is_patch = True

            trs = label.find('table').find(
                'tr').items()

            for tr in trs:
                tds = tr.find('td')
                if not is_patch:
                    sub_model = self._get_sharehold_info_list_td_text(tds)
                else:
                    sub_model = self._get_sharehold_info_patch(tds)
                if sub_model is None or len(sub_model) <= 0:
                    continue

                if not is_patch:
                    part_a_con[tds.eq(1).text().strip()] = sub_model
                else:
                    part_a_con[tds.eq(2).text().strip()] = sub_model

        pages_detail = self.get_crawl_page(contributive_info, True, Model.type_detail)
        part_b_con = {}
        if pages_detail is not None:
            for page in pages_detail:
                shareholder_name, sub_model = self._get_contributive_detail(page)
                part_b_con[shareholder_name] = sub_model
        con_list = []
        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            con_list.append(v_list)

        contributive_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = con_list
        return contributive_info_dict

    # 年报信息
    def get_annual_info(self, annual_item_list):
        annual_info_dict = {}
        # 湖南一个年报只有一个页面
        if annual_item_list is None or len(annual_item_list) <= 0:
            return {}

        annual_item = annual_item_list[0]

        page = annual_item.get(u'text', u'')
        py_all = PyQuery(page, parser='html')
        # 基本信息
        base_info__tds = py_all.find('.tableYyzz').find('td').items()
        annual_base_info = self._get_annual_base_info(base_info__tds)
        annual_info_dict.update(annual_base_info)

        # web_site_info
        py_websites = py_all.find('.dlPiece3').find('ul').items()
        lst_websites = []
        for py_web_item in py_websites:
            ps = py_web_item.find('li').items()
            web_item = self._get_annual_web_site_info(ps)
            lst_websites.append(web_item)
        annual_info_dict[AnnualReports.WEBSITES] = lst_websites

        divs = py_all.find('.content1').items()
        for item_div in divs:
            content = item_div.find('h1').text()
            if u'股东及出资信息' in content:
                trs = item_div.find('table').find('.page-item').items()
                annual_info_dict[AnnualReports.SHAREHOLDER_INFORMATION] = self._get_annual_sharehold_info(trs)

            if u'行政许可信息' in content:
                trs = item_div.find('table').find('tr').items()
                annual_info_dict[AnnualReports.ADMIN_LICENSE_INFO] = self._get_annual_license_info(trs)

            # 年报 对外投资信息
            if u'对外投资信息' in content:
                tds = item_div.find('.dlPiece4').find('ul').items()
                annual_info_dict[AnnualReports.INVESTED_COMPANIES] = self._get_annual_inv_info(tds, default_dom='li')

            if u'分支机构信息' in content:
                tds = item_div.find('.dlPiece2').find('ul').items()
                annual_info_dict[AnnualReports.BRANCH_INFO] = self._get_annual_branch_info(tds)

            if u'企业资产状况信息' in content or u'资产状况信息' in content:
                ths = item_div.find('th')
                tds = item_div.find('td')
                annual_info_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = self._get_annual_asset_info(ths,
                                                                                                                  tds)
            if u'对外提供保证担保信息' in content:
                trs = item_div.find('table').find('.page-item').items()
                annual_info_dict[AnnualReports.OUT_GUARANTEE_INFO] = self._get_annual_sharehold_info(trs)

            if u'修改信息' in content:
                trs = item_div.find('table').find('.page-item').items()
                annual_info_dict[AnnualReports.EDIT_CHANGE_INFOS] = self._get_annual_edit_change(trs)

            if u'股权变更信息' in content:
                trs = item_div.find('table').find('.page-item').items()
                annual_info_dict[
                    AnnualReports.EDIT_SHAREHOLDING_CHANGE_INFOS] = self._get_annual_edit_shareholding_change(trs)
        return annual_info_dict

    # 年报网站信息
    def _get_annual_web_site_info(self, py_items):
        web_item = {}
        for item in py_items:
            if len(item.find('span')) == 0:
                web_item[AnnualReports.WebSites.NAME] = item.text()
            else:
                item_content = item.text().replace(u'·', u'')
                if item_content.find('：') == -1:
                    continue

                part = item_content.split(u'：', 1)
                k = AnnualReports.format_website_model(part[0].strip())
                web_item[k] = part[1].strip()
        return web_item

    # 行政许可信息
    def _get_annual_license_info(self, trs):
        if trs is None:
            return None

        lst = []
        for tr in trs:
            tds = tr.find('td')
            if tds is None or len(tds) < 2:
                continue

            license_model = {
                AnnualReports.AdminLicenseInfo.LICENSE_NAME: tds.eq(0).text(),
                AnnualReports.AdminLicenseInfo.LICENSE_PERIOD_DATE: tds.eq(1).text(),
            }
            lst.append(license_model)
        return lst

    # 年报 股东出资信息(客户端分页)
    # todo 如果是有多页 怎么处理
    def _get_annual_sharehold_info(self, trs):

        if trs is None:
            return None

        lst = []
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

    # 年报
    def _get_annual_out_guarantee_info(self, trs):
        if trs is None:
            return None

        lst = []
        for tr in trs:
            tds = tr.find('td')
            share_model = {
                AnnualReports.OutGuaranteeInfo.CREDITOR: tds.eq(1).text(),
                AnnualReports.OutGuaranteeInfo.OBLIGOR: tds.eq(2).text(),
                AnnualReports.OutGuaranteeInfo.DEBT_TYPE: tds.eq(3).text(),
                AnnualReports.OutGuaranteeInfo.DEBT_AMOUNT: util.get_amount_with_unit(tds.eq(4).text()),
                AnnualReports.OutGuaranteeInfo.PERFORMANCE_PERIOD: tds.eq(5).text(),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_PERIOD: tds.eq(6).text(),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_TYPE: tds.eq(7).text(),
            }
            lst.append(share_model)
        return lst

    # 年报基本信息
    def _get_annual_base_info(self, py_items):
        annual_base_info_dict = {}
        for item in py_items:
            item_content = item.text().replace(u'·', u'')
            if item_content.find('：') == -1:
                continue
            part = item_content.split(u'：', 1)
            k = AnnualReports.format_base_model(part[0].strip().replace(u' ', u''))
            annual_base_info_dict[k] = part[1].strip()
        return annual_base_info_dict

    # 年报 对外投资信息
    def _get_annual_inv_info(self, py_items, default_dom="p"):
        lst = []
        for item in py_items:
            company = item.find(default_dom).eq(0).text()
            code_array = item.find(default_dom).eq(1).text().split(u'：')
            code = code_array[1] if len(code_array) == 2 else None
            model = {AnnualReports.InvestedCompanies.COMPANY_NAME: company,
                     AnnualReports.InvestedCompanies.CODE: code}
            lst.append(model)
        return lst

    # 年报 分支机构
    def _get_annual_branch_info(self, py_items, default_dom="li"):
        lst = []
        for item in py_items:
            model = {}
            dom_list = item.find(default_dom).items()
            for dom in dom_list:
                text = dom.text().replace(u'·', u'')
                if text.find('：') == -1:
                    continue
                part = text.split(u'：', 1)
                k = AnnualReports.format_branch_model(part[0].strip().replace(u' ', u''))
                model[k] = part[1].strip()
            lst.append(model)
        return lst

    # 年报 企业资产状况信息
    def _get_annual_asset_info(self, ths, tds):
        lst_value = tds.map(lambda i, e: PyQuery(e).text())
        lst_title = ths.map(lambda i, e: PyQuery(e).text())
        map_title_value = zip(lst_title, lst_value)
        model = {}
        for k_title, v_value in map_title_value:
            if k_title == '': continue
            k = AnnualReports.format_asset_model(k_title)
            model[k] = v_value
        return model

    # 年报 修改记录 湖南鼎懋科技有限公司
    def _get_annual_edit_change(self, trs):
        if trs is None:
            return None

        lst = []
        for tr in trs:
            tds = tr.find('td')
            edit_model = {
                AnnualReports.EditChangeInfos.CHANGE_ITEM: tds.eq(1).text(),
                AnnualReports.EditChangeInfos.BEFORE_CONTENT: tds.eq(2).text(),
                AnnualReports.EditChangeInfos.AFTER_CONTENT: tds.eq(3).text(),
                AnnualReports.EditChangeInfos.CHANGE_DATE: tds.eq(4).text(),
            }
            lst.append(edit_model)
        return lst

    # 年报 股权变更
    def _get_annual_edit_shareholding_change(self, trs):
        if trs is None:
            return None

        lst = []
        for tr in trs:
            tds = tr.find('td')
            edit_model = {
                AnnualReports.EditShareholdingChangeInfos.SHAREHOLDER_NAME: tds.eq(1).text(),
                AnnualReports.EditShareholdingChangeInfos.BEFORE_CONTENT: tds.eq(2).text(),
                AnnualReports.EditShareholdingChangeInfos.AFTER_CONTENT: tds.eq(3).text(),
                AnnualReports.EditShareholdingChangeInfos.CHANGE_DATE: tds.eq(4).text(),
            }
            lst.append(edit_model)
        return lst
