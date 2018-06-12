#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: parse_base_worker.py
@time: 2017/2/3 17:32
"""

import hashlib
import string

from pyquery import PyQuery

from base.parse_base_worker import ParseBaseWorker
from common import util
from common.annual_field import *
from common.global_field import Model
from common.gsxt_field import *

'''
主要人员映射失败企业:
三星（中国）投资有限公司天津分公司 56f69ee518c98ba3db1ba779f2c2e642
天津市津南经济开发区总公司 56f69ee518c98ba3db1ba779f2c2e642

'''


class GsxtParseTianJinWorker(ParseBaseWorker):
    def __init__(self, **kwargs):
        ParseBaseWorker.__init__(self, **kwargs)
        # 必须反馈抓取情况到种子列表
        self.report_status = self.REPORT_SEED

    # 基本信息
    def get_base_info(self, base_info):
        base_info_dict = {}

        page = self.get_crawl_page(base_info)
        if page is None:
            return base_info_dict

        dls = PyQuery(page, parser='html').find('.overview').find('dl').items()
        for dl in dls:
            dt = dl.find('dt').text()
            dd = dl.find('dd').text()
            dt = dt.replace('：', '')
            dt = dt.replace(':', '')
            k = GsModel.format_base_model(dt)
            base_info_dict[k] = dd
        base_info_dict[GsModel.PERIOD] = u"{0}至{1}".format(
            base_info_dict.get(GsModel.PERIOD_FROM, ''), base_info_dict.get(GsModel.PERIOD_TO, ''))
        return base_info_dict

    # 股东信息
    def get_shareholder_info(self, shareholder_info):
        shareholder_info_dict = {}
        lst_shareholder = []

        page_list = self.get_crawl_page(shareholder_info, multi=True)
        for page in page_list:
            if page is None:
                continue

            text = page.get("text", "{}")
            if text is None:
                continue

            json_data = util.json_loads(text)
            if json_data is None:
                continue

            data_arr = json_data.get('data', [])
            if data_arr is None:
                continue

            for data in data_arr:

                share_model = {
                    GsModel.ShareholderInformation.SHAREHOLDER_NAME: str(data.get(u'inv', u'')),
                    GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT: str(util.get_amount_with_unit(
                        data.get(u'subSum', u''))),
                    GsModel.ShareholderInformation.PAIED_AMOUNT: str(util.get_amount_with_unit(
                        data.get(u'aubSum', u''))),
                }

                # 无真实数据的测试下
                sub_detail_list = data.get('subDetails', u'')
                aub_details_list = data.get('aubDetails', u'')
                for sub_detail_item in sub_detail_list:
                    sub_detail_dict = {
                        GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                            sub_detail_item.get('subConAmStr', u'')),
                        GsModel.ShareholderInformation.SHAREHOLDER_NAME: str(
                            sub_detail_item.get('subConFormName', u'')),
                        GsModel.ShareholderInformation.SUBSCRIPTION_TYPE: str(
                            sub_detail_item.get('subConForm_CN', u'')),
                        # 认缴方式
                        GsModel.ShareholderInformation.SUBSCRIPTION_TIME: str(sub_detail_item.get('conDate', u'')),
                        # 认缴时间
                        GsModel.ShareholderInformation.SUBSCRIPTION_PUBLISH_TIME: str(
                            sub_detail_item.get('publicDate', u'')),  # 认缴公式时间
                    }
                    if GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL in share_model:
                        share_model[GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL].append(sub_detail_dict)
                    else:
                        share_model[GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL] = [sub_detail_dict]

                for aub_details_item in aub_details_list:
                    paid_detail_dict = {
                        GsModel.ShareholderInformation.PAIED_AMOUNT: str(util.get_amount_with_unit(
                            aub_details_item.get('acConAmStr', u''))),
                        GsModel.ShareholderInformation.SHAREHOLDER_NAME: str(
                            aub_details_item.get('acConFormName', u'')),
                        GsModel.ShareholderInformation.PAIED_TYPE: str(aub_details_item.get('acbConForm_CN', u'')),
                        # 实缴类型
                        GsModel.ShareholderInformation.PAIED_TIME: str(aub_details_item.get('conDate', u'')),  # 实缴 时间
                        GsModel.ShareholderInformation.PAIED_PUBLISH_TIME: str(aub_details_item.get('publicDate', u'')),
                        # 实缴公式时间
                    }
                    if GsModel.ShareholderInformation.PAIED_DETAIL in share_model:
                        share_model[GsModel.ShareholderInformation.PAIED_DETAIL].append(paid_detail_dict)
                    else:
                        share_model[GsModel.ShareholderInformation.PAIED_DETAIL] = [paid_detail_dict]
                if len(share_model) > 0:
                    lst_shareholder.append(share_model)

        shareholder_info_dict[GsModel.SHAREHOLDER_INFORMATION] = lst_shareholder
        return shareholder_info_dict

    # 变更信息
    def get_change_info(self, change_info):
        change_info_dict = {}
        change_info_list = []
        pages = self.get_crawl_page(change_info, True)
        if pages is None or len(pages) <= 0:
            return change_info_dict

        page = pages[0]
        page_text = page.get(u'text', u'')
        json_data = util.json_loads(page_text)
        if json_data is None:
            return change_info_dict
        data_arr = json_data.get('data', [])
        if data_arr is None:
            return change_info_dict

        for data in data_arr:
            # 转为网页格式
            html_alt_item = data.get('altItem_CN', '')
            alt_item = ''
            if html_alt_item != '':
                alt_item = PyQuery(html_alt_item, parser='html').remove('div').remove("span") \
                    .text().replace(' ', '').strip()
            change_model = {
                GsModel.ChangeRecords.CHANGE_ITEM: alt_item,
                GsModel.ChangeRecords.BEFORE_CONTENT: data.get('altAf', u''),
                GsModel.ChangeRecords.AFTER_CONTENT: data.get('altBe', u''),
                GsModel.ChangeRecords.CHANGE_DATE: data.get('altDate', u'')
            }
            change_info_list.append(change_model)

        if len(change_info_list) > 0:
            change_info_dict[GsModel.CHANGERECORDS] = change_info_list
        return change_info_dict

    # 主要人员
    def get_key_person_info(self, key_person_info):
        key_person_info_dict = {}
        lst_key_person = []

        page = self.get_crawl_page(key_person_info)
        if page is None or page == u'':
            # 如果是抓取失败或者抓取异常,一律为None,不能影响后面的解析
            return key_person_info_dict

        json_data = util.json_loads(page)
        if json_data is None:
            return key_person_info_dict

        data_arr = json_data.get('data', [])
        if data_arr is None:
            return key_person_info_dict

        for data in data_arr:
            key_person_name_html = data.get('name', '')
            key_person_name = ''
            # 去除html代码
            if key_person_name_html != '':
                key_person_name = PyQuery(key_person_name_html, parser='html').remove('span').remove('div').text()

            # 职位需要进行判断
            key_position_temp = data.get('position_CN', '')
            if string.find(key_position_temp, 'img') != -1:

                pic_md5 = util.get_match_value('"', '"', key_position_temp)
                m = hashlib.md5()
                m.update(pic_md5.strip().replace('\n', ''))
                psw = m.hexdigest()
                key_position = GsModel.get_md5_key_position(psw)
            else:
                key_position = key_position_temp

            if key_position is None:
                key_position = ''

            key_person = {
                GsModel.KeyPerson.KEY_PERSON_NAME: key_person_name.replace(" ", ""),
                GsModel.KeyPerson.KEY_PERSON_POSITION: key_position,
            }
            lst_key_person.append(key_person)

        if len(lst_key_person) > 0:
            key_person_info_dict[GsModel.KEY_PERSON] = lst_key_person

        return key_person_info_dict

    # 分支机构
    def get_branch_info(self, branch_info):
        branch_info_dict = {}
        lst_branch = []
        page = self.get_crawl_page(branch_info)
        if page is None or page == u'':
            return {}
        json_data = util.json_loads(page)
        if json_data is None:
            return branch_info_dict
        data_arr = json_data.get('data', [])
        if data_arr is None:
            return branch_info_dict

        for data in data_arr:
            branch_model = {
                GsModel.Branch.COMPAY_NAME: data.get('brName', ''),
                GsModel.Branch.CODE: data.get('regNo', '')
            }
            lst_branch.append(branch_model)

        if len(lst_branch) > 0:
            branch_info_dict[GsModel.BRANCH] = lst_branch

        return branch_info_dict

    # 解析出资列表页
    @staticmethod
    def contributive_info_list(con_table_list):
        con_table_dict = {}
        if con_table_list is None or len(con_table_list) <= 0:
            return con_table_dict

        for con_item in con_table_list:
            status = con_item.get('status', 'fail')
            if status != 'success':
                break

            text = con_item.get('text')
            if text is None or text == '':
                break

            json_data = util.json_loads(text)
            if json_data is None:
                break

            data_array = json_data.get('data')
            if not isinstance(data_array, list):
                break

            for item in data_array:
                b_lic_no = item.get('bLicNo')
                b_lic_type_cn = item.get('blicType_CN')
                inv = item.get('inv')
                inv_type_cn = item.get('invType_CN')
                inv_id = item.get('invId')
                if inv is None or inv.strip() == '':
                    continue

                if inv_id is None or inv_id.strip() == '':
                    continue

                inv = inv.strip()
                inv_id = inv_id.strip()

                if b_lic_no is not None and b_lic_no.strip() != '':
                    b_lic_no = PyQuery(b_lic_no, parser='html').remove('div').remove('span'). \
                        text().replace(' ', '').strip()
                else:
                    b_lic_no = ''

                if b_lic_type_cn is None or b_lic_no.strip() == '':
                    b_lic_type_cn = ''
                else:
                    b_lic_type_cn = b_lic_type_cn.strip()

                if inv_type_cn is None or inv_type_cn.strip() == '':
                    inv_type_cn = ''
                else:
                    inv_type_cn = PyQuery(inv_type_cn, parser='html').remove('div').remove('span'). \
                        text().replace(' ', '').strip()

                sub_model = {
                    GsModel.ContributorInformation.SHAREHOLDER_NAME: inv,
                    GsModel.ContributorInformation.SHAREHOLDER_TYPE: inv_type_cn,
                    GsModel.ContributorInformation.CERTIFICATE_TYPE: b_lic_type_cn,
                    GsModel.ContributorInformation.CERTIFICATE_NO: b_lic_no
                }
                con_table_dict[inv_id] = sub_model

        return con_table_dict

    # 解析出资详情页
    @staticmethod
    def contributive_info_detail(con_detail_list):
        con_detail_dict = {}
        if con_detail_list is None or len(con_detail_list) <= 0:
            return con_detail_dict

        for con_item in con_detail_list:
            status = con_item.get('status', 'fail')
            if status != 'success':
                break

            text = con_item.get('text')
            if text is None or text == '':
                break

            json_data = util.json_loads(text)
            if json_data is None:
                break

            data_array = json_data.get('data')
            if not isinstance(data_array, list):
                break

            sub_list = []
            ac_list = []
            inv_id = None
            sub_model = {}
            sub_amount = 0
            ac_amount = 0
            for item_list in data_array:
                if not isinstance(item_list, list):
                    continue
                if len(item_list) <= 0:
                    continue

                for item in item_list:

                    # 实缴
                    if 'acId' in item:
                        ac_con_am = item.get('acConAm', 0)
                        if isinstance(ac_con_am, int):
                            ac_amount += ac_con_am
                        paid_model_detail = {
                            GsModel.ContributorInformation.PaiedDetail.PAIED_TYPE:
                                item.get('conForm_CN', ''),
                            GsModel.ContributorInformation.PaiedDetail.PAIED_AMOUNT:
                                util.get_amount_with_unit(ac_con_am),
                            GsModel.ContributorInformation.PaiedDetail.PAIED_TIME:
                                item.get('conDate', '')
                        }
                        inv_id = item.get('invId')
                        ac_list.append(paid_model_detail)
                        continue

                    # 认缴
                    if 'subId' in item:
                        sub_con_am = item.get('subConAm', 0)
                        if isinstance(sub_con_am, int):
                            sub_amount += sub_con_am
                        sub_model_detail = {
                            GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TYPE:
                                item.get('conForm_CN', ''),
                            GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT:
                                util.get_amount_with_unit(sub_con_am),
                            GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TIME:
                                item.get('conDate', '')
                        }
                        inv_id = item.get('invId')
                        sub_list.append(sub_model_detail)
                        continue

            if len(sub_list) > 0:
                sub_model[GsModel.ContributorInformation.SUBSCRIPTION_DETAIL] = sub_list

            if len(ac_list) > 0:
                sub_model[GsModel.ContributorInformation.PAIED_DETAIL] = ac_list

            sub_model[GsModel.ContributorInformation.SUBSCRIPTION_AMOUNT] = util.get_amount_with_unit(sub_amount)
            sub_model[GsModel.ContributorInformation.PAIED_AMOUNT] = util.get_amount_with_unit(ac_amount)

            if inv_id is not None:
                con_detail_dict[inv_id] = sub_model

        return con_detail_dict

    # 出资信息解析
    def get_contributive_info(self, contributive_info):
        con_info_dict = dict()

        con_list = []

        # 分别解析列表与详情页面
        con_table_dict = self.contributive_info_list(contributive_info.get(Model.type_list, []))
        con_detail_dict = self.contributive_info_detail(contributive_info.get(Model.type_detail, []))

        for table_k, table_v in con_table_dict.iteritems():

            for detail_k, detail_v in con_detail_dict.iteritems():
                if table_k in detail_k or detail_k in table_k:
                    table_v.update(detail_v)
                    break

            con_list.append(table_v)

        con_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = con_list
        return con_info_dict

    # 清算信息
    def get_liquidation_info(self, liquidation_info):
        return {}

    # 股权出质登记信息
    def get_equity_pledged_info(self, equity_pledged_info):
        info_dict = {}
        lst_info = []
        page = self.get_crawl_page(equity_pledged_info)
        if page is None or page == u'':
            return {}
        json_data = util.json_loads(page)
        if json_data is None:
            return info_dict
        data_arr = json_data.get('data', [])
        if data_arr is None:
            return info_dict

        for data in data_arr:
            info_model = {
                GsModel.EquityPledgedInfo.REGISTER_NUM: data.get('equityNo', ''),
                GsModel.EquityPledgedInfo.MORTGAGOR: data.get('pledgor', ''),
                GsModel.EquityPledgedInfo.MORTGAGOR_NUM: data.get('pledBLicNo', ''),
                GsModel.EquityPledgedInfo.PLEDGE_STOCK_AMOUNT: data.get('impAm', ''),
                GsModel.EquityPledgedInfo.PLEDGEE: data.get('impOrg', ''),
                GsModel.EquityPledgedInfo.PLEDGEE_NUM: data.get('impOrgBLicNo', ''),
                GsModel.EquityPledgedInfo.REGISTER_DATE: util.from_13stamp_to_time(data.get('equPleDate', '')),
                GsModel.EquityPledgedInfo.STATUS: '有效' if data.get('type', '') == '1' else '',
                GsModel.EquityPledgedInfo.PUBLISH_DATE: util.from_13stamp_to_time(data.get('publicDate', '')),

            }
            lst_info.append(info_model)

        if len(lst_info) > 0:
            info_dict[GsModel.EQUITY_PLEDGED_INFO] = lst_info

        return info_dict

    # 股权变更信息
    def get_change_shareholding_info(self, change_shareholding_info):
        info_dict = {}
        lst_info = []
        page = self.get_crawl_page(change_shareholding_info)
        if page is None or page == u'':
            return {}
        json_data = util.json_loads(page)
        if json_data is None:
            return info_dict
        data_arr = json_data.get('data', [])
        if data_arr is None:
            return info_dict

        for data in data_arr:
            info_model = {
                GsModel.ChangeShareholding.SHAREHOLDER: data.get('inv', ''),
                GsModel.ChangeShareholding.CHANGE_BEFORE: data.get('transAmPrBf', ''),
                GsModel.ChangeShareholding.CHANGE_AFTER: util.from_13stamp_to_time(data.get('transAmPrAf', '')),
                GsModel.ChangeShareholding.CHANGE_DATE: data.get('altDate', ''),
                GsModel.ChangeShareholding.PUBLIC_DATE: util.from_13stamp_to_time(data.get('publicDate', '')),
            }
            lst_info.append(info_model)

        if len(lst_info) > 0:
            info_dict[GsModel.CHANGE_SHAREHOLDING] = lst_info

        return info_dict

    # 年报信息
    def get_annual_info(self, annual_item_list):
        return ParseTianJinAnnual(annual_item_list, self.log).get_result()


# 年报解析类
class ParseTianJinAnnual(object):
    def __init__(self, annual_item_list, log):
        self.annual_info_dict = {}
        if not isinstance(annual_item_list, list) or len(annual_item_list) <= 0:
            return

        self.log = log
        self.annual_item_list = annual_item_list

        # 分发解析
        self.dispatch()

    def dispatch(self):
        web_flag = False
        transfer_flag = False
        for_guaranty_flag = False
        for_investment_flag = False
        if self.annual_item_list is None:
            raise IndexError("未抓取到相关网页,或者抓取网页失败")
        if len(self.annual_item_list) <= 0:
            return {}

        for item in self.annual_item_list:
            url = item.get(u'url', u'')
            page_web = item.get(u'text', u'')
            if page_web is None or page_web.strip() == u'':
                continue

            # 网站或网店信息
            if u'webSiteInfo' in url:
                lst_websites, web_flag = self.get_annual_out_website(page_web)
                self.annual_info_dict[AnnualReports.WEBSITES] = lst_websites
            # 年报 企业资产状况信息
            if u'vAnnualReportBranchProduction' in url:
                asset_model = self.get_annual_asset_info(page_web)
                self.annual_info_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = asset_model
            # 股东出资信息
            if u'sponsor' in url:
                lst_share_hold = self.get_annual_share_hold_info(page_web)
                self.annual_info_dict[AnnualReports.SHAREHOLDER_INFORMATION] = lst_share_hold
            # 对外投资
            if u'forInvestment-' in url:
                lst_investment, for_investment_flag = self.get_annual_inv_info(
                    page_web)
                self.annual_info_dict[AnnualReports.INVESTED_COMPANIES] = lst_investment
            # 对外担保
            if u'forGuaranteeinfo-' in url:
                lst_out_guaranty, for_guaranty_flag = self.get_annual_out_guarantee_info(page_web)
                self.annual_info_dict[AnnualReports.OUT_GUARANTEE_INFO] = lst_out_guaranty
            # 股权变更
            if u'vAnnualReportAlterstockinfo' in url:
                lst_edit_shareholding_change, transfer_flag = self.get_annual_edit_shareholding_change(
                    page_web)
                self.annual_info_dict[AnnualReports.EDIT_SHAREHOLDING_CHANGE_INFOS] = lst_edit_shareholding_change
            # 修改信息
            if u'annualAlter' in url:
                lst_edit_change = self.get_annual_edit_change(page_web)
                self.annual_info_dict[AnnualReports.EDIT_CHANGE_INFOS] = lst_edit_change

        # 基本信息要单独循环
        for item in self.annual_item_list:
            url = item.get(u'url', u'')
            page_web = item.get(u'text', u'')
            if page_web is None or page_web.strip() == u'':
                continue
                # 基本信息
            if u'baseinfo' in url:
                annual_base_info = self.get_annual_base_info(page_web, web_flag, transfer_flag,
                                                             for_guaranty_flag, for_investment_flag)
                self.annual_info_dict.update(annual_base_info)

    # 年报基本信息
    @staticmethod
    def get_annual_base_info(page_web, web_flag, transfer_flag,
                             for_guaranty_flag, for_investment_flag):
        annual_base_info_dict = {}
        big_json = util.json_loads(page_web)
        if big_json is None:
            return annual_base_info_dict
        json_data_basic_info = big_json.get('data', '')
        if json_data_basic_info == '' or json_data_basic_info is None:
            return annual_base_info_dict
        #
        # 注册号
        basic_info_record = json_data_basic_info[0]
        unis_cid = basic_info_record.get('uniscId', '')
        if unis_cid == '' or unis_cid is None:
            annual_code = basic_info_record.get('regno', '')
        else:
            annual_code = unis_cid
        # 从业人数
        if basic_info_record.get('empNumDis', '') == u'1':
            emp_num = basic_info_record.get('empNum', '')
        else:
            emp_num = u'企业选择不公示'

        # 网店,其他股权,股权转让,有对外担保,是特殊的
        if web_flag:
            is_web = u'是'
        else:
            is_web = u'否'
        if for_investment_flag:
            is_invest = u'是'
        else:
            is_invest = u'否'
        if transfer_flag:
            is_transfer = u'是'
        else:
            is_transfer = u'否'
        if for_guaranty_flag:
            is_out_guaranty = u'是'
        else:
            is_out_guaranty = u'否'

        basic_model = {
            AnnualReports.CODE: annual_code,  # 注册号 1
            AnnualReports.ZIP_CODE: basic_info_record.get('postalCode'),  # 邮政2
            AnnualReports.CONTACT_NUMBER: basic_info_record.get('tel'),  # 联系电话 3
            AnnualReports.COMPANY_NAME: basic_info_record.get('entName'),  # 公司 4
            AnnualReports.ADDRESS: basic_info_record.get('addr'),  # 地址 5
            AnnualReports.EMAIL: basic_info_record.get('email'),  # 邮件 6
            # 从业人数
            AnnualReports.EMPLOYED_POPULATION: emp_num,
            # 经营状况
            AnnualReports.BUSINESS_STATUS: basic_info_record.get('busSt_CN', ''),
            # 网站
            AnnualReports.IS_WEB: is_web,
            # 是否买其他股权
            AnnualReports.IS_INVEST: is_invest,
            # 是否股权转让
            AnnualReports.IS_TRANSFER: is_transfer,
            # 是否有对外担保
            AnnualReports.IS_OUT_GUARANTEE: is_out_guaranty,

        }
        return basic_model

    # 年报网站信息

    @staticmethod
    def get_annual_out_website(page_web):
        lst_web = []
        big_json = util.json_loads(page_web)
        if big_json is None:
            return {}, False
        json_data_web = big_json.get('data', '')
        if json_data_web == '' or json_data_web is None:
            return {}, False
        for js_item in json_data_web:
            if js_item.get(u'webtype', u'') == u'1':
                web_type = u'网站'
            else:
                web_type = u'网店'
            web_model = {
                AnnualReports.WebSites.TYPE: web_type,
                AnnualReports.WebSites.SITE: js_item.get('domain', u''),
                AnnualReports.WebSites.NAME: js_item.get('webSitName', u'')
            }
            lst_web.append(web_model)
        return lst_web, True

    # 出资人出资信息
    @staticmethod
    def get_annual_share_hold_info(page_web):
        lst = []
        big_json = util.json_loads(page_web)
        if big_json is None:
            return {}, False

        json_data_shareholder = big_json.get('data', '')
        if json_data_shareholder == '' or json_data_shareholder is None:
            return {}, False

        for js_item in json_data_shareholder:
            sub_con_date_time = ''
            ac_con_date_time = ''
            sub_con_date = js_item.get('subConDate', u'')
            ac_con_date = js_item.get('acConDate', u'')

            if sub_con_date != '' and sub_con_date is not None:
                sub_con_date_time = str(sub_con_date)
            if ac_con_date != '' and ac_con_date is not None:
                ac_con_date_time = str(ac_con_date)
            share_model = {
                AnnualReports.ShareholderInformation.SHAREHOLDER_NAME: js_item.get('invName', u''),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                    js_item.get('liSubConAm', u'')),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TIME: sub_con_date_time,  # 认缴时间
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TYPE: js_item.get('subConFormName', u''),  # 认缴类型 #有坑

                AnnualReports.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(
                    js_item.get('liAcConAm', u'')),  # 1实缴金额
                AnnualReports.ShareholderInformation.PAIED_TIME: ac_con_date_time,  # 实缴时间
                AnnualReports.ShareholderInformation.PAIED_TYPE: js_item.get('acConForm_CN', u''),  # 实缴类型 #有坑

            }
            lst.append(share_model)
        return lst

    # 年报 企业资产状况信息
    @staticmethod
    def get_annual_asset_info(page_web):
        # 资产总额
        big_json = util.json_loads(page_web)
        if big_json is None:
            return {}
        json_array = big_json.get('data', [])
        if len(json_array) == 0:
            return {}
        json_array = json_array[0]
        if json_array is None:
            return {}
        if json_array.get('assGroDis', '') == u'1':
            total_assets = json_array.get('assGro', '')
            total_assets = util.get_amount_with_unit(total_assets)
        else:
            total_assets = u'企业选择不公示'
        # 所有者权益合计
        if json_array.get('totEquDis', '') == u'1':
            total_owners_equity = json_array.get('totEqu', '')
            total_owners_equity = util.get_amount_with_unit(total_owners_equity)
        else:
            total_owners_equity = u'企业选择不公示'
        # 销售总额
        if json_array.get('vendIncDis', '') == u'1':
            total_sales = json_array.get('vendInc', '')
            total_sales = util.get_amount_with_unit(total_sales)
        else:
            total_sales = u'企业选择不公示'
        # 利润总额
        if json_array.get('proGroDis', '') == u'1':
            profit_total = json_array.get('proGro', '')
            profit_total = util.get_amount_with_unit(profit_total)
        else:
            profit_total = u'企业选择不公示'
        # 营业总收入中主营业务收入
        if json_array.get('maiBusIncDis', '') == u'1':
            main_business_income = json_array.get('maiBusInc', '')
            main_business_income = util.get_amount_with_unit(main_business_income)
        else:
            main_business_income = u'企业选择不公示'
        # 净利润
        if json_array.get('netIncDis', '') == u'1':
            net_profit = json_array.get('netInc', '')
            net_profit = util.get_amount_with_unit(net_profit)
        else:
            net_profit = u'企业选择不公示'

        # 纳税总额
        if json_array.get('ratGroDis', '') == u'1':
            total_tax = json_array.get('ratGro', '')
            total_tax = util.get_amount_with_unit(total_tax)
        else:
            total_tax = u'企业选择不公示'
        # 负债总额
        if json_array.get('liaGroDis', '') == u'1':
            total_liabilities = json_array.get('liaGro', '')
            total_liabilities = util.get_amount_with_unit(total_liabilities)
        else:
            total_liabilities = u'企业选择不公示'
        asset_model = {
            AnnualReports.EnterpriseAssetStatusInformation.GENERAL_ASSETS: total_assets,  # 资产总额
            AnnualReports.EnterpriseAssetStatusInformation.TOTAL_EQUITY: total_owners_equity,  # 所有者权益合计
            AnnualReports.EnterpriseAssetStatusInformation.GROSS_SALES: total_sales,  # 销售总额
            AnnualReports.EnterpriseAssetStatusInformation.TOTAL_PROFIT: profit_total,  # 利润总额
            AnnualReports.EnterpriseAssetStatusInformation.INCOME_OF_TOTAL: main_business_income,  # 营业总收入中主营业务收入
            AnnualReports.EnterpriseAssetStatusInformation.RETAINED_PROFITS: net_profit,  # 净利润
            AnnualReports.EnterpriseAssetStatusInformation.TOTAL_TAX: total_tax,  # 纳税总额
            AnnualReports.EnterpriseAssetStatusInformation.TOTAL_INDEBTEDNESS: total_liabilities  # 负债总额
        }
        return asset_model

    # 年报 对外投资信息
    @staticmethod
    def get_annual_inv_info(page_web):
        lst_inv = []
        big_json = util.json_loads(page_web)
        if big_json is None:
            return lst_inv, False
        json_data = big_json.get('data', '')
        if len(json_data) == 0:
            return lst_inv, False
        for js_item in json_data:
            model = {AnnualReports.InvestedCompanies.COMPANY_NAME: js_item.get('entName', u''),
                     AnnualReports.InvestedCompanies.CODE: js_item.get('uniscId', u'')}
            lst_inv.append(model)
        return lst_inv, True

    # 年报 对外担保方法
    @staticmethod
    def get_annual_out_guarantee_info(page_web):
        lst = []
        big_json = util.json_loads(page_web)
        if big_json is None:
            return lst, False
        json_data = big_json.get('data', '')

        if len(json_data) == 0:
            return lst, False
        for json_item in json_data:
            pri_class_kind = json_item.get('priClaSecKind', u'')
            if pri_class_kind == u'1':
                pri_class = u'合同'
            else:
                pri_class = u'其他'
            period_to = json_item.get('pefPerTo', u'')
            period_from = json_item.get('pefPerForm', u'')
            period_to_time = ''
            period_from_time = ''
            if period_to != '' and period_to is not None:
                period_to_time = str(period_to)[:-3]
            if period_from_time != '' and period_from is not None:
                period_from_time = str(period_from)[:-3]
            performance_period = "{0}-{1}".format(period_from_time, period_to_time)
            out_guarantee_model = {
                AnnualReports.OutGuaranteeInfo.CREDITOR: json_item.get('more', u''),
                AnnualReports.OutGuaranteeInfo.OBLIGOR: json_item.get('mortgagor', u''),
                AnnualReports.OutGuaranteeInfo.DEBT_TYPE: pri_class,
                AnnualReports.OutGuaranteeInfo.DEBT_AMOUNT: json_item.get('priClaSecAm', u''),
                AnnualReports.OutGuaranteeInfo.PERFORMANCE_PERIOD: performance_period,
                AnnualReports.OutGuaranteeInfo.GUARANTEE_PERIOD: json_item.get('guaranperiod', u''),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_TYPE: json_item.get('gaType', u'')
            }
            lst.append(out_guarantee_model)

        return lst, True

        # 年报 股权变更方法

    @staticmethod
    def get_annual_edit_shareholding_change(page_web):
        lst = []
        big_json = util.json_loads(page_web)
        if big_json is None:
            return lst, False
        json_data = big_json.get('data', '')
        if len(json_data) == 0:
            return lst, False

        for json_item in json_data:
            edit_model = {
                AnnualReports.EditShareholdingChangeInfos.SHAREHOLDER_NAME: json_item.get('inv', u''),
                AnnualReports.EditShareholdingChangeInfos.BEFORE_CONTENT: json_item.get('transAmAft', u''),
                AnnualReports.EditShareholdingChangeInfos.AFTER_CONTENT: json_item.get('transAmAft', u''),
                AnnualReports.EditShareholdingChangeInfos.CHANGE_DATE: json_item.get('altDate', u'')
            }
            lst.append(edit_model)
        return lst, True

        # 年报 修改记录

    @staticmethod
    def get_annual_edit_change(page_web):
        lst = []
        big_json = util.json_loads(page_web)
        if big_json is None:
            return lst
        json_data = big_json.get('data', '')
        for json_item in json_data:
            edit_model = {
                AnnualReports.EditChangeInfos.CHANGE_ITEM: json_item.get('alitem', u''),
                AnnualReports.EditChangeInfos.BEFORE_CONTENT: json_item.get('altBe', u''),
                AnnualReports.EditChangeInfos.AFTER_CONTENT: json_item.get('altAf', u''),
                AnnualReports.EditChangeInfos.CHANGE_DATE: json_item.get('altDate', u'')
            }
            lst.append(edit_model)
        return lst

    def get_result(self):
        return self.annual_info_dict
