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

from pyquery import PyQuery

from base.parse_base_worker import ParseBaseWorker
from common import util
from common.annual_field import *
from common.global_field import Model
from common.gsxt_field import *

'''
异常案例:
1. 内蒙古禾为贵农业发展（集团）有限公司   股东信息 抛异常
'''

'''
年报需要翻页
'''


class GsxtParseNeiMengGuWorker(ParseBaseWorker):
    def __init__(self, **kwargs):
        ParseBaseWorker.__init__(self, **kwargs)

    # 基本信息 内蒙古伊利实业集团股份有限公司
    def get_base_info(self, base_info):
        page = self.get_crawl_page(base_info)
        res = PyQuery(page, parser='html').find('.infoStyle').find('table').eq(0).find('td[class!="title"]').remove(
            '.punctuation').items()
        base_info_dict = self._get_base_item_info(res)
        return base_info_dict

    # 股东信息
    def get_shareholder_info(self, shareholder_info):
        shareholder_info_dict = {}
        pages = self.get_crawl_page(shareholder_info, True)
        if pages is None or len(pages) <= 0:
            return {}
        lst_shareholder = []
        for page in pages:
            res_text = page.get(u'text', u'{}')
            json_data = util.json_loads(res_text)
            if json_data is None:
                self.log.error('json转换失败: res_text = {text}'.format(text=res_text))
                continue
            json_list_dict = json_data.get(u'list', {})
            obj_list = json_list_dict.get(u'list', [])
            if obj_list is None:
                self.log.info('没有 股东信息..')
                continue

            for obj in obj_list:
                share_model = {
                    GsModel.ShareholderInformation.SHAREHOLDER_NAME: str(obj.get(u'invName', u'')),
                    GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT: str(util.get_amount_with_unit(
                        obj.get(u'subConAm', u''))),
                    GsModel.ShareholderInformation.PAIED_AMOUNT: str(util.get_amount_with_unit(
                        obj.get(u'acConAm', u''))),
                    GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL: self.get_sharehold_info_sub_paid_detail(
                        obj.get(u'invFormList', [])),
                    GsModel.ShareholderInformation.PAIED_DETAIL: self.get_sharehold_info_ac_paid_detail(
                        obj.get(u'invFormList', []))
                }
                lst_shareholder.append(share_model)

            # 因为数据是通过json中读取的，而每一页都包含了所有数据，所以只需要解析一页的数据
            break
        shareholder_info_dict[GsModel.SHAREHOLDER_INFORMATION] = lst_shareholder
        return shareholder_info_dict

    # 认缴详情获取  ac 实缴  paid 认缴
    def get_sharehold_info_sub_paid_detail(self, json_obj, amount_unit=u"万元"):
        lst = []
        if json_obj is None or len(json_obj) == 0:
            return lst

        for obj in json_obj:

            model = {}

            sub_type = obj.get(u'paidForm', u'')
            if sub_type is None:
                model[GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TYPE] = ""
            else:
                model[GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TYPE] = sub_type

            amount = util.get_amount_with_unit(
                obj.get(u'paidAm', u''), amount_unit)
            if amount is None:
                model[GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT] = ""
            else:
                model[GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT] = amount

            sub_time = obj.get(u'paidDate', 0)
            if sub_time is None:
                model[GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TIME] = ""
            else:
                model[GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TIME] = sub_time

            publish_time = obj.get(u'paidDate', 0)
            if publish_time is None:
                model[GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TIME] = ""
            else:
                model[GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TIME] = publish_time

            # model = {
            #     GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TYPE: str(obj.get(u'paidForm', u'')),
            #     GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT: str(util.get_amount_with_unit(
            #         obj.get(u'paidAm', u''), amount_unit)),
            #     GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TIME: str(obj.get(u'paidDate', 0)),
            #     GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_PUBLISH_TIME: str(
            #         obj.get(u'paidPubDate', 0)),
            # }

            lst.append(model)
        return lst

    # 实缴详情获取
    def get_sharehold_info_ac_paid_detail(self, json_obj, amount_unit=u"万元"):
        lst = []
        if json_obj is None or len(json_obj) == 0:
            return lst

        for obj in json_obj:
            model = {}

            acform = obj.get(u'acForm', u'')
            if acform is None:
                model[GsModel.ShareholderInformation.PaiedDetail.PAIED_TYPE] = ""
            else:
                model[GsModel.ShareholderInformation.PaiedDetail.PAIED_TYPE] = acform

            acam = util.get_amount_with_unit(
                obj.get(u'acAm', u''), amount_unit)
            if acam is None:
                model[GsModel.ShareholderInformation.PaiedDetail.PAIED_AMOUNT] = ""
            else:
                model[GsModel.ShareholderInformation.PaiedDetail.PAIED_AMOUNT] = acam

            acdate = obj.get(u'acDate', 0)
            if acdate is None:
                model[GsModel.ShareholderInformation.PaiedDetail.PAIED_TIME] = ""
            else:
                model[GsModel.ShareholderInformation.PaiedDetail.PAIED_TIME] = acdate

            acpubdate = obj.get(u'acPubDate', 0)
            if acpubdate is None:
                model[GsModel.ShareholderInformation.PaiedDetail.PAIED_PUBLISH_TIME] = ""
            else:
                model[GsModel.ShareholderInformation.PaiedDetail.PAIED_PUBLISH_TIME] = acpubdate

            # model = {
            #     GsModel.ShareholderInformation.PaiedDetail.PAIED_TYPE: str(obj.get(u'acForm', u'')),
            #     GsModel.ShareholderInformation.PaiedDetail.PAIED_AMOUNT: str(util.get_amount_with_unit(
            #         obj.get(u'acAm', u''), amount_unit)),
            #     GsModel.ShareholderInformation.PaiedDetail.PAIED_TIME: str(obj.get(u'acDate', 0)),
            #     GsModel.ShareholderInformation.PaiedDetail.PAIED_PUBLISH_TIME: str(obj.get(u'acPubDate', 0)),
            # }
            lst.append(model)
        return lst

    # 变更信息 内蒙古伊利实业集团股份有限公司
    def get_change_info(self, change_info):
        change_info_dict = {}
        pages = self.get_crawl_page(change_info, True)
        lst_changerecords = []
        for page in pages:
            res_text = page.get(u'text', u'{}')
            json_data = util.json_loads(res_text)
            if json_data is None:
                self.log.error('json转换失败: res_text = {text}'.format(text=res_text))
                continue
            json_list = json_data.get(u'list', {})
            obj_list = json_list.get(u'list', [])
            if obj_list is None:
                self.log.info('没有 变更信息..')
                continue
            # js_obj = json.loads(page.get(u'text', u'{}')).get(u'list', {}).get(u'list', [])
            for obj in obj_list:
                change_model = self._get_change_info_2_model(obj)
                lst_changerecords.append(change_model)
            # json 包含了全部页面的数据信息 所以只需要解析一页
            break

        change_info_dict[GsModel.CHANGERECORDS] = lst_changerecords
        return change_info_dict

    def _get_change_info_2_model(self, obj):
        change_model = {
            GsModel.ChangeRecords.CHANGE_ITEM: obj.get(u'altFiledName', u''),
            # 去除多余的字
            GsModel.ChangeRecords.BEFORE_CONTENT: util.format_content(obj.get(u'altBe', u'')),
            GsModel.ChangeRecords.AFTER_CONTENT: util.format_content(obj.get(u'altAf', u'')),
            # 日期格式化
            GsModel.ChangeRecords.CHANGE_DATE: obj.get(u'altDate', 0)
        }
        return change_model

    # 主要人员 内蒙古伊利实业集团股份有限公司
    def get_key_person_info(self, key_person_info):
        key_person_info_dict = {}
        page = self.get_crawl_page(key_person_info)
        items = PyQuery(page, parser='html').find('.twoLine').items()
        lst_key_person = []
        for item in items:
            spans = item.find('span')
            key_person = {
                GsModel.KeyPerson.KEY_PERSON_NAME: spans.eq(0).text().strip(),
                GsModel.KeyPerson.KEY_PERSON_POSITION: spans.eq(1).text().strip()}
            lst_key_person.append(key_person)
        key_person_info_dict[GsModel.KEY_PERSON] = lst_key_person
        return key_person_info_dict

    # 分支机构 内蒙古伊利实业集团股份有限公司
    def get_branch_info(self, branch_info):
        branch_info_dict = {}
        page = self.get_crawl_page(branch_info)

        items = PyQuery(page, parser='html').find('.brabox').items()
        lst_branch = []
        for item in items:
            spans = item.find('span[class="infoText"]')
            code_array = spans.eq(0).text().split('：')
            code = code_array[1] if len(code_array) == 2 else None
            reg_addr_array = spans.eq(1).text().split('：')
            reg_addr = reg_addr_array[1] if len(reg_addr_array) == 2 else None
            branch_model = {
                GsModel.Branch.COMPAY_NAME: item.find('.conpany').text(),
                GsModel.Branch.CODE: code,
                GsModel.Branch.REGISTERED_ADDRESS: reg_addr
            }
            lst_branch.append(branch_model)
        branch_info_dict[GsModel.BRANCH] = lst_branch
        return branch_info_dict

    # 出资信息 内蒙古伊利实业集团股份有限公司
    def get_contributive_info(self, contributive_info):
        contributive_info_dict = {}
        pages_list = self.get_crawl_page(contributive_info, True)

        part_a_con = {}
        for page in pages_list:

            res_text = page.get(u'text', u'{}')
            json_data = util.json_loads(res_text)
            if json_data is None:
                self.log.error('json转换失败: res_text = {text}'.format(text=res_text))
                continue
            json_list = json_data.get(u'list', {})
            obj_list = json_list.get(u'list', [])
            if obj_list is None:
                self.log.info('没有 出资信息..')
                continue

            for obj in obj_list:
                sub_model = self.get_contributive_info_detail(obj)
                if sub_model is None:
                    continue
                part_a_con[obj.get(u'inv', u'')] = sub_model

        part_b_con = {}
        pages_detail = self.get_crawl_page(contributive_info, True, Model.type_detail)
        if pages_detail is not None:
            for page in pages_detail:
                tables = PyQuery(page.get(u'text', u''), parser='html').find('.tableInfo').items()
                shareholder_name, sub_model = self._get_sharehold_detail(tables, "td")
                part_b_con[shareholder_name] = sub_model
        lst_contributive = []
        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            lst_contributive.append(v_list)
        contributive_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = lst_contributive
        return contributive_info_dict

    def get_contributive_info_detail(self, obj, start_index=1):
        if obj is None:
            return None
        sub_model = {
            GsModel.ContributorInformation.SHAREHOLDER_NAME: str(obj.get(u'inv', u'')),
            GsModel.ContributorInformation.SHAREHOLDER_TYPE: str(obj.get(u'invType', u'')),
            GsModel.ContributorInformation.CERTIFICATE_TYPE: str(obj.get(u'certName', u'')),
            GsModel.ContributorInformation.CERTIFICATE_NO: str(obj.get(u'certNo', u''))
        }
        return sub_model

    def get_equity_pledged_info_list(self, obj):
        if obj is None:
            return None

        cur_date = util.from_time_stamp_to_time(obj.get(u"registDate") / 1000)

        sub_model = {
            # 登记编号
            GsModel.EquityPledgedInfo.REGISTER_NUM: str(obj.get(u'stoRegNo', u'')),
            GsModel.EquityPledgedInfo.MORTGAGOR: str(obj.get(u'inv', u'')),
            GsModel.EquityPledgedInfo.MORTGAGOR_NUM: str(obj.get(u'regNo', u'')),
            GsModel.EquityPledgedInfo.PLEDGE_STOCK_AMOUNT: str(obj.get(u'impAm')) + str(obj.get(u'pleAmUnit')),
            GsModel.EquityPledgedInfo.PLEDGEE: str(obj.get(u'impOrg')),
            GsModel.EquityPledgedInfo.PLEDGEE_NUM: str(obj.get(u'impOrgID')),
            GsModel.EquityPledgedInfo.REGISTER_DATE: cur_date,
            GsModel.EquityPledgedInfo.PUBLISH_DATE: cur_date,
        }

        # 替换源数据错误
        sub_model[GsModel.EquityPledgedInfo.PLEDGE_STOCK_AMOUNT] = sub_model[
            GsModel.EquityPledgedInfo.PLEDGE_STOCK_AMOUNT].replace("null", "")
        # sub_model[GsModel.EquityPledgedInfo.PLEDGE_STOCK_AMOUNT] = sub_model[GsModel.EquityPledgedInfo.PLEDGE_STOCK_AMOUNT].replace("万股", "万元")


        biz_sta_id = obj.get(u'type')
        if biz_sta_id is None:
            sub_model[GsModel.EquityPledgedInfo.STATUS] = u'无效'
        elif biz_sta_id.strip() == u'1':
            sub_model[GsModel.EquityPledgedInfo.STATUS] = u'有效'
        else:
            sub_model[GsModel.EquityPledgedInfo.STATUS] = u'无效'

        return sub_model

    def get_sto_ple_no(self, url):
        pattern = u"http://.*?/aiccips/GSpublicity/curStoPleXQ\.html\?stoPleNo=(.*?)&.*?&.*?&.*?"
        regex = re.compile(pattern)
        search_list = regex.findall(url)
        if len(search_list) > 0:
            return search_list[0]

        return None

    # 详情页解析
    def get_equity_pledged_info_detail(self, text):
        detail = {}
        if text is None:
            return detail

        change_list = []
        jq = PyQuery(text, parser='html')
        tr_list = jq.find(".tableInfo").find("tr")
        for index, tr in enumerate(tr_list):
            if index == 0:
                continue

            td_list = tr.find("td")
            if len(td_list) < 3:
                continue
            model = {
                GsModel.EquityPledgedInfo.EquityPledgedDetail.ChangeInfo.CHANGE_DATE: td_list.eq(1).text(),
                GsModel.EquityPledgedInfo.EquityPledgedDetail.ChangeInfo.CHANGE_CONTENT: td_list.eq(2).text(),
            }

            change_list.append(model)

        detail[GsModel.EquityPledgedInfo.EquityPledgedDetail.CHANGE_INFO] = change_list
        return {GsModel.EquityPledgedInfo.EQUITY_PLEDGED_DETAIL: detail}

    # 股权出质登记信息 股权出资登记
    def get_equity_pledged_info(self, equity_pledged_info):
        pledged_info = {}
        pages_list = self.get_crawl_page(equity_pledged_info, True)
        part_a_con = {}
        for page in pages_list:

            res_text = page.get(u'text', u'{}')
            json_data = util.json_loads(res_text)
            if json_data is None:
                self.log.error('json转换失败: res_text = {text}'.format(text=res_text))
                continue
            json_list = json_data.get(u'list', {})
            obj_list = json_list.get(u'list', [])
            if obj_list is None:
                self.log.info('没有 出资信息..')
                continue

            for obj in obj_list:
                sub_model = self.get_equity_pledged_info_list(obj)
                if sub_model is None:
                    continue
                part_a_con[obj.get(u'stoPleNo', u'')] = sub_model

        part_b_con = {}
        pages_detail = self.get_crawl_page(equity_pledged_info, True, Model.type_detail)
        if pages_detail is not None:
            for page in pages_detail:
                url = page.get(u'url')
                text = page.get(u'text')
                sto_ple_no = self.get_sto_ple_no(url)
                if sto_ple_no is None:
                    continue
                part_b_con[sto_ple_no] = self.get_equity_pledged_info_detail(text)

        lst_equity_pledged = []
        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            lst_equity_pledged.append(v_list)

        pledged_info[GsModel.EQUITY_PLEDGED_INFO] = lst_equity_pledged
        return pledged_info

    # 股权变更信息
    def get_change_shareholding_info_list(self, obj):

        if obj is None:
            return {}

        cur_date = util.from_time_stamp_to_time(obj.get(u"altDate") / 1000)

        sub_model = {
            # 登记编号
            GsModel.ChangeShareholding.SHAREHOLDER: str(obj.get(u'guDName', u'')),
            GsModel.ChangeShareholding.CHANGE_BEFORE: str(obj.get(u'transBePr', u'')),
            GsModel.ChangeShareholding.CHANGE_AFTER: str(obj.get(u'transAfPr', u'')),
            GsModel.ChangeShareholding.CHANGE_DATE: cur_date,
            GsModel.ChangeShareholding.PUBLIC_DATE: cur_date,
        }

        return sub_model

    # 股权变更信息
    def get_change_shareholding_info(self, change_shareholding_info):
        change_shareholding_dict = {}
        text = self.get_crawl_page(change_shareholding_info)

        json_data = util.json_loads(text)
        if json_data is None:
            self.log.error('json转换失败: res_text = {text}'.format(text=text))
            return change_shareholding_dict
        json_list = json_data.get(u'list', {})
        obj_list = json_list.get(u'list', [])
        if obj_list is None:
            self.log.info('没有 出资信息..')
            return change_shareholding_dict

        change_list = []
        for obj in obj_list:
            sub_model = self.get_change_shareholding_info_list(obj)
            if sub_model is None:
                continue
            change_list.append(sub_model)
        return {GsModel.CHANGE_SHAREHOLDING: change_list}

    # 清算信息
    def get_liquidation_info(self, liquidation_info):
        return {}

    # 年报信息
    def get_annual_info(self, annual_item_list):
        return ParseNeiMengGuAnnual(annual_item_list, self.log).get_result()


# 测试企业: 步步高通信科技有限公司
# 年报解析类
class ParseNeiMengGuAnnual(object):
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

            # 基本信息
            if 'annualReport' in url:
                table_list = PyQuery(text, parser='html').find('table').items()
                for table in table_list:
                    if len(table.find('#RegistNo')) > 0:
                        # 基本信息
                        self.annual_info_dict.update(self.get_base_info(table))
                    if len(table.find('.webBox')) > 0:
                        # 网站网点
                        self.annual_info_dict.update(self.get_wzwd_info(table))
                    if len(table.find('#capitalList')) > 0 or table.attr('id') == 'capitalList':
                        # 股东出资
                        self.annual_info_dict.update(self.get_gdcz_info(table))

                    if len(table.find('#AssetsTotalAmount')) > 0:
                        # 企业资产状况
                        self.annual_info_dict.update(self.get_asset_info(table))

                    if table.attr('id') == 'guarantyList':
                        # 对外担保
                        self.annual_info_dict.update(self.get_dwdb_info(table))

                    if len(table.find('.investmentBox')) > 0:
                        # 对外投资
                        self.annual_info_dict.update(self.get_dwtz_info(table))

                    if table.attr('id') == 'table_1NA':
                        # 修改信息
                        self.annual_info_dict.update(self.get_qybg_info(table))

                    if table.attr('id') == 'stockTransferList':
                        # 股权变更信息
                        self.annual_info_dict.update(self.get_changestock_info(table))

    # 基本信息
    def get_base_info(self, table):
        py_items = table.find('td').items()
        base_dict = {}
        for item in py_items:
            item_content = item.text().replace(u'.', u'')
            if item_content.find('：') == -1:
                continue
            part = item_content.split(u'：', 1)
            k = AnnualReports.format_base_model(part[0].strip())
            base_dict[k] = part[1].strip()

        return base_dict

    # 股权变更信息
    def get_changestock_info(self, table):
        changestock_dict = {}
        lst = []

        trs = table.find('.tablebodytext').items()
        for tr in trs:
            tds = tr.find('td')
            if tds is None or len(tds) < 4:
                continue

            model = {
                AnnualReports.EditShareholdingChangeInfos.SHAREHOLDER_NAME: tds.eq(1).text(),
                AnnualReports.EditShareholdingChangeInfos.BEFORE_CONTENT: tds.eq(2).text(),
                AnnualReports.EditShareholdingChangeInfos.AFTER_CONTENT: tds.eq(3).text(),
                AnnualReports.EditShareholdingChangeInfos.CHANGE_DATE: tds.eq(4).text(),
            }
            lst.append(model)

        changestock_dict[AnnualReports.EDIT_SHAREHOLDING_CHANGE_INFOS] = lst
        return changestock_dict

    # 企业资产状况
    def get_asset_info(self, table):
        asset_dict = {}
        ths = table.find('.tdTitleText')
        tds = table.find('.baseText')

        lst_value = tds.map(lambda i, e: PyQuery(e).text())
        lst_title = ths.map(lambda i, e: PyQuery(e).text())
        map_title_value = zip(lst_title, lst_value)
        model = {}
        for k_title, v_value in map_title_value:
            if k_title.strip() == '':
                continue
            k = AnnualReports.format_asset_model(k_title)
            model[k] = v_value

        asset_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = model
        return asset_dict

    # 修改信息
    def get_qybg_info(self, table):

        qybg_dict = {}
        lst = []

        trs = table.find('.tablebodytext').items()
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

        qybg_dict[AnnualReports.EDIT_CHANGE_INFOS] = lst
        return qybg_dict

    # 股东出资
    def get_gdcz_info(self, table):
        gdcz_dict = {}
        lst = []

        trs = table.find('.tablebodytext').items()
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

        gdcz_dict[AnnualReports.SHAREHOLDER_INFORMATION] = lst
        return gdcz_dict

    # 对外担保
    def get_dwdb_info(self, table):
        dwdb_dict = {}
        lst = []

        trs = table.find('.tablebodytext').items()
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

        dwdb_dict[AnnualReports.OUT_GUARANTEE_INFO] = lst
        return dwdb_dict

    # 对外投资
    def get_dwtz_info(self, table):
        dwtz_dict = {}
        lst = []
        div_list = table.find('.investmentBox').items()
        for div in div_list:
            model = {}

            web_info_name = div.find('.webInfo_name').text().strip()
            if web_info_name != u'':
                model[AnnualReports.InvestedCompanies.COMPANY_NAME] = web_info_name

            web_info_msg = div.find('.webInfo_msg').text().replace(u'·', u'', 1)
            text_array = web_info_msg.split(u'：')
            if len(text_array) >= 2:
                model[AnnualReports.InvestedCompanies.CODE] = text_array[1]
            else:
                model[AnnualReports.InvestedCompanies.CODE] = None
            lst.append(model)

        dwtz_dict[AnnualReports.INVESTED_COMPANIES] = lst
        return dwtz_dict

    # 网站网点
    def get_wzwd_info(self, table):
        dwtz_dict = {}
        web_item = {}
        item_list = table.find('.webInfo').find('div').items()
        for item in item_list:
            if item.attr('class') == 'webInfo_name':
                web_item[AnnualReports.WebSites.NAME] = item.text().strip()
                continue
            item_content = item.text().replace(u'.', u'', 1)
            part = item_content.split(u'：', 1)
            k = AnnualReports.format_website_model(part[0].strip().replace(u' ', u''))
            web_item[k] = part[1].strip()
        if len(web_item) > 0:
            dwtz_dict[AnnualReports.WEBSITES] = [web_item]
        return dwtz_dict

    def get_result(self):
        return self.annual_info_dict
