#!/usr/bin/env python
# encoding: utf-8
import traceback

from pyquery import PyQuery

from base.parse_base_worker import ParseBaseWorker
from common import util
from common.annual_field import *
from common.global_field import Model
from common.gsxt_field import *


class GsxtParseShenZhenWorker(ParseBaseWorker):
    def __init__(self, **kwargs):
        ParseBaseWorker.__init__(self, **kwargs)

    # 基本信息
    def get_base_info(self, page_dict):
        if not isinstance(page_dict, dict):
            self.log.error('传入参数不正确, 不是dict')
            raise StandardError('传入参数不正确, 不是dict')

        page_list = page_dict.get(Model.type_list, None)
        if page_list is None:
            raise StandardError('获取list失败')

        page_item = page_list[0]
        status = page_item.get('status', 'fail')
        if status != 'success':
            self.log.error('基本信息抓取失败...')
            raise StandardError('基本信息抓取失败')

        page = page_item.get('text', '')
        if page == '':
            raise StandardError('没有获得正确的文本信息')

        res = PyQuery(page, parser='html').find('#yyzz').find('.infor_ul').find('li').items()
        base_info_dict = self._get_base_item_info(res)
        try:
            change_info_dict = {}
            change_list = self.get_inline_change_info(page)
            change_info_dict[GsModel.CHANGERECORDS] = change_list
        except:
            self.log.error(
                'company:{0},error-part:changerecords_info,error-info:{1}'.format(base_info_dict.get('company', ''),
                                                                                  traceback.format_exc()))
            change_info_dict = {}
        base_info_dict.update(change_info_dict)

        try:
            key_person_info_dict = self.get_inline_key_person_info(page)
        except:
            self.log.error(
                'company:{0},error-part:key_person_info,error-info:{1}'.format(base_info_dict.get('company', ''),
                                                                               traceback.format_exc()))
            key_person_info_dict = {}
        base_info_dict.update(key_person_info_dict)

        try:
            branch_info_dict = self.get_inline_branch_info(page)
        except:
            self.log.error(
                'company:{0},error-part:branch_info,error-info:{1}'.format(base_info_dict.get('company', ''),
                                                                           traceback.format_exc()))
            branch_info_dict = {}
        base_info_dict.update(branch_info_dict)

        try:
            shareholder_info_dict = self.get_inline_shareholder_info(page)
        except:
            self.log.error(
                'company:{0},error-part:shareholder_info_dict,error-info:{1}'.format(base_info_dict.get('company', ''),
                                                                                     traceback.format_exc()))
            shareholder_info_dict = {}
        base_info_dict.update(shareholder_info_dict)

        return base_info_dict

    # 股东信息 深圳TCL光电科技有限公司
    def get_inline_shareholder_info(self, page):
        shareholder_info_dict = {}
        lst_shareholder = []
        # for page in pages:
        table = PyQuery(page, parser='html').find('#UpdatePanel1').find('table')
        trs = table.find('tr').items()
        amount_unit = util.get_amount_unit(table)
        for tr in trs:
            start_index = 0
            tds = tr.find('td')
            if tds is None or len(tds) <= start_index + 10:
                continue
            share_model = {
                GsModel.ShareholderInformation.SHAREHOLDER_NAME: tds.eq(start_index).text().replace(u'\\t', u''),
                GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                    tds.eq(start_index + 1).text(), amount_unit),
                GsModel.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(
                    tds.eq(start_index + 2).text(), amount_unit),
                GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL: [
                    {
                        GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TYPE: tds.eq(
                            start_index + 3).text(),
                        GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                            tds.eq(start_index + 4).text(), amount_unit),
                        GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TIME: tds.eq(
                            start_index + 5).text(),
                        GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_PUBLISH_TIME: tds.eq(
                            start_index + 6).text(),
                    }
                ],
                GsModel.ShareholderInformation.PAIED_DETAIL: [
                    {
                        GsModel.ShareholderInformation.PaiedDetail.PAIED_TYPE: tds.eq(start_index + 7).text(),
                        GsModel.ShareholderInformation.PaiedDetail.PAIED_AMOUNT: util.get_amount_with_unit(
                            tds.eq(start_index + 8).text(), amount_unit),
                        GsModel.ShareholderInformation.PaiedDetail.PAIED_TIME: tds.eq(start_index + 9).text(),
                        GsModel.ShareholderInformation.PaiedDetail.PAIED_PUBLISH_TIME: tds.eq(start_index + 10).text(),
                    }
                ]
            }
            if len(share_model) > 0:
                lst_shareholder.append(share_model)
        shareholder_info_dict[GsModel.SHAREHOLDER_INFORMATION] = lst_shareholder
        return shareholder_info_dict

    # 变更信息
    def get_change_info(self, change_info):
        '''
        :param change_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        变更信息一般只包含list列表, 但是特殊情况下也会有detail详情页列表 比如 北京这个省份有发现过包含详情页的变更信息
        :return: 返回工商schema字典
        '''
        change_info_dict = {}
        if change_info is None:
            return change_info_dict

        # list 网页列表
        change_info_list = change_info.get(Model.type_list)
        if change_info_list is None:
            return change_info_dict

        change_list = []
        change_set = set()
        for page_item in change_info_list:
            status = page_item.get('status', 'fail')
            if status != 'success':
                self.log.error('变更抓取失败...')
                continue

            page = page_item.get('text', '')
            if page == '':
                self.log.error('没有获得正确的文本信息')
                continue

            change_temp_list = self.get_inline_change_info(page)

            for item in change_temp_list:
                hash_code = ''
                for key, value in item.items():
                    hash_code += str(value)
                if hash_code not in change_set:
                    change_set.add(hash_code)
                    change_list.append(item)

                    # change_list.extend(change_temp_list)

        change_info_dict[GsModel.CHANGERECORDS] = change_list
        return change_info_dict

    # 变更信息  深圳TCL光电科技有限公司
    def get_inline_change_info(self, page):
        lst_changerecords = []
        trs = PyQuery(page, parser='html').find('#bgxx').find('table').find('tr').items()
        for tr in trs:
            tds = tr.find('td')
            if tds is None or len(tds) < 4:
                continue
            change_model = self._get_change_info_td_text(tds, 0)
            if len(change_model) > 0:
                lst_changerecords.append(change_model)
        return lst_changerecords

    # 主要人员 深圳TCL光电科技有限公司
    def get_inline_key_person_info(self, page):
        key_person_info_dict = {}
        items = PyQuery(page, parser='html').find('#PeopleMain').find('#MainPeople').find('li').items()
        lst_key_person = []
        for item in items:
            ps = item.find('p')
            key_person = {
                GsModel.KeyPerson.KEY_PERSON_NAME: ps.eq(0).text().strip(),
                GsModel.KeyPerson.KEY_PERSON_POSITION: ps.eq(1).text().strip()}
            lst_key_person.append(key_person)
        key_person_info_dict[GsModel.KEY_PERSON] = lst_key_person
        return key_person_info_dict

    # 分支机构  招商银行股份有限公司
    def get_inline_branch_info(self, page):
        branch_info_dict = {}
        items = PyQuery(page, parser='html').find('#InformationOfAffiliatedAgency').find('li').items()
        lst_branch = []
        for item in items:
            ps = item.find('p')
            code_array = ps.eq(0).text().split('：')
            code = code_array[1] if len(code_array) == 2 else None
            reg_addr_array = ps.eq(1).text().split('：')
            reg_addr = reg_addr_array[1] if len(reg_addr_array) == 2 else None
            branch_model = {
                GsModel.Branch.COMPAY_NAME: item.find('h4').text(),
                GsModel.Branch.CODE: code,
                GsModel.Branch.REGISTERED_ADDRESS: reg_addr
            }
            lst_branch.append(branch_model)
        branch_info_dict[GsModel.BRANCH] = lst_branch
        return branch_info_dict

    # 出资信息 深圳TCL光电科技有限公司
    def get_contributive_info(self, contributive_info):

        con_info_dict = {}
        pages_list = self.get_crawl_page(contributive_info, True)

        part_a_con = {}
        for page in pages_list:
            page_text = page.get(u'text', u'')
            if page_text.strip() == u'':
                continue

            trs = PyQuery(page_text, parser='html').find('#UpdatePanel2').find('.table_block').find('tr').items()
            if trs is None:
                trs = PyQuery(page_text, parser='html').find('.table_block').find('tr').items()

            start_index = 0
            for tr in trs:
                tds = tr.find('td')
                sub_model = self._get_sharehold_info_list_td_text(tds, start_index=start_index)
                if sub_model is None or len(sub_model) <= 0:
                    continue
                part_a_con[tds.eq(start_index).text().strip()] = sub_model

        pages_detail = self.get_crawl_page(contributive_info, True, Model.type_detail)
        part_b_con = {}
        if pages_detail is not None:
            for page in pages_detail:
                page_text = page.get(u'text', u'')
                if page_text.strip() == u'':
                    continue

                tables = PyQuery(page_text, parser='html').find('.table_block').items()
                shareholder_name, sub_model = self._get_sharehold_detail(tables)
                part_b_con[shareholder_name] = sub_model

        con_list = []
        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            con_list.append(v_list)

        con_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = con_list

        return con_info_dict

    # 股权出质登记信息 股权出资登记
    def get_equity_pledged_info(self, equity_pledged_info):
        return {}

    # 股权变更信息
    def get_change_shareholding_info(self, change_shareholding_info):
        parse_dict = {}
        return parse_dict

    # 年报信息
    def get_annual_info(self, annual_item_list):
        return ParseShenZhenAnnual(annual_item_list, self.log).get_result()


# 测试企业: 招商银行股份有限公司
# 年报解析类
class ParseShenZhenAnnual(object):
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
            if 'QYNBInfo' in url:
                # 基本信息
                self.annual_info_dict.update(self.get_base_info(text))

                # 网站网点
                self.annual_info_dict.update(self.get_wzwd_info(text))

                # 股东出资
                self.annual_info_dict.update(self.get_gdcz_info(text))

                # 企业资产状况
                self.annual_info_dict.update(self.get_asset_info(text))

                # 对外担保
                self.annual_info_dict.update(self.get_dwdb_info(text))

                # 对外投资
                self.annual_info_dict.update(self.get_dwtz_info(text))

                # 修改信息
                self.annual_info_dict.update(self.get_qybg_info(text))

                # 股权变更信息
                self.annual_info_dict.update(self.get_changestock_info(text))

    # 基本信息
    def get_base_info(self, page_text):
        jq = PyQuery(page_text, parser='html')
        py_items = jq.find('.infor_ul').find('ul').find('li').items()
        base_dict = {}
        for item in py_items:
            span_list = item.find('span')
            if len(span_list) <= 0:
                continue

            key = span_list.eq(0).text().strip(u'>').strip().replace(u' ', u'')
            if len(span_list) >= 2:
                value = span_list.eq(1).text().strip()
            else:
                value = ''

            k = AnnualReports.format_base_model(key)
            base_dict[k] = value

        return base_dict

    # 股权变更信息
    def get_changestock_info(self, page_text):
        changestock_dict = {}
        lst = []

        jq = PyQuery(page_text, parser='html')
        item_list = jq.find('.item_box').items()
        for item in item_list:
            if item.find('.item_title').text().find('股权变更') != -1:
                trs = item.find('tr').items()
                for tr in trs:
                    tds = tr.find('td')
                    if tds is None or len(tds) < 6:
                        continue

                    model = {
                        AnnualReports.EditShareholdingChangeInfos.SHAREHOLDER_NAME: tds.eq(1).text(),
                        AnnualReports.EditShareholdingChangeInfos.BEFORE_CONTENT: tds.eq(2).text(),
                        AnnualReports.EditShareholdingChangeInfos.AFTER_CONTENT: tds.eq(3).text(),
                        AnnualReports.EditShareholdingChangeInfos.CHANGE_DATE: tds.eq(4).text(),
                        AnnualReports.EditShareholdingChangeInfos.PERFORMANCE_PERIOD: tds.eq(5).text(),
                    }
                    lst.append(model)
                break
        changestock_dict[AnnualReports.EDIT_SHAREHOLDING_CHANGE_INFOS] = lst
        return changestock_dict

    # 企业资产状况
    def get_asset_info(self, page_text):
        asset_dict = {}
        model = {}
        jq = PyQuery(page_text, parser='html')
        item_list = jq.find('.item_box').items()
        for item in item_list:
            if item.find('.item_title').text().find('企业资产') != -1:
                ths = item.find('th')
                tds = item.find('td')

                lst_value = tds.map(lambda i, e: PyQuery(e).text())
                lst_title = ths.map(lambda i, e: PyQuery(e).text())
                map_title_value = zip(lst_title, lst_value)
                for k_title, v_value in map_title_value:
                    if k_title.strip() == '':
                        continue
                    k = AnnualReports.format_asset_model(k_title)
                    model[k] = v_value
                break
        if len(model) > 0:
            asset_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = model
        return asset_dict

    # 修改信息
    def get_qybg_info(self, page_text):

        qybg_dict = {}
        lst = []

        jq = PyQuery(page_text, parser='html')
        item_list = jq.find('.item_box').items()
        for item in item_list:
            if item.find('.item_title').text().find('修改信息') != -1:
                trs = item.find('tr').items()
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
                break

        qybg_dict[AnnualReports.EDIT_CHANGE_INFOS] = lst
        return qybg_dict

    # 股东出资
    def get_gdcz_info(self, page_text):
        gdcz_dict = {}
        lst = []

        jq = PyQuery(page_text, parser='html')
        item_list = jq.find('.item_box').items()
        for item in item_list:
            if item.find('.item_title').text().find('股东及出资') != -1:
                trs = item.find('tr').items()
                for tr in trs:
                    tds = tr.find('td')
                    if tds is None or len(tds) < 8:
                        continue

                    share_model = {
                        AnnualReports.ShareholderInformation.SHAREHOLDER_NAME: tds.eq(1).text(),
                        AnnualReports.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                            tds.eq(2).text()),
                        AnnualReports.ShareholderInformation.SUBSCRIPTION_TIME: tds.eq(3).text(),
                        AnnualReports.ShareholderInformation.SUBSCRIPTION_TYPE: tds.eq(4).text(),
                        AnnualReports.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(tds.eq(5).text()),
                        AnnualReports.ShareholderInformation.PAIED_TIME: tds.eq(6).text(),
                        AnnualReports.ShareholderInformation.PAIED_TYPE: tds.eq(7).text(),
                    }
                    lst.append(share_model)
                break

        gdcz_dict[AnnualReports.SHAREHOLDER_INFORMATION] = lst
        return gdcz_dict

    # 对外担保
    def get_dwdb_info(self, page_text):
        dwdb_dict = {}
        lst = []

        jq = PyQuery(page_text, parser='html')
        item_box_list = jq.find('.item_box').items()
        for item_box in item_box_list:
            title = item_box.find('.item_title').text()
            if title.find('担保') != -1 and title.find('对外') != -1:
                trs = item_box.find('tr').items()
                for tr in trs:
                    tds = tr.find('td')
                    if tds is None or len(tds) < 8:
                        continue

                    model = {
                        AnnualReports.OutGuaranteeInfo.CREDITOR: tds.eq(1).text(),
                        AnnualReports.OutGuaranteeInfo.OBLIGOR: tds.eq(2).text(),
                        AnnualReports.OutGuaranteeInfo.DEBT_TYPE: tds.eq(3).text(),
                        AnnualReports.OutGuaranteeInfo.DEBT_AMOUNT: util.get_amount_with_unit(tds.eq(4).text()),
                        AnnualReports.OutGuaranteeInfo.PERFORMANCE_PERIOD: tds.eq(5).text(),
                        AnnualReports.OutGuaranteeInfo.GUARANTEE_PERIOD: tds.eq(6).text(),
                        AnnualReports.OutGuaranteeInfo.GUARANTEE_TYPE: tds.eq(7).text(),
                    }
                    lst.append(model)
                break
        dwdb_dict[AnnualReports.OUT_GUARANTEE_INFO] = lst
        return dwdb_dict

    # 对外投资
    def get_dwtz_info(self, page_text):
        dwtz_dict = {}
        lst = []
        jq = PyQuery(page_text, parser='html')
        item_box_list = jq.find('.item_box').items()
        for item_box in item_box_list:
            title = item_box.find('.item_title').text()
            if title.find('对外投资') != -1:
                li_list = item_box.find('li').items()
                for li in li_list:
                    model = {}

                    webInfo_name = li.find('h4').text().strip()
                    if webInfo_name != '':
                        model[AnnualReports.InvestedCompanies.COMPANY_NAME] = webInfo_name

                    webInfo_msg = li.find('span').text().replace(u'・', u'', 1)
                    text_array = webInfo_msg.split(u'：')
                    if len(text_array) >= 2:
                        model[AnnualReports.InvestedCompanies.CODE] = text_array[1]
                    else:
                        model[AnnualReports.InvestedCompanies.CODE] = None
                    lst.append(model)
                break

        dwtz_dict[AnnualReports.INVESTED_COMPANIES] = lst
        return dwtz_dict

    # 网站网点
    def get_wzwd_info(self, page_text):
        dwtz_dict = {}

        lst = []
        jq = PyQuery(page_text, parser='html')
        item_box_list = jq.find('.item_box').items()
        for item_box in item_box_list:
            title = item_box.find('.item_title').text()
            if title.find('网站') != -1 or title.find('网店') != -1:
                ul_list = item_box.find('ul').items()
                web_item = {}
                for item in ul_list:
                    if item.find('h3').text().strip() != '':
                        web_item[AnnualReports.WebSites.NAME] = item.find('h3').text().strip()
                    p_list = item.find('p').items()
                    for p in p_list:
                        item_content = p.text().replace(u'・', u'')
                        part = item_content.split(u'：', 2)
                        k = AnnualReports.format_website_model(part[0].strip().replace(u' ', u''))
                        web_item[k] = part[1].strip()
                if len(web_item) > 0:
                    lst.append(web_item)
                break
        dwtz_dict[AnnualReports.WEBSITES] = lst
        return dwtz_dict

    def get_result(self):
        return self.annual_info_dict
