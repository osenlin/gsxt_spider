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
from common.global_field import Model, PageCrawlError
from common.gsxt_field import *

# todo 需要找到有多个明细的股东信息
# todo 单位怎么处理
# todo 基本信息统一社会信用号和注册号如何处理
# todo 股东信息里面的单位金额如何处理
# done 如果需要多页合并需要找到对应的case 进行测试
'''
异常案例记录:
1.安徽三利丝绸集团有限公司彭畈茧站
2.安徽川海工贸有限公司蚌埠五金机电分公司
3.北京汇源果汁饮料集团宜昌有限责任公司 amount有问题
'''


class GsxtParseAnHuiWorker(ParseBaseWorker):
    def __init__(self, **kwargs):
        ParseBaseWorker.__init__(self, **kwargs)

    # 基本信息
    def get_base_info(self, base_info):
        '''
        :param base_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        基本信息一般存储在list列表中, 因为基本信息不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        '''
        page = self.get_crawl_page(base_info)
        res = PyQuery(page, parser='html').find('#zhizhao').find('td').items()
        base_info_dict = self._get_base_item_info(res)
        return base_info_dict

    # 股东信息 靠下面的是股东信息  安徽省高速石化有限公司
    # done 日期要格式化
    def get_shareholder_info(self, shareholder_info):
        '''
        :param shareholder_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        股东信息一般存储在list列表中, 因为股东信息不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        '''
        shareholder_info_dict = {}
        page_list = self.get_crawl_page(shareholder_info, True)
        shareholder_list = []
        for page in page_list:
            table = PyQuery(page.get('text', u''), parser='html').find('.detailsList')
            trs = table.find('tr').items()
            amount_unit = util.get_amount_unit(table)
            for tr in trs:
                tds = tr.find('td')
                if tds is None or len(tds) <= 11:
                    continue
                share_model = {
                    GsModel.ShareholderInformation.SHAREHOLDER_NAME: tds.eq(1).text().replace(u'\\t', u''),
                    GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT:
                        util.get_amount_with_unit(tds.eq(2).text(), amount_unit),
                    GsModel.ShareholderInformation.PAIED_AMOUNT:
                        util.get_amount_with_unit(tds.eq(3).text(), amount_unit),
                    GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL: [
                        {
                            GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TYPE: tds.eq(4).text(),
                            GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT:
                                util.get_amount_with_unit(tds.eq(5).text(), amount_unit),
                            GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TIME: tds.eq(6).text(),
                            GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_PUBLISH_TIME: tds.eq(
                                7).text(),
                        }
                    ],
                    GsModel.ShareholderInformation.PAIED_DETAIL: [{
                        GsModel.ShareholderInformation.PaiedDetail.PAIED_TYPE: tds.eq(8).text(),
                        GsModel.ShareholderInformation.PaiedDetail.PAIED_AMOUNT:
                            util.get_amount_with_unit(tds.eq(9).text(), amount_unit),
                        GsModel.ShareholderInformation.PaiedDetail.PAIED_TIME: tds.eq(10).text(),
                        GsModel.ShareholderInformation.PaiedDetail.PAIED_PUBLISH_TIME: tds.eq(11).text(),
                    }]
                }
                shareholder_list.append(share_model)

        if len(shareholder_list) > 0:
            shareholder_info_dict[GsModel.SHAREHOLDER_INFORMATION] = shareholder_list

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
        page_list = self.get_crawl_page(change_info, True)
        change_records_list = []
        for page in page_list:
            trs = PyQuery(page.get(u'text', u''), parser='html').find('#alterInfo').find('tr').items()
            for tr in trs:
                tds = tr.find('td')
                change_model = self._get_change_info_td_text(tds)
                if len(change_model) > 0:
                    change_records_list.append(change_model)

        if len(change_records_list) > 0:
            change_info_dict[GsModel.CHANGERECORDS] = change_records_list
        return change_info_dict

    # 主要人员
    def get_key_person_info(self, key_person_info):
        '''
        :param key_person_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        主要人员一般存储在list列表中, 因为主要人员不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        '''
        key_person_info_dict = {}
        page = self.get_crawl_page(key_person_info)
        items = PyQuery(page, parser='html').find('center').items()
        key_person_list = []
        for item in items:
            spans = item.find('span')
            key_person = {
                GsModel.KeyPerson.KEY_PERSON_NAME: spans.eq(0).text().strip(),
                GsModel.KeyPerson.KEY_PERSON_POSITION: spans.eq(1).text().strip()}
            key_person_list.append(key_person)

        if len(key_person_list) > 0:
            key_person_info_dict[GsModel.KEY_PERSON] = key_person_list
        return key_person_info_dict

    # 分支机构
    def get_branch_info(self, branch_info):
        '''
        :param branch_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        分支机构一般存储在list列表中, 因为分支机构不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        '''
        branch_info_dict = {}
        page = self.get_crawl_page(branch_info)

        items = PyQuery(page, parser='html').find('.fenzhixinxin').remove('font').items()
        branch_list = []
        for item in items:
            branch_model = {
                GsModel.Branch.COMPAY_NAME: item.find('p').eq(0).text(),
                GsModel.Branch.CODE: item.find('p').eq(1).text()
            }
            branch_list.append(branch_model)

        if len(branch_list) > 0:
            branch_info_dict[GsModel.BRANCH] = branch_list

        return branch_info_dict

    # 出资信息 基本信息下面的模块
    def get_contributive_info(self, contributive_info):
        '''
        :param contributive_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        出资信息一般会由两个列表分别进行存储, 但部分省份也可能只包含list列表, 没有详情页信息
        :return: 返回工商schema字典
        '''
        con_info_dict = {}
        pages_list = self.get_crawl_page(contributive_info, True)

        part_a_con = {}
        for page in pages_list:
            trs = PyQuery(page.get(u'text', u''), parser='html').find('.detailsListGDCZ').find('tr').items()
            for tr in trs:
                tds = tr.find('td')
                sub_model = self._get_sharehold_info_list_td_text(tds)
                if sub_model is None or len(sub_model) <= 0:
                    continue
                part_a_con[tds.eq(1).text().strip()] = sub_model

        pages_detail = self.get_crawl_page(contributive_info, True, Model.type_detail)
        part_b_con = {}
        if pages_detail is not None:
            for page in pages_detail:
                tables = PyQuery(page.get(u'text', u''), parser='html').find('.detailsList').items()
                shareholder_name, sub_model = self._get_sharehold_detail(tables)
                part_b_con[shareholder_name] = sub_model

        con_list = []
        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            con_list.append(v_list)

        if len(con_list) > 0:
            con_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = con_list

        return con_info_dict

    # 年报信息
    def get_annual_info(self, annual_item_list):
        '''
        :param annual_item_list:
        :return: 返回工商schema字典
        '''
        annual_info_dict = {}

        if annual_item_list is None or len(annual_item_list) <= 0:
            return {}

        annual_item = annual_item_list[0]
        if annual_item is None or annual_item.get(u'status', u'fail') != u'success':
            raise PageCrawlError("为抓取到相关网页,或者抓取网页失败")

        page = annual_item.get(u'text', u'')
        py_all = PyQuery(page, parser='html')
        table_list = py_all.find('table').items()
        for table in table_list:
            # 基本信息
            content = table.text()
            if u'基本信息' in content:
                tds = table.find('td[celspan!="2"]').items()
                annual_base_info = self._get_annual_base_info(tds)
                annual_info_dict.update(annual_base_info)
            # 年报 企业资产状况信息
            if u'企业资产状况信息' in content or u'生产经营情况信息' in content:
                tds = table.find('tr[@height="38px"]').find('td')
                asset_model = self._get_annual_asset_info(tds)
                annual_info_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = asset_model

        # 对外投资
        py_inv_company = py_all.find('.webContent[id=""]').items()
        inv_company_list = self._get_annual_inv_info(py_inv_company)
        if len(inv_company_list) > 0:
            annual_info_dict[AnnualReports.INVESTED_COMPANIES] = inv_company_list

        # 对外担保
        json_out_guarantee = util.get_match_value("var guaranListJsonStr='", "';", page)
        out_guarante_list = self._get_annual_out_guarantee_info(json_out_guarantee)
        if len(out_guarante_list) > 0:
            annual_info_dict[AnnualReports.OUT_GUARANTEE_INFO] = out_guarante_list

        # 网站或网店信息
        py_websites = py_all.find('.webContent[onmouseover="mouseOver(this)"]').items()
        website_list = []
        for py_web_item in py_websites:
            ps = py_web_item.find('p').items()
            web_item = self._get_annual_web_site_info(ps)
            website_list.append(web_item)
        if len(website_list) > 0:
            annual_info_dict[AnnualReports.WEBSITES] = website_list

        # 股东出资信息
        json_shareholder = util.get_match_value("var listJsonStr='", "';", page)
        shareholder_list = self._get_annual_sharehold_info(json_shareholder)
        if len(shareholder_list) > 0:
            annual_info_dict[AnnualReports.SHAREHOLDER_INFORMATION] = shareholder_list

        # 修改记录
        json_edit_change = util.get_match_value("var modifyListJsonStr='", "';", page)
        edit_change_list = self._get_annual_edit_change(json_edit_change)
        if len(edit_change_list) > 0:
            annual_info_dict[AnnualReports.EDIT_CHANGE_INFOS] = edit_change_list
        return annual_info_dict

    # 年报 修改记录
    def _get_annual_edit_change(self, json_str):
        lst = []
        if json_str is None or json_str == '':
            return lst

        js_obj = json.loads(json_str)
        for js_item in js_obj:
            edit_model = {
                AnnualReports.EditChangeInfos.CHANGE_ITEM: js_item.get(u'descr', u''),
                AnnualReports.EditChangeInfos.BEFORE_CONTENT: js_item.get(u'modifyBefore', u''),
                AnnualReports.EditChangeInfos.AFTER_CONTENT: js_item.get(u'modifyAfter', u''),
                AnnualReports.EditChangeInfos.CHANGE_DATE: js_item.get(u'modifyDate', 0),
            }
            lst.append(edit_model)
        return lst

    # 年报 对外担保
    # 查询存在对外担保信息 db.getCollection('annual_reports').find({company:/^安徽/,out_guarantee_info:{'$ne':null}},{company:1,out_guarantee_info:1}).limit(30)
    def _get_annual_out_guarantee_info(self, json_str):
        lst = []
        if json_str is None or json_str == '':
            return lst

        js_obj = util.json_loads(json_str)
        for js_item in js_obj:
            perfrom = js_item.get(u'pefPerForm', 0)
            perto = js_item.get(u'pefPerTo', 0)
            share_model = {
                AnnualReports.OutGuaranteeInfo.CREDITOR: js_item.get(u'more', u''),
                AnnualReports.OutGuaranteeInfo.OBLIGOR: js_item.get(u'mortgagor', u''),
                AnnualReports.OutGuaranteeInfo.DEBT_TYPE: js_item.get(u'priClaSecKindName', u''),
                AnnualReports.OutGuaranteeInfo.DEBT_AMOUNT: util.get_amount_with_unit(js_item.get(u'priClaSecAm', u'')),
                AnnualReports.OutGuaranteeInfo.PERFORMANCE_PERIOD: u"{0}-{1}".format(perfrom, perto),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_PERIOD: js_item.get(u'guaranPeriodName', u''),
                AnnualReports.OutGuaranteeInfo.GUARANTEE_TYPE: js_item.get(u'gaTypeName', u''),
            }
            lst.append(share_model)
        return lst

    # 年报 股东出资信息(客户端分页)
    def _get_annual_sharehold_info(self, json_str):

        lst = []
        if json_str is None or json_str == '':
            return lst

        js_obj = util.json_loads(json_str)
        for js_item in js_obj:
            share_model = {
                AnnualReports.ShareholderInformation.SHAREHOLDER_NAME: js_item.get(u'inv', u''),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                    js_item.get(u'subConAm', u'')),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TIME: str(js_item.get(u'conDate', 0)),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TYPE: js_item.get(u'conFormName', u''),
                AnnualReports.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(
                    js_item.get(u'acConAm', u'')),
                AnnualReports.ShareholderInformation.PAIED_TIME: str(js_item.get(u'realConDate', 0)),
                AnnualReports.ShareholderInformation.PAIED_TYPE: js_item.get(u'realConFormName', u''),
            }
            lst.append(share_model)
        return lst

    # 年报 对外投资信息
    def _get_annual_inv_info(self, py_items):
        lst = []
        if py_items is None:
            return lst

        for item in py_items:
            company = item.find('p').eq(0).text()
            code_array = item.find('p').eq(1).text().split(u'：')
            code = code_array[1] if len(code_array) == 2 else None
            model = {AnnualReports.InvestedCompanies.COMPANY_NAME: company,
                     AnnualReports.InvestedCompanies.CODE: code}
            lst.append(model)
        return lst

    # 年报 企业资产状况信息
    def _get_annual_asset_info(self, tds):
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

    # 年报网站信息
    def _get_annual_web_site_info(self, py_items):
        web_item = {}
        for item in py_items:
            if len(item.find('span')) == 1:
                web_item[AnnualReports.WebSites.NAME] = item.text()
            else:
                item_content = item.text().replace(u'·', u'')
                part = item_content.split(u'：', 1)
                k = AnnualReports.format_website_model(part[0].strip().replace(u' ', u''))
                web_item[k] = part[1].strip()
        return web_item

    # 年报基本信息
    def _get_annual_base_info(self, py_items):
        annual_base_info_dict = {}
        for item in py_items:
            item_content = item.text().replace(u'·', u'')
            part = item_content.split(u'：', 1)
            if len(part) < 2:
                continue

            k = AnnualReports.format_base_model(part[0].strip().replace(u' ', u''))
            annual_base_info_dict[k] = part[1].strip()
        return annual_base_info_dict
