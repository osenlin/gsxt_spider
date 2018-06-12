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


class GsxtParseLiaoNingWorker(ParseBaseWorker):
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
        # 登记状态： 存续（在营、开业、在册） 住所 ; 住所这个词不要
        '''
        page = self.get_crawl_page(base_info)
        res_single = py(page, parser='html')
        remove_dl = res_single.find('dl').find('dd').find('dl').remove('dl')
        res = res_single.find('dl').items()

        base_info_dict = self._get_base_item_info(res)
        other_part = remove_dl.text().split(' ', 1)
        if other_part is not None and len(other_part) == 2:
            k = GsModel.format_base_model(other_part[0].replace('：', ''))
            base_info_dict[k] = other_part[1]

        base_info_dict = self.bu_ding(base_info_dict)  # 补丁
        return base_info_dict

    # 股东信息
    def get_shareholder_info(self, shareholder_info):
        shareholder_info_dict = {}
        lst_shareholder = []
        page = self.get_crawl_page(shareholder_info)
        if page is None:
            return shareholder_info_dict

        native_json = util.json_loads(page)
        json_data_arr = native_json.get('jsonArray', [])
        if json_data_arr is None:
            return {}

        for data in json_data_arr:
            share_model = {}
            tj = data.get('tJsTzrxx')  # 关于钱与公示
            if len(data.get('tJsTzrrjxxList')) != 0:
                rj = data.get('tJsTzrrjxxList')[0]  # 认缴
            else:
                rj = {}
            if len(data.get('tJsTzrsjxxList')) != 0:
                sj = data.get('tJsTzrsjxxList')[0]  # 实缴
            else:
                sj = {}
            share_model[GsModel.ShareholderInformation.SHAREHOLDER_NAME] = tj.get('inv', '')
            share_model[GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT] = str(
                tj.get('lisubconam', '')) + rj.get(
                'subcurrencyName', '')  # 认缴
            share_model[GsModel.ShareholderInformation.PAIED_AMOUNT] = str(tj.get('liacconam', '')) + sj.get(
                'accurrencyName', '')  # 实缴
            share_model[GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL] = []
            sub_detail_dict = {
                GsModel.ShareholderInformation.SUBSCRIPTION_TYPE: rj.get('subconformName', ''),  # 认缴方式
                GsModel.ShareholderInformation.SUBSCRIPTION_TIME: rj.get('subcondate', ''),  # 认缴时间
                GsModel.ShareholderInformation.SUBSCRIPTION_PUBLISH_TIME: tj.get('gstimeStr', ''),  # 认缴公式时间
            }
            sub_detail_dict = self.bu_ding(sub_detail_dict)
            share_model[GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL].append(sub_detail_dict)

            share_model[GsModel.ShareholderInformation.PAIED_DETAIL] = []
            paid_detail_dict = {
                GsModel.ShareholderInformation.PAIED_TYPE: sj.get('acconformName', ''),  # 实缴类型
                GsModel.ShareholderInformation.PAIED_TIME: sj.get('accondate', ''),  # 实缴 时间
                GsModel.ShareholderInformation.PAIED_PUBLISH_TIME: tj.get('gstimeStr', ''),  # 实缴公式时间
            }
            paid_detail_dict = self.bu_ding(paid_detail_dict)
            share_model[GsModel.ShareholderInformation.PAIED_DETAIL].append(paid_detail_dict)
            share_model = self.bu_ding(share_model)  # 补丁

            lst_shareholder.append(share_model)
        shareholder_info_dict[GsModel.SHAREHOLDER_INFORMATION] = lst_shareholder
        return shareholder_info_dict

    # 变更信息
    def get_change_info(self, change_info):
        change_info_dict = {}
        pages = self.get_crawl_page(change_info, True)
        all_item_list = []
        for page in pages:
            page_text = page.get(u'text', u'')
            # 有特殊例子
            page_text = util.get_match_value('global_bgxxJosnData=\[', '}\];', page_text)
            if page_text != '':
                page_text += "}"
            json_data_arr = util.json_loads("[{0}]".format(page_text[:]))
            if json_data_arr is None:
                return {}

            for data in json_data_arr:
                data_item_list = {
                    'after_content': data.get(u'altaf', u''),
                    'before_content': data.get(u'altbe', u''),
                    'change_item': data.get(u'altitemName', u''),
                    'change_date': data.get(u'altdate', u'')}
                data_item_list = self.bu_ding(data_item_list)  # 补丁
                all_item_list.append(data_item_list)
        change_info_dict['changerecords'] = all_item_list
        return change_info_dict

    # 主要人员
    def get_key_person_info(self, key_person_info):
        key_person_info_dict = {}
        all_item_list = []
        page_text = self.get_crawl_page(key_person_info)
        if page_text is None:
            return key_person_info_dict

        page_text = util.get_match_value('obj=\[', '\];', page_text)
        json_data_arr = util.json_loads("[{0}]".format(page_text[:]))
        if json_data_arr is None:
            return key_person_info_dict

        for data in json_data_arr:
            data_item_list = {
                'key_person_name': data.get(u'name', u''),
                'key_person_position': data.get(u'positionName', u'')
            }
            data_item_list = self.bu_ding(data_item_list)  # 补丁
            all_item_list.append(data_item_list)
        key_person_info_dict['key_person'] = all_item_list
        return key_person_info_dict

    # 分支机构
    def get_branch_info(self, branch_info):
        all_item_list = []
        branch_info_dict = {}
        page_text = self.get_crawl_page(branch_info)
        if page_text is None:
            return branch_info_dict

        page_text = util.get_match_value("fzjgPaging\\(", '],', page_text)
        page_text += ']'
        json_data_arr = util.json_loads("{0}".format(page_text[:]))
        if json_data_arr is None:
            return branch_info_dict

        for data in json_data_arr:
            data_item_list = {
                'compay_name': data.get(u'brname', u''),
                'regno': data.get(u'regno', u'')
            }
            data_item_list = self.bu_ding(data_item_list)  # 补丁
            all_item_list.append(data_item_list)
        branch_info_dict['branch'] = all_item_list
        return branch_info_dict

    # 出资信息
    def get_contributive_info(self, con_info):
        con_info_dict = {}
        part_a_con = {}
        part_b_con = {}
        lst_con = []
        pages_list = self.get_crawl_page(con_info, True)

        for page in pages_list:
            page_text = page.get(u'text', u'')
            page_text = util.get_match_value("global_gdJosnData=\[", '\];', page_text)
            json_data_arr = util.json_loads("[{0}]".format(page_text[:]))
            if json_data_arr is None:
                return con_info_dict

            for data in json_data_arr:
                data_item_list = {
                    'shareholder_name': data.get(u'inv', u''),
                    'shareholder_type': data.get(u'invtypeName', u''),
                    'certificate_no': data.get(u'blicno', u''),
                    'certificate_type': data.get(u'blictypeName', u'')
                }
                if data_item_list['certificate_no'] == u'':
                    data_item_list['certificate_no'] = u'非公示项'
                    data_item_list['certificate_type'] = u'非公示项'

                shareholder_name = data_item_list['shareholder_name']
                part_a_con[shareholder_name] = data_item_list

        pages_detail = self.get_crawl_page(con_info, True, Model.type_detail)

        if pages_detail is not None:
            for page_item in pages_detail:
                if page_item is None:
                    continue

                shareholder_name, sub_model = self.get_con_detail(page_item.get(u'text', u''))
                part_b_con[shareholder_name] = sub_model

        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            lst_con.append(v_list)
        con_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = lst_con
        return con_info_dict

    def get_con_detail(self, page):
        shareholder_name = ""
        sub_model = {}
        json_data_arr = util.json_loads(page)
        if json_data_arr is None:
            return shareholder_name, sub_model

        for json_item in json_data_arr:
            if len(json_item.get('tRegTzrrjxxList')) != 0:
                rj = json_item.get('tRegTzrrjxxList')[0]  # 认缴
            else:
                rj = {}
            if len(json_item.get('tRegTzrsjxxList')) != 0:
                sj = json_item.get('tRegTzrsjxxList')[0]  # 实缴
            else:
                sj = {}
            if len(json_item.get('tRegTzrxx')) != 0:
                other = json_item.get('tRegTzrxx')

            shareholder_name = other.get('inv', '')
            sub_model[GsModel.ContributorInformation.SHAREHOLDER_NAME] = other.get('inv', '')
            sub_model[GsModel.ContributorInformation.SUBSCRIPTION_AMOUNT] = util.get_amount_with_unit(
                other.get('lisubconam', ''))
            sub_model[GsModel.ContributorInformation.PAIED_AMOUNT] = util.get_amount_with_unit(
                other.get('liacconam', ''))

            lst_sub_detail = []

            sub_model_detail = {
                GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TYPE: rj.get('conformName', ''),
                GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                    rj.get('subconam', '')),
                GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TIME:
                    rj.get('condate', ''),
            }
            sub_model_detail = self.bu_ding(sub_model_detail)  # 补丁
            lst_sub_detail.append(sub_model_detail)
            sub_model[GsModel.ContributorInformation.SUBSCRIPTION_DETAIL] = lst_sub_detail

            lst_paid_detail = []
            paid_model_detail = {
                GsModel.ContributorInformation.PaiedDetail.PAIED_TYPE: sj.get('conformName', ''),
                GsModel.ContributorInformation.PaiedDetail.PAIED_AMOUNT: util.get_amount_with_unit(
                    sj.get('acconam', '')),
                GsModel.ContributorInformation.PaiedDetail.PAIED_TIME:
                    sj.get('condate', ''),
            }
            paid_model_detail = self.bu_ding(paid_model_detail)  # 补丁
            lst_paid_detail.append(paid_model_detail)
            sub_model[GsModel.ContributorInformation.PAIED_DETAIL] = lst_paid_detail
            sub_model = self.bu_ding(sub_model)  # 补丁
        return shareholder_name, sub_model

    # 清算信息
    def get_liquidation_info(self, liquidation_info):
        return {}

    # 年报信息
    def get_annual_info(self, annual_item):
        annual_info_dict = {}
        if annual_item is None or annual_item[0].get(u'status', u'fail') != u'success':
            raise IndexError("为抓取到相关网页,或者抓取网页失败")

        page = annual_item[0].get(u'text', u'')
        py_all = py(page, parser='html')
        # 基本信息
        dls = py_all.find('#gsfrnbxq_jbxx').find('dl').items()
        annual_base_info = self.get_annual_base_info(dls)
        annual_info_dict.update(annual_base_info)

        # 年报 企业资产状况信息
        table = py_all.find('#zczcxx').find('table')
        trs = table.find('tr').items()
        asset_model = self.get_annual_asset_info(trs)
        annual_info_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = asset_model

        # 对外投资
        json_inv_company = util.get_match_value("global_gsfrnbxqtzxxJosnData=\[", "\];", page)
        annual_info_dict[AnnualReports.INVESTED_COMPANIES] = self.get_annual_inv_info(json_inv_company)

        # 对外担保
        json_out_guarantee = util.get_match_value("global_gsfrnbxqdbxxJosnData=\[", "\];", page)
        lst_out_guaranty = self.get_annual_out_guarantee_info(json_out_guarantee)
        annual_info_dict[AnnualReports.OUT_GUARANTEE_INFO] = lst_out_guaranty

        # 网站或网店信息
        json_out_websites = util.get_match_value("swPaging\(\[", "\]\);", page)
        lst_websites = self.get_annual_out_website(json_out_websites)
        annual_info_dict[AnnualReports.WEBSITES] = lst_websites

        # 股东出资信息
        json_share_hold = util.get_match_value("global_gsfrnbxqczxxJosnData=\[", "\];", page)
        lst_share_hold = self.get_annual_share_hold_info(json_share_hold)
        annual_info_dict[AnnualReports.SHAREHOLDER_INFORMATION] = lst_share_hold

        # 修改记录
        json_edit_change = util.get_match_value("global_gsfrnbxqxgxxJosnData=\[", "\];", page)
        lst_edit_change = self.get_annual_edit_change(json_edit_change)
        annual_info_dict[AnnualReports.EDIT_CHANGE_INFOS] = lst_edit_change

        # 股权变更
        json_edit_shareholding_change = util.get_match_value("global_gsfrnbxqbgxxJosnData=\[", "\];", page)
        lst_edit_shareholding_change = self.get_annual_edit_shareholding_change(json_edit_shareholding_change)
        annual_info_dict[AnnualReports.EDIT_SHAREHOLDING_CHANGE_INFOS] = lst_edit_shareholding_change
        return annual_info_dict

    @staticmethod
    def get_annual_base_info(dls):
        annual_base_info = {}
        for dl in dls:
            be_k_list = dl.find('dt').text().split(u'：', 2)
            k = AnnualReports.format_base_model(be_k_list[0])
            annual_base_info[k] = dl.find('dd').text()
        return annual_base_info

    @staticmethod
    def get_annual_asset_info(trs):
        asset_model = {}
        for tr in trs:
            k1 = AnnualReports.format_asset_model(tr.find('th').eq(0).text().strip())
            asset_model[k1] = tr.find('td').eq(0).text().strip().replace(' ', '')
            k2 = AnnualReports.format_asset_model(tr.find('th').eq(1).text().strip())
            asset_model[k2] = tr.find('td').eq(1).text().strip().replace(' ', '')
        return asset_model

    def get_annual_inv_info(self, json_inv_company):
        lst = []
        if json_inv_company is None or len(json_inv_company) < 10:
            return lst

        json_data_arr = util.json_loads("[{0}]".format(json_inv_company))
        if json_data_arr is None:
            return lst

        for js_item in json_data_arr:
            model = {AnnualReports.InvestedCompanies.COMPANY_NAME: js_item.get(u'inventname', u''),
                     AnnualReports.InvestedCompanies.CODE: js_item.get(u'regno', u'')}
            model = self.bu_ding(model)  # 补丁
            lst.append(model)
        return lst

    # 年报 对外担保
    def get_annual_out_guarantee_info(self, json_str):
        lst = []
        if json_str is None or len(json_str) < 10:
            return lst

        json_data_arr = util.json_loads("[{0}]".format(json_str))
        if json_data_arr is None:
            return lst

        for js_item in json_data_arr:
            performance = js_item.get(u'pefperformandto', u'')

            share_model = {
                AnnualReports.OutGuaranteeInfo.CREDITOR: js_item.get(u'more', u''),  #
                AnnualReports.OutGuaranteeInfo.OBLIGOR: js_item.get(u'mortgagor', u''),  #
                AnnualReports.OutGuaranteeInfo.DEBT_TYPE: js_item.get(u'priclaseckindvalue', u''),  #
                AnnualReports.OutGuaranteeInfo.DEBT_AMOUNT: util.get_amount_with_unit(js_item.get(u'priclasecam', u'')),
                AnnualReports.OutGuaranteeInfo.PERFORMANCE_PERIOD: performance,
                AnnualReports.OutGuaranteeInfo.GUARANTEE_PERIOD: js_item.get(u'guaranperiodvalue', u''),  # 担保期限
                AnnualReports.OutGuaranteeInfo.GUARANTEE_TYPE: js_item.get(u'gatypevalue', u''),  # 担保方式
            }
            share_model = self.bu_ding(share_model)  # 补丁
            lst.append(share_model)
        return lst

    def get_annual_out_website(self, json_str):
        lst = []
        if json_str is None or len(json_str) < 10:
            return lst

        json_data_arr = util.json_loads("[{0}]".format(json_str))
        if json_data_arr is None:
            return lst

        for js_item in json_data_arr:
            web_model = {
                AnnualReports.WebSites.TYPE: js_item.get(u'typofwebName', u''),
                AnnualReports.WebSites.SITE: js_item.get(u'domain', u''),
                AnnualReports.WebSites.NAME: js_item.get(u'websitname', u'')
            }
            web_model = self.bu_ding(web_model)  # 补丁
            lst.append(web_model)
        return lst

    def get_annual_share_hold_info(self, json_str):
        lst = []
        if json_str is None or len(json_str) < 10:
            return lst

        json_data_arr = util.json_loads("[{0}]".format(json_str))
        if json_data_arr is None:
            return lst

        for js_item in json_data_arr:
            share_model = {
                AnnualReports.ShareholderInformation.SHAREHOLDER_NAME: js_item.get(u'inv', u''),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_AMOUNT: js_item.get(u'lisubconam', u'') + u'万元',
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TIME: js_item.get(u'subcondatelabel', u''),  # 认缴时间
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TYPE: js_item.get(u'subconformvalue', u''),  # 认缴类型
                AnnualReports.ShareholderInformation.PAIED_AMOUNT: js_item.get(u'liacconam', u'') + u'万元',  # 1实缴金额
                AnnualReports.ShareholderInformation.PAIED_TIME: js_item.get(u'accondatelabel', u''),  # 实缴时间
                AnnualReports.ShareholderInformation.PAIED_TYPE: js_item.get(u'acconformvalue', u''),  # 实缴类型
            }
            share_model = self.bu_ding(share_model)  # 补丁
            lst.append(share_model)
        return lst

    def get_annual_edit_change(self, json_str):
        lst = []
        if json_str is None or len(json_str) < 10:
            return lst

        json_data_arr = util.json_loads("[{0}]".format(json_str))
        if json_data_arr is None:
            return lst

        for js_item in json_data_arr:
            edit_model = {
                AnnualReports.EditChangeInfos.CHANGE_ITEM: js_item.get(u'alt', u''),
                AnnualReports.EditChangeInfos.BEFORE_CONTENT: js_item.get(u'altbe', u''),
                AnnualReports.EditChangeInfos.AFTER_CONTENT: js_item.get(u'altaf', u''),
                AnnualReports.EditChangeInfos.CHANGE_DATE: js_item.get(u'getAltdatevalue', u'')
            }
            edit_model = self.bu_ding(edit_model)  # 补丁
            lst.append(edit_model)
        return lst

    def get_annual_edit_shareholding_change(self, json_str):
        lst = []
        if json_str is None or len(json_str) < 10:
            return lst

        json_data_arr = util.json_loads("[{0}]".format(json_str))
        if json_data_arr is None:
            return lst

        for js_item in json_data_arr:
            edit_model = {
                AnnualReports.EditShareholdingChangeInfos.SHAREHOLDER_NAME: js_item.get(u'inv', u''),
                AnnualReports.EditShareholdingChangeInfos.BEFORE_CONTENT: js_item.get(u'transbmpr', u''),
                AnnualReports.EditShareholdingChangeInfos.AFTER_CONTENT: js_item.get(u'transampr', u''),
                AnnualReports.EditShareholdingChangeInfos.CHANGE_DATE: js_item.get(u'altdatelabel', u'')
            }
            edit_model = self.bu_ding(edit_model)  # 补丁
            lst.append(edit_model)
        return lst

    @staticmethod
    def bu_ding(temp_model):
        for k, v in temp_model.items():
            if v is None:
                v = ''
                temp_model[k] = v
        return temp_model
