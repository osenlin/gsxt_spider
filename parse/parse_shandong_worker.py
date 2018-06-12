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
import traceback

from base.parse_base_worker import ParseBaseWorker
from common import util
from common.annual_field import *
from common.global_field import Model
from common.gsxt_field import *


# todo 缺失出资信息详情页中的认缴和实缴明细
# todo 股东信息里面的单位金额如何处理
# todo 山东泰山能源有限责任公司 这个企业没有解析出详情页

class GsxtParseShanDongWorker(ParseBaseWorker):
    def __init__(self, **kwargs):
        ParseBaseWorker.__init__(self, **kwargs)

    # 基本信息
    def get_base_info(self, base_info):
        """
        :param base_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        基本信息一般存储在list列表中, 因为基本信息不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        """
        page = self.get_crawl_page(base_info)
        bi = util.json_loads(page).get('jbxx')
        if bi is None:
            return {}

        page_base_info_dict = {}
        base_info_dict = {}
        page_base_info_dict[u'统一社会信用代码'] = bi.get('uniscid') if 'uniscid' in bi.keys() \
                                                                and bi.get('uniscid') is not None else bi.get('regno')
        page_base_info_dict[u'企业名称'] = bi.get('entname') if 'entname' in bi.keys() else bi.get('traname')
        page_base_info_dict[u'类型'] = bi.get('enttype')
        registered_capital = bi.get('regcapcur') if bi.get('regcapcur') is not None else u'元'
        page_base_info_dict[u'注册资本'] = u'{0}万{1}'.format(bi.get('regcap'), registered_capital)
        page_base_info_dict[u'成立日期'] = bi.get('estdate')
        page_base_info_dict[u'经营期限自'] = bi.get('opfrom')
        page_base_info_dict[u'经营期限至'] = bi.get('opto')
        page_base_info_dict[u'登记机关'] = bi.get('regorg')
        page_base_info_dict[u'核准日期'] = bi.get('apprdate')
        page_base_info_dict[u'登记状态'] = bi.get('regstate')
        page_base_info_dict[u'住所'] = bi.get('dom') if 'dom' in bi.keys() else bi.get('oploc')
        page_base_info_dict[u'经营范围'] = bi.get('opscope')
        if 'compform' in bi.keys():
            page_base_info_dict[u'组成形式'] = u'个体经营' if bi.get('compform') == '1' else u'家庭经营'
        if 'opername' in bi.keys():
            page_base_info_dict[u'经营者'] = bi.get('opername')
        else:
            if bi.get('lereptype', '') is not None:
                page_base_info_dict[unicode(bi.get('lereptype', ''))] = bi.get('lerep')
            else:
                page_base_info_dict[u'法定代表人'] = bi.get('lerep')

        bi_reg_state_dict = {'0': u'迁出', '1': u'存续', '2': u'吊销', '3': u'注销'}
        if bi.get('regstate') in bi_reg_state_dict.keys():
            page_base_info_dict[u'登记状态'] = bi_reg_state_dict.get(bi.get('regstate'))

        page_base_info_dict = replace_none(page_base_info_dict)
        for k, v in page_base_info_dict.items():
            new_k = GsModel.format_base_model(k)
            base_info_dict[new_k] = v
        base_info_dict[GsModel.PERIOD] = u"{0}至{1}".format(
            base_info_dict.get(GsModel.PERIOD_FROM), base_info_dict.get(GsModel.PERIOD_TO))

        # 主要人员
        try:
            key_person_info_dict = self.get_key_person_info(page)
        except:
            self.log.error('company:{0},error-part:get_key_person_info,error-info:{1}'.format(
                page_base_info_dict.get('company', ''), traceback.format_exc()))
            key_person_info_dict = {}
        base_info_dict.update(key_person_info_dict)

        # 分支机构
        try:
            branch_info_dict = self.get_branch_info(page)
        except:
            self.log.error('company:{0},error-part:get_key_person_info,error-info:{1}'.format(
                page_base_info_dict.get('company', ''), traceback.format_exc()))
            branch_info_dict = {}
        base_info_dict.update(branch_info_dict)

        # 变更信息
        try:
            change_info_dict = self.get_change_info(page)
        except:
            self.log.error('company:{0},error-part:changerecords_info,error-info:{1}'.format(
                page_base_info_dict.get('company', u''), traceback.format_exc()))
            change_info_dict = {}
        base_info_dict.update(change_info_dict)

        return base_info_dict

    # 股东信息 靠下面的是股东信息
    # done 日期要格式化
    def get_shareholder_info(self, shareholder_info):
        shareholder_info_dict = {}
        pages = self.get_crawl_page(shareholder_info, True)
        lst_shareholder = []

        page_render_conform = {
            '1': u'货币',
            '2': u'实物',
            '3': u'知识产权',
            '4': u'债权',
            '6': u'土地使用权',
            '7': u'股权',
            '9': u'其他',
            'default': u'货币'
        }
        for page in pages:
            page = util.json_loads(page.get('text')).get('czxxs', u'')
            if page is None:
                return {}

            for item in page:
                share_model = {}
                share_detail = item.get('czxx')
                share_model[GsModel.ShareholderInformation.SHAREHOLDER_NAME] = share_detail.get('inv')

                sub_detail = item.get('rjxxs', u'')
                share_model[GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL] = []
                li_sub_con = 0
                for sd in sub_detail:
                    sub_con = is_num(sd.get('subconam'))
                    li_sub_con += sub_con
                    sub_dict = {
                        GsModel.ShareholderInformation.SUBSCRIPTION_TYPE: page_render_conform.get(
                            sd.get('conform')),
                        GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(sub_con),
                        GsModel.ShareholderInformation.SUBSCRIPTION_TIME: sd.get('condate'),
                        GsModel.ShareholderInformation.SUBSCRIPTION_PUBLISH_TIME: sd.get('updatetime'),
                    }
                    sub_dict = replace_none(sub_dict)
                    share_model[GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL].append(sub_dict)
                share_model[GsModel.ShareholderInformation.SUBSCRIPTION_AMOUNT] = util.get_amount_with_unit(li_sub_con)

                paid_detail = item.get('sjxxs', u'')
                share_model[GsModel.ShareholderInformation.PAIED_DETAIL] = []
                li_ac_con = 0
                for pd in paid_detail:
                    ac_con = is_num(pd.get('acconam'))
                    li_ac_con += ac_con
                    paid_dict = {
                        GsModel.ShareholderInformation.PAIED_TYPE: page_render_conform.get(pd.get('conform')),
                        GsModel.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(ac_con),
                        GsModel.ShareholderInformation.PAIED_TIME: pd.get('condate'),
                        GsModel.ShareholderInformation.PAIED_PUBLISH_TIME: pd.get('updatetime'),
                    }
                    paid_dict = replace_none(paid_dict)
                    share_model[GsModel.ShareholderInformation.PAIED_DETAIL].append(paid_dict)
                share_model[GsModel.ShareholderInformation.PAIED_AMOUNT] = util.get_amount_with_unit(li_ac_con)
                share_model = replace_none(share_model)

                lst_shareholder.append(share_model)
        shareholder_info_dict[GsModel.SHAREHOLDER_INFORMATION] = lst_shareholder
        return shareholder_info_dict

    # 变更信息
    def get_change_info(self, page):
        change_info_dict = {}
        lst_change_records = []
        json_data_arr = util.json_loads(page).get('bgsx', u'')
        if json_data_arr is None:
            return {}

        for json_data in json_data_arr:
            change_model = {
                GsModel.ChangeRecords.CHANGE_ITEM: json_data.get('altitem'),
                GsModel.ChangeRecords.BEFORE_CONTENT: util.format_content(json_data.get('altbe')),
                GsModel.ChangeRecords.AFTER_CONTENT: util.format_content(json_data.get('altaf')),
                GsModel.ChangeRecords.CHANGE_DATE: json_data.get('altdate')
            }
            change_model = replace_none(change_model)
            lst_change_records.append(change_model)
        change_info_dict[GsModel.CHANGERECORDS] = lst_change_records
        return change_info_dict

    # 主要人员
    def get_key_person_info(self, page):
        lst_key_person = []
        key_person_info_dict = {}
        json_data_arr = util.json_loads(page).get('ryxx', u'')
        if json_data_arr is None:
            return {}

        for json_data in json_data_arr:
            key_person_model = {
                GsModel.KeyPerson.KEY_PERSON_NAME: json_data.get('name'),
                GsModel.KeyPerson.KEY_PERSON_POSITION: json_data.get('position')}
            key_person_model = replace_none(key_person_model)
            lst_key_person.append(key_person_model)

        key_person_info_dict[GsModel.KEY_PERSON] = lst_key_person
        return key_person_info_dict

    # 分支机构
    def get_branch_info(self, page):
        branch_info_dict = {}
        json_data_arr = util.json_loads(page).get('fzjg', u'')
        lst_branch = []
        if json_data_arr is None:
            return {}

        for json_data in json_data_arr:
            branch_model = {
                GsModel.Branch.COMPAY_NAME: json_data.get('brname'),
                GsModel.Branch.CODE: json_data.get('regno'),
                GsModel.Branch.REGISTERED_ADDRESS: json_data.get('regorg')
            }
            branch_model = replace_none(branch_model)
            lst_branch.append(branch_model)
        branch_info_dict[GsModel.BRANCH] = lst_branch
        return branch_info_dict

    # 出资信息 基本信息下面的模块
    def get_contributive_info(self, con_info):
        page_list = self.get_crawl_page(con_info, True)
        con_info_dict = {}
        part_a_con = {}
        part_b_con = {}
        for page in page_list:
            json_data_arr = util.json_loads(page.get('text')).get('czxx', [])
            if json_data_arr is None:
                continue

            for json_data in json_data_arr:
                sub_model = {
                    GsModel.ContributorInformation.SHAREHOLDER_NAME: json_data.get('inv'),
                    GsModel.ContributorInformation.SHAREHOLDER_TYPE: json_data.get('invtype'),
                    GsModel.ContributorInformation.CERTIFICATE_TYPE: json_data.get('blictype'),
                    GsModel.ContributorInformation.CERTIFICATE_NO: json_data.get('blicno')
                }
                sub_model = replace_none(sub_model)
                part_a_con[sub_model[GsModel.ContributorInformation.SHAREHOLDER_NAME]] = sub_model

        pages_detail = self.get_crawl_page(con_info, True, Model.type_detail)
        if pages_detail is not None:
            for detail in pages_detail:
                shareholder_name, sub_model = self.get_share_hold_detail(detail)
                part_b_con[shareholder_name] = sub_model

        lst_con = []
        for k_list, v_list in part_a_con.items():
            v_list.update(part_b_con.get(k_list, {}))
            lst_con.append(v_list)
        con_info_dict[GsModel.CONTRIBUTOR_INFORMATION] = lst_con
        return con_info_dict

    # 从json中获取出资信息的股东信息详情页
    @staticmethod
    def get_share_hold_detail(detail):
        shareholder_name = ""
        sh_model = {}
        paid_list = []
        sub_list = []
        if detail is None:
            return shareholder_name, sh_model

        sh_type = {
            '1': u'货币',
            '2': u'实物',
            '3': u'知识产权',
            '4': u'债权',
            '6': u'土地使用权',
            '7': u'股权',
            '9': u'其他',
            'default': u'货币'
        }
        detail_text = util.json_loads(detail.get('text'))
        if detail_text is None:
            return shareholder_name, sh_model

        json_data = detail_text.get('czxx', {})
        if json_data is None:
            return shareholder_name, sh_model

        sh_model = {
            GsModel.ContributorInformation.SHAREHOLDER_NAME: json_data.get('inv'),
            GsModel.ContributorInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                json_data.get('lisubconam')),
            GsModel.ContributorInformation.PAIED_AMOUNT: util.get_amount_with_unit(
                json_data.get('liacconam'))
        }
        rjs = detail_text.get('rjs', {})
        if rjs is None:
            return shareholder_name, sh_model

        for rj in rjs:
            sub_model_detail = {
                GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TYPE: sh_type.get(rj.get('conform')),
                GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                    rj.get('subconam')),
                GsModel.ContributorInformation.SubscriptionDetail.SUBSCRIPTION_TIME: rj.get('condate')
            }
            sub_model_detail = replace_none(sub_model_detail)
            sub_list.append(sub_model_detail)
        sh_model[GsModel.ContributorInformation.SUBSCRIPTION_DETAIL] = sub_list

        sjs = detail_text.get('sjs', {})
        if sjs is None:
            return shareholder_name, sh_model

        for sj in sjs:
            paid_model_detail = {
                GsModel.ContributorInformation.PaiedDetail.PAIED_TYPE: sh_type.get(sj.get('conform')),
                GsModel.ContributorInformation.PaiedDetail.PAIED_AMOUNT: util.get_amount_with_unit(
                    sj.get('acconam')),
                GsModel.ContributorInformation.PaiedDetail.PAIED_TIME: sj.get('condate')
            }
            paid_model_detail = replace_none(paid_model_detail)
            paid_list.append(paid_model_detail)
        sh_model[GsModel.ContributorInformation.PAIED_DETAIL] = paid_list

        sh_model = replace_none(sh_model)
        shareholder_name = sh_model[GsModel.ContributorInformation.SHAREHOLDER_NAME]
        return shareholder_name, sh_model

    # 年报信息
    def get_annual_info(self, annual_item_list):
        return ParseShanDongAnnual(annual_item_list, self.log).get_result()


# 年报解析类
class ParseShanDongAnnual(object):
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

        if self.annual_item_list is None or self.annual_item_list[0].get(u'status', u'fail') != u'success':
            raise IndexError("为抓取到相关网页,或者抓取网页失败")

        page = self.annual_item_list[0].get(u'text', u'')
        json_all = util.json_loads(page)

        annual_base_info = self.get_annual_base_info(json_all)
        self.annual_info_dict.update(annual_base_info)

        # 企业资产状况信息
        asset_model = self.get_annual_asset_info(json_all.get('zczk', u''))
        self.annual_info_dict[AnnualReports.ENTERPRISE_ASSET_STATUS_INFORMATION] = asset_model

        # 对外投资
        json_inv_company = json_all.get('dwtzs', u'')
        self.annual_info_dict[AnnualReports.INVESTED_COMPANIES] = self.get_annual_inv_info(json_inv_company)

        # 股东出资信息
        json_share_hold = json_all.get('czxxs', '')
        lst_share_hold = self.get_annual_share_hold_info(json_share_hold)
        self.annual_info_dict[AnnualReports.SHAREHOLDER_INFORMATION] = lst_share_hold

        # 对外担保
        json_out_guarantee = json_all.get('dwdbs', '')
        lst_out_guaranty = self.get_annual_out_guarantee_info(json_out_guarantee)
        self.annual_info_dict[AnnualReports.OUT_GUARANTEE_INFO] = lst_out_guaranty

        # 网站或网店信息
        json_out_websites = json_all.get('wdxxs', '')
        lst_websites = self.get_annual_out_website(json_out_websites)
        self.annual_info_dict[AnnualReports.WEBSITES] = lst_websites

        # 修改记录
        json_edit_change = json_all.get('alterHis', '')
        lst_edit_change = self.get_annual_edit_change(json_edit_change)
        self.annual_info_dict[AnnualReports.EDIT_CHANGE_INFOS] = lst_edit_change

        # 股权变更
        json_edit_shareholding_change = json_all.get('gqbgs', '')
        lst_edit_shareholding_change = self.get_annual_edit_shareholding_change(json_edit_shareholding_change)
        self.annual_info_dict[AnnualReports.EDIT_SHAREHOLDING_CHANGE_INFOS] = lst_edit_shareholding_change

    # 年报 企业资产状况信息
    @staticmethod
    def get_annual_asset_info(json_data):
        # 资产总额
        if json_data is None:
            return {}
        if json_data.get('ifassgro', '') == u'1':
            total_assets = json_data.get('assgro', '')
            total_assets = util.get_amount_with_unit(total_assets)
        else:
            total_assets = u'企业选择不公示'
        # 所有者权益合计
        if json_data.get('iftotequ', '') == u'1':
            total_owners_equity = json_data.get('totequ', '')
            total_owners_equity = util.get_amount_with_unit(total_owners_equity)
        else:
            total_owners_equity = u'企业选择不公示'
        # 销售总额
        if json_data.get('zczkvendinc', '') == u'1':
            total_sales = json_data.get('vendinc', '')
            total_sales = util.get_amount_with_unit(total_sales)
        else:
            total_sales = u'企业选择不公示'
        # 利润总额
        if json_data.get('ifprogro', '') == u'1':
            profit_total = json_data.get('progro', '')
            profit_total = util.get_amount_with_unit(profit_total)
        else:
            profit_total = u'企业选择不公示'
        # 营业总收入中主营业务收入
        if json_data.get('ifmaibusinc', '') == u'1':
            main_business_income = json_data.get('maibusinc', '')
            main_business_income = util.get_amount_with_unit(main_business_income)
        else:
            main_business_income = u'企业选择不公示'
        # 净利润
        if json_data.get('ifnetinc', '') == u'1':
            net_profit = json_data.get('netinc', '')
            net_profit = util.get_amount_with_unit(net_profit)
        else:
            net_profit = u'企业选择不公示'

        # 纳税总额
        if json_data.get('ifratgro', '') == u'1':
            total_tax = json_data.get('ratgro', '')
            total_tax = util.get_amount_with_unit(total_tax)
        else:
            total_tax = u'企业选择不公示'
        # 负债总额
        if json_data.get('ifliagro', '') == u'1':
            total_liabilities = json_data.get('liagro', '')
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
        asset_model = replace_none(asset_model)
        return asset_model

    # 年报基本信息
    @staticmethod
    def get_annual_base_info(json_data):
        # 从业人数
        if json_data.get('ifpubempnum', '') == u'1':
            employed_population = json_data.get('empnum', '')
        else:
            employed_population = u'企业选择不公示'
        # 经营状况
        business_status_num = json_data.get('busst', '')
        if business_status_num == u'1':
            business_status = u'开业'
        elif business_status_num == u'4':
            business_status = u'歇业'
        else:
            business_status = u'清算'
        # 网店状况
        if len(json_data.get('wdxxs', '')) > 0:
            is_web = u'是'
        else:
            is_web = u'否'
        # ·是否有投资信息或购买其他公司股权：
        if len(json_data.get('dwtzs', '')) > 0:
            is_invest = u'是'
        else:
            is_invest = u'否'
        # 是否股权转让
        if len(json_data.get('gqbgs', '')) > 0:
            is_transfer = u'是'
        else:
            is_transfer = u'否'

        # 是否对外担保
        if len(json_data.get('dwdbs', '')) > 0:
            is_out_guarantee = u'是'
        else:
            is_out_guarantee = u'否'
        basic_info_data = json_data.get('jbxx', '')
        if basic_info_data is None:
            return {}

        basic_model = {
            AnnualReports.CODE: basic_info_data.get('regno'),  # 注册号 1
            AnnualReports.ZIP_CODE: basic_info_data.get('postalcode'),  # 邮政2
            AnnualReports.CONTACT_NUMBER: basic_info_data.get('tel'),  # 联系电话 3
            AnnualReports.COMPANY_NAME: basic_info_data.get('entname'),  # 公司 4
            AnnualReports.ADDRESS: basic_info_data.get('addr'),  # 地址 5
            AnnualReports.EMAIL: basic_info_data.get('email'),  # 邮件 6
            # 从业人数
            AnnualReports.EMPLOYED_POPULATION: employed_population,
            # 经营状况
            AnnualReports.BUSINESS_STATUS: business_status,
            # 网站
            AnnualReports.IS_WEB: is_web,
            # 是否买其他股权
            AnnualReports.IS_INVEST: is_invest,
            # 是否股权转让
            AnnualReports.IS_TRANSFER: is_transfer,
            # 是否有对外担保
            AnnualReports.IS_OUT_GUARANTEE: is_out_guarantee,
            AnnualReports.EMPLOYED_POPULATION_WOMAN: basic_info_data.get('womenpnum'),  # 女士从业 7
            AnnualReports.ENTERPRISE_HOLDING: basic_info_data.get('kgqk'),  # 企业控股情况
            AnnualReports.BUSINESS_ACTIVITIES: basic_info_data.get('zyyw')  # 企业主营业务活动
        }
        basic_model = replace_none(basic_model)
        return basic_model

    @staticmethod
    def switch_type(item_key):
        switch = {
            "1": u'货币',
            "2": u'实物',
            "3": u'知识产权',
            "4": u'债权',
            "6": u'土地使用权',
            "7": u'股权',
            "9": u'其他',
        }
        if item_key not in switch.keys():
            return ''
        return switch[item_key]

    # 年报 对外担保
    @staticmethod
    def get_annual_out_guarantee_info(json_list):
        lst = []
        for js_item in json_list:
            time_from = js_item.get(u'pefperfrom', u'')
            time_to = js_item.get(u'pefperto', u'')
            performance_period = "{0}-{1}".format(time_from, time_to)

            if js_item.get('priclaseckind', u'') == u'1':
                debt_type = "合同"
            else:
                debt_type = "其他"

            if js_item.get('guaranperiod', u'') == u'1':
                guarantee_period = "期限"
            else:
                guarantee_period = "未约定"

            if js_item.get('gatype', u'') == u'1':
                guarantee_type = "一般保证"
            elif js_item.get('gatype', u'') == u'2':
                guarantee_type = "连带保证"
            else:
                guarantee_type = "未约定"
            share_model = {
                AnnualReports.OutGuaranteeInfo.CREDITOR: js_item.get(u'more'),  #
                AnnualReports.OutGuaranteeInfo.OBLIGOR: js_item.get(u'mortgagor'),  #
                AnnualReports.OutGuaranteeInfo.DEBT_TYPE: debt_type,  #
                AnnualReports.OutGuaranteeInfo.DEBT_AMOUNT: util.get_amount_with_unit(js_item.get(u'priclasecam')),
                AnnualReports.OutGuaranteeInfo.PERFORMANCE_PERIOD: performance_period,
                AnnualReports.OutGuaranteeInfo.GUARANTEE_PERIOD: guarantee_period,  # 担保期限
                AnnualReports.OutGuaranteeInfo.GUARANTEE_TYPE: guarantee_type,  # 担保方式
            }
            share_model = replace_none(share_model)
            lst.append(share_model)

        return lst

    # 年报 对外投资信息
    @staticmethod
    def get_annual_inv_info(json_list):
        lst = []
        for js_item in json_list:
            model = {AnnualReports.InvestedCompanies.COMPANY_NAME: js_item.get(u'entname'),
                     AnnualReports.InvestedCompanies.CODE: js_item.get(u'regno')}
            model = replace_none(model)
            lst.append(model)
        return lst

    # 年报网站信息
    @staticmethod
    def get_annual_out_website(json_list):
        lst = []
        for js_item in json_list:
            if js_item.get(u'webtype', u'') == u'1':
                web_type = '网站'
            else:
                web_type = '网店'
            web_model = {
                AnnualReports.WebSites.TYPE: web_type,  # 有坑
                AnnualReports.WebSites.SITE: js_item.get(u'domain'),
                AnnualReports.WebSites.NAME: js_item.get(u'websitname')
            }
            web_model = replace_none(web_model)
            lst.append(web_model)
        return lst

    # 股权变更信息
    @staticmethod
    def get_annual_edit_shareholding_change(json_list):
        lst = []
        for js_item in json_list:
            edit_model = {
                AnnualReports.EditShareholdingChangeInfos.SHAREHOLDER_NAME: js_item.get(u'inv'),
                AnnualReports.EditShareholdingChangeInfos.BEFORE_CONTENT: js_item.get(u'transamprpre'),
                AnnualReports.EditShareholdingChangeInfos.AFTER_CONTENT: js_item.get(u'transampraf'),
                AnnualReports.EditShareholdingChangeInfos.CHANGE_DATE: js_item.get(u'altdate')
            }
            edit_model = replace_none(edit_model)
            lst.append(edit_model)
        return lst

    # 年报 修改记录
    @staticmethod
    def get_annual_edit_change(json_list):
        lst = []
        for js_item in json_list:
            edit_model = {
                AnnualReports.EditChangeInfos.CHANGE_ITEM: js_item.get(u'altfield'),
                AnnualReports.EditChangeInfos.BEFORE_CONTENT: js_item.get(u'altbefore'),
                AnnualReports.EditChangeInfos.AFTER_CONTENT: js_item.get(u'altafter'),
                AnnualReports.EditChangeInfos.CHANGE_DATE: js_item.get(u'altdate')
            }
            edit_model = replace_none(edit_model)
            lst.append(edit_model)
        return lst

    # 出资人出资信息
    def get_annual_share_hold_info(self, json_list):
        lst = []
        for js_item in json_list:
            js_item = replace_none(js_item)
            if js_item.get('subconform', '') == '':
                sub_con_form = ''
            else:
                sub_con_form = self.switch_type(js_item.get('subconform', '').replace(',', ''))
            if js_item.get('acconform', '') == '':
                ac_con_form = ''
            else:
                ac_con_form = self.switch_type(js_item.get('acconform', '').replace(',', ''))
            share_model = {
                AnnualReports.ShareholderInformation.SHAREHOLDER_NAME: js_item.get(u'inv'),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                    js_item.get(u'lisubconam')),
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TIME: js_item.get(u'subcondate'),  # 认缴时间
                AnnualReports.ShareholderInformation.SUBSCRIPTION_TYPE: sub_con_form,  # 认缴类型 #有坑

                AnnualReports.ShareholderInformation.PAIED_AMOUNT: util.get_amount_with_unit(
                    js_item.get(u'liacconam')),  # 1实缴金额
                AnnualReports.ShareholderInformation.PAIED_TIME: js_item.get(u'accondate'),  # 实缴时间
                AnnualReports.ShareholderInformation.PAIED_TYPE: ac_con_form,  # 实缴类型 #有坑
            }
            share_model = replace_none(share_model)
            lst.append(share_model)
        return lst

    def get_result(self):
        return self.annual_info_dict


def replace_none(item_dict):
    for k, v in item_dict.items():
        if v is None:
            item_dict[k] = ''
    return item_dict


def is_num(text):
    try:
        text + 1
    except TypeError:
        return 0
    else:
        return text
