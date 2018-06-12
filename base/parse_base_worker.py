#!/usr/bin/env python
# encoding: utf-8
"""
"""
import json
import traceback

from base.task_base_worker import TaskBaseWorker
from bdp.i_crawler.i_extractor.ttypes import ExtractInfo, ExStatus, CrawlInfo, BaseInfo, PageParseInfo
from common import tools
from common import util
from common.annual_field import *
from common.global_field import *
from common.global_resource import is_debug, CHOOSE_DB_OLD, CHOOSE_DB_NEW
from common.gsxt_field import *
from common.tools import get_url_info


class ParseBaseWorker(TaskBaseWorker):
    province_py_to_zh = {
        'shanghai': '上海',
        'yunnan': '云南',
        'neimenggu': '内蒙古',
        'beijing': '北京',
        'jilin': '吉林',
        'sichuan': '四川',
        'tianjin': '天津',
        'ningxia': '宁夏',
        'anhui': '安徽',
        'shandong': '山东',
        'shanxicu': '山西',
        'guangdong': '广东',
        'guangxi': '广西',
        'xinjiang': '新疆',
        'jiangsu': '江苏',
        'jiangxi': '江西',
        'hebei': '河北',
        'henan': '河南',
        'zhejiang': '浙江',
        'hainan': '海南',
        'hubei': '湖北',
        'hunan': '湖南',
        'gansu': '甘肃',
        'fujian': '福建',
        'xizang': '西藏',
        'guizhou': '贵州',
        'liaoning': '辽宁',
        'chongqing': '重庆',
        'shanxi': '陕西',
        'qinghai': '青海',
        'heilongjiang': '黑龙江',
        'gsxt': '总局',
    }

    # 工商信息field最少需要的数目
    MIN_GS_FIELD_NUM = 15

    # 年报信息field最少需要的数目
    MIN_NB_FIELD_NUM = 7

    # 记录错误次数
    ERROR_TIMES = 'error_times'

    # 最大尝试次数
    MAX_ERROR_TIMES = 1

    # 反馈种子抓取情况状态设置
    # 全部都可以设置状态
    REPORT_ALL = 3
    # 只反馈状态到种子列表 主要针对列表页过期的省份
    REPORT_SEED = 1
    # 只反馈状态到列表
    REPORT_SEARCH = 2

    def __init__(self, **kwargs):
        TaskBaseWorker.__init__(self, **kwargs)
        self.min_gs_field_num = self.MIN_GS_FIELD_NUM
        self.min_nb_field_num = self.MIN_NB_FIELD_NUM
        self.online_all_search = 'online_all_search'
        self.offline_all_list = 'offline_all_list'
        self.report_status = self.REPORT_ALL

    # 判断实体对象是否抓取正常
    @staticmethod
    def get_crawl_page(field_item, multi=False, part=Model.type_list):
        '''
        :param field_item:
        :param multi:
        :param part:
        :return:TRUE 返回对象数组 调用者如果需要获取页面需要单独处理,FALSE 返回具体的页面信息
        '''
        if field_item is None:
            raise StandardError("info_model不能为None")

        page_list = field_item.get(part)
        # 如果是详情页则允许为None
        if part == Model.type_detail and page_list is None:
            return None

        if not isinstance(page_list, list) \
                or len(page_list) <= 0:
            raise PageCrawlError("未抓取到网页,或者抓取到无效或者失败的网页")

        if len(page_list) <= 1:
            for page in page_list:
                if page.get(u'status', u'fail') != u'success':
                    raise PageCrawlError("未抓取到网页,或者抓取到无效或者失败的网页")

        if not multi:
            return page_list[0].get(u'text')

        return page_list

    # 基本信息
    def get_base_info(self, base_info):
        '''
        :param base_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        基本信息一般存储在list列表中, 因为基本信息不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        '''
        base_info_dict = {}
        if base_info is None:
            return base_info_dict

        base_info_list = base_info.get(Model.type_list, None)
        if base_info_list is None:
            return base_info_dict

        return base_info_dict

    # 股东信息
    def get_shareholder_info(self, shareholder_info):
        '''
        :param shareholder_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        股东信息一般存储在list列表中, 因为股东信息不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        '''
        shareholder_info_dict = {}
        if shareholder_info is None:
            return shareholder_info_dict

        shareholder_info_list = shareholder_info.get(Model.type_list, None)
        if shareholder_info_list is None:
            return shareholder_info_dict

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
        change_info_list = change_info.get(Model.type_list, None)
        if change_info_list is None:
            return change_info_dict

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
        if key_person_info is None:
            return key_person_info_dict

        # list 网页列表
        key_person_info_list = key_person_info.get(Model.type_list, None)
        if key_person_info_list is None:
            return key_person_info_dict

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
        if branch_info is None:
            return branch_info_dict

        # list 网页列表
        branch_info_list = branch_info.get(Model.type_list, None)
        if branch_info_list is None:
            return branch_info_dict

        return branch_info_dict

    # 出资信息
    def get_contributive_info(self, contributive_info):
        '''
        :param contributive_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        出资信息一般会由两个列表分别进行存储, 但部分省份也可能只包含list列表, 没有详情页信息
        :return: 返回工商schema字典
        '''
        contributive_info_dict = {}
        if contributive_info is None:
            return contributive_info_dict

        # list 网页列表
        contributive_info_list = contributive_info.get(Model.type_list, None)
        if contributive_info_list is None:
            return contributive_info_dict

        return contributive_info_dict

    # 清算信息
    def get_liquidation_info(self, liquidation_info):
        '''
        :param liquidation_info: 网页库字典, 里面包含list 与 detail 两个列表, 列表中存储的为网页数据
        其中两个列表一定会存在一个, 否则则认为这个数据包无效, list一般储存列表翻页信息, detail存储列表项详情信息
        具体结构参考mongodb网页库或者查看 common/global_field.py 中Model定义注释
        清算信息一般存储在list列表中, 因为清算信息不包含列表结构不需要detail列表
        :return: 返回工商schema字典
        '''
        liquidation_info_dict = {}
        if liquidation_info is None:
            return liquidation_info_dict

        # list 网页列表
        liquidation_info_list = liquidation_info.get(Model.type_list, None)
        if liquidation_info_list is None:
            return liquidation_info_dict

        return liquidation_info_dict

    # 动产抵押登记信息
    def get_chattel_mortgage_info(self, chattel_mortgage_info):
        return {}

    # 列入经营异常名录信息
    def get_abnormal_operation_info(self, abnormal_operation_info):
        return {}

    # 股权出质登记信息 股权出资登记
    def get_equity_pledged_info(self, equity_pledged_info):
        return {}

    # 股权变更信息
    def get_change_shareholding_info(self, change_shareholding_info):
        parse_dict = {}
        return parse_dict

    # 年报信息
    def get_annual_info(self, annual_item_list):
        '''
        :param annual_item_list: 网页结构列表 [{url:xxx, text:xxx, status:xxx}]
        :return: 返回工商schema字典
        '''
        annual_info_dict = {}
        if not isinstance(annual_item_list, list):
            return annual_info_dict

        return annual_info_dict

    def get_entity_extractor_info(self, company, base_info_url, in_time, model, topic, year=None):

        # 去除none值
        store_model = util.del_none(model)

        base_url = base_info_url.encode('utf-8')
        replace_company = company.replace('(', '（').replace(')', '）')
        if year is None:
            record = '|' + replace_company
        else:
            record = '|' + replace_company + '|' + str(year)
        _site_record_id = tools.get_md5(record)

        if year is None:
            self.log.info('company = {company} record_id = {_set_record_id} url = {url}'.
                          format(company=company, _set_record_id=_site_record_id,
                                 url=base_url))
        else:
            self.log.info('company = {company} year = {year} record_id = {_set_record_id} url = {url}'.
                          format(company=company, _set_record_id=_site_record_id,
                                 url=base_url, year=year))

        store_model['_src'] = []
        store_model['_src'].append({'url': base_url, 'site': self.host, 'download_time': in_time})
        store_model['_site_record_id'] = _site_record_id

        extract_info = ExtractInfo()
        extract_info.ex_status = ExStatus.kEsSuccess
        extract_info.extract_data = json.dumps(store_model)
        extract_info.topic_id = topic

        crawl_info = CrawlInfo()
        crawl_info.content = ""
        crawl_info.download_time = in_time

        url_info = get_url_info(base_url)

        base_info = BaseInfo()
        base_info.site = url_info.get('site', '')
        base_info.url = url_info.get('url', '')
        base_info.site_id = url_info.get('site_id', 0)
        base_info.url_id = url_info.get('url_id', 0)

        return PageParseInfo(extract_info=extract_info, crawl_info=crawl_info, base_info=base_info)

    # 存储 同时发送到消息队列
    def __store_model(self, company, base_info_url, in_time, model):

        is_success = True

        if self.is_gs_mq_open:
            entity_extract_data = self.get_entity_extractor_info(company, base_info_url, in_time, model, self.gs_topic)
            is_success = self.merge_mq.push_sync_msg(entity_extract_data)
            self.log.info('发送工商到消息队列: province = {province} company = {company}'.format(
                company=company, province=self.province))
        else:  # 存储到数据库中
            try:
                self.target_db.find_and_modify(self.target_table,
                                               query={'company': company},
                                               update={'$set': model},
                                               upsert=True)
            except Exception as e:
                self.log.error('存储工商信息到数据库失败')
                self.log.exception(e)
                is_success = False

        return is_success

    # 存储年报信息
    def __store_annual_model(self, company, year, base_info_url, in_time, model):

        is_success = False
        if self.annual_table is None:
            return is_success

        # 判断是否需要发送到消息队列
        if self.is_nb_mq_open:
            entity_extract_data = self.get_entity_extractor_info(company, base_info_url, in_time, model,
                                                                 self.gs_nb_topic, year=year)
            is_success = self.merge_mq.push_sync_msg(entity_extract_data)
            self.log.info('发送年报到消息队列: province = {province} company = {company} year = {year}'.format(
                company=company, year=year, province=self.province))
        else:
            try:
                self.target_db.find_and_modify(self.annual_table,
                                               query={'company': company, 'year': year},
                                               update={'$set': model},
                                               upsert=True)
                is_success = True
            except Exception as e:
                self.log.error('存储工商年报到数据库失败')
                self.log.exception(e)
                is_success = False

        return is_success

    @staticmethod
    def __get_annual_year_dict(annual_info_detail):
        year_dict = {}
        for item in annual_info_detail:
            year = item.get('year', None)
            if year is None:
                continue
            if item.get('status', None) != 'success':
                continue
            if year in year_dict:
                year_dict[year].append(item)
            else:
                year_dict[year] = [item]

        return year_dict

    # 获得基本信息url
    @staticmethod
    def __get_base_info_url(base_info):
        base_info_list = base_info.get(Model.type_list)
        if base_info_list is None:
            return ''
        if not isinstance(base_info_list, list):
            return ''
        return base_info_list[0].get('url', '')

    # 解析年报
    def parse_nb_info(self, company, data_list, u_time,
                      _in_time, province, in_time,
                      base_info_url):
        # 年报信息
        annual_info = data_list.get(Model.annual_info)
        if annual_info is None:
            return True

        # try:
        annual_info_detail = annual_info.get(Model.type_detail)
        if annual_info_detail is None:
            self.log.error(
                '年报没有detail字段: province = {province} company = {company}'.format(
                    company=company, province=self.province))
            return False

        raise_msg_list = []
        annual_info_year_dict = self.__get_annual_year_dict(annual_info_detail)
        for year, item in annual_info_year_dict.iteritems():

            try:
                nb_model = dict(company=company,
                                province=province,
                                _utime=u_time,
                                _in_time=_in_time,
                                year=year)

                annual_info_dict = self.get_annual_info(item)
                if not isinstance(annual_info_dict, dict):
                    self.log.error(
                        '年报返回数据类型错误: province = {province} company = {company} year = {year}'.format(
                            company=company, year=year, province=self.province))
                    continue

                if len(annual_info_dict) < self.min_nb_field_num:
                    raise_msg_list.append(
                        'province = {province} company = {company} year = {year} field len = {length} '
                        '年报字段过少'.format(company=company, length=len(annual_info_dict),
                                        year=year, province=self.province))
                    continue

                nb_model.update(annual_info_dict)

                # company 保存为详情页的名称  company_name 保存为年报名称
                nb_model['company'] = company

                # 年报需要单独存储
                if not self.__store_annual_model(company, year, base_info_url, in_time, nb_model):
                    return False
            except FieldMissError as e:
                self.log.error('年报字段缺失错误: province = {province} company = {company} year = {year}'.format(
                    company=company, year=year, province=self.province))
                raise e
            except Exception as e:
                self.log.error('年报解析错误: province = {province} company = {company} year = {year}'.format(
                    company=company, year=year, province=self.province))
                self.log.exception(e)
                return False

        # 打印异常列表信息
        if len(raise_msg_list) > 0:
            for msg in raise_msg_list:
                try:
                    raise StandardError(msg)
                except Exception as e:
                    self.log.exception(e)

            return False

        return True

    # 反馈抓取失败的情况
    def report_crawl_fail(self, item):
        _id = item.get('_id')
        search_name = item.get('search_name')
        if search_name is None:
            search_name = _id
            self.log.info('search_name is None: _id = {_id}'.format(_id=_id))

        # 判断是否由权限反馈到搜索列表
        if (self.report_status & self.REPORT_SEARCH) > 0:
            result_item = self.company_data_db.find_one(self.online_all_search,
                                                        {'search_name': search_name, 'province': self.province})
            if result_item is not None:
                result_item[self.crawl_flag] = 0
                self.company_data_db.save(self.online_all_search, result_item)
                self.log.info('save online_all_search success {com}'.format(com=search_name))
                return

        # 判断是否由权限反馈到种子列表
        if (self.report_status & self.REPORT_SEED) > 0:
            result_item = self.company_data_db.find_one(self.offline_all_list,
                                                        {'company_name': _id, 'province': self.province})
            if result_item is not None:
                result_item[self.crawl_flag] = 0
                self.company_data_db.save(self.offline_all_list, result_item)
                self.log.info('save offline_all_list success {com}'.format(com=_id))
                return

            data = {
                '_id': util.generator_id({}, _id, self.province),
                'company_name': _id,
                'province': self.province,
                'in_time': util.get_now_time(),
                self.crawl_flag: 0,
            }
            self.company_data_db.insert_batch_data(self.offline_all_list, [data])
            self.log.info('insert new company = {company}'.format(company=_id))

    # 解析成员
    def __parse_model(self, company, data_list, item):
        model = {}
        if data_list is None:
            return model

        for key, value in vars(Model).items():
            if 'info' not in key or 'info' not in value:
                continue

            # 年报不进行调用
            if Model.annual_info in key or Model.annual_info in value:
                continue

            field_info = data_list.get(value)
            if field_info is not None:
                try:
                    field_info_dict = eval('self.get_' + value)(field_info)
                except FieldMissError as e:
                    # 如果是字段缺失异常 则抛出异常
                    self.log.error('工商字段缺失错误: province = {province} company = {company}'.format(
                        company=company, province=self.province))
                    raise e
                except PageCrawlError as e:
                    field_info_dict = {}
                    self.log.exception(e)
                except:
                    self.log.error('province:{province},company:{company},error-part:{value},error-info:{error}'.format(
                        company=company, error=traceback.format_exc(), province=self.province, value=value))
                    field_info_dict = {}

                model.update(field_info_dict)

        # todo 暂先去掉 反馈太多 会导致抓取压力很大
        # # 如果有抓取失败的
        # if is_crawl_fail:
        #     # 反馈抓取失败, 重新抓取
        #     self.report_crawl_fail(item)

        return model

    # 处理统一社会信用号
    def process_register_code(self, company, model):

        if GsModel.CODE not in model:
            model[GsModel.CODE] = ""
        if GsModel.REGISTERED_CODE not in model:
            model[GsModel.REGISTERED_CODE] = ""
        if GsModel.UNIFIED_SOCIAL_CREDIT_CODE not in model:
            model[GsModel.UNIFIED_SOCIAL_CREDIT_CODE] = ""

        # 去空格处理
        model[GsModel.CODE] = model[GsModel.CODE].strip()
        model[GsModel.REGISTERED_CODE] = model[GsModel.REGISTERED_CODE].strip()
        model[GsModel.UNIFIED_SOCIAL_CREDIT_CODE] = model[GsModel.UNIFIED_SOCIAL_CREDIT_CODE].strip()

        if model[GsModel.CODE] == "" and \
                        model[GsModel.REGISTERED_CODE] == "" and \
                        model[GsModel.UNIFIED_SOCIAL_CREDIT_CODE] == "":
            self.log.error("当前企业没有统一社会信用号或者注册号: {province} {company}".format(
                company=company,
                province=self.province
            ))
            return

        # 有统一社会信用号
        if model[GsModel.UNIFIED_SOCIAL_CREDIT_CODE] != "" and \
                        model[GsModel.CODE] == "" and \
                        model[GsModel.REGISTERED_CODE] == "":
            if len(model[GsModel.UNIFIED_SOCIAL_CREDIT_CODE]) != 18:
                self.log.error("统一社会信用号不为18位: {province} {company}".format(
                    company=company, province=self.province))
            return

        # 有注册号
        if model[GsModel.UNIFIED_SOCIAL_CREDIT_CODE] == "" and \
                        model[GsModel.CODE] == "" and \
                        model[GsModel.REGISTERED_CODE] != "":
            return

        # 如果有code 但是没有统一社会信用号 和注册号
        if model[GsModel.CODE] != "" and \
                        model[GsModel.UNIFIED_SOCIAL_CREDIT_CODE] == "" and \
                        model[GsModel.REGISTERED_CODE] == "":
            if len(model[GsModel.CODE]) == 18:
                model[GsModel.UNIFIED_SOCIAL_CREDIT_CODE] = model[GsModel.CODE]
            else:
                model[GsModel.REGISTERED_CODE] = model[GsModel.CODE]
            return

        if model[GsModel.CODE] != "" and \
                        model[GsModel.UNIFIED_SOCIAL_CREDIT_CODE] != "" and \
                        model[GsModel.REGISTERED_CODE] != "":
            if len(model[GsModel.UNIFIED_SOCIAL_CREDIT_CODE]) != 18:
                model[GsModel.UNIFIED_SOCIAL_CREDIT_CODE] = ""
                self.log.error("统一社会信用号不为18位: {province} {company}".format(
                    company=company, province=self.province))
            return

        if model[GsModel.UNIFIED_SOCIAL_CREDIT_CODE] != "" and \
                        model[GsModel.REGISTERED_CODE] != "":
            if len(model[GsModel.UNIFIED_SOCIAL_CREDIT_CODE]) != 18:
                model[GsModel.UNIFIED_SOCIAL_CREDIT_CODE] = ""
                self.log.error("统一社会信用号不为18位: {province} {company}".format(
                    company=company, province=self.province))
            return

        self.log.error("有多个注册号信息: {province} {company} {code} {re_code} {un_code}".format(
            province=self.province,
            company=company,
            code=model[GsModel.CODE],
            re_code=model[GsModel.REGISTERED_CODE],
            un_code=model[GsModel.UNIFIED_SOCIAL_CREDIT_CODE]))

    # 解析工商信息
    def parse_gs_info(self, company, data_list, u_time,
                      _in_time, province, in_time,
                      base_info_url, item):

        model = dict(company=company,
                     _utime=u_time,
                     _in_time=_in_time,
                     province=province)

        # 开始解析
        model.update(self.__parse_model(company, data_list, item))

        # 判断解析属性个数是否符合要求
        if len(model) < self.min_gs_field_num:
            raise StandardError('province = {province} company = {company} field len = {length} 工商字段过少'.format(
                company=company, length=len(model), province=self.province))

        # 统一社会信用号处理
        self.process_register_code(company, model)

        # 存储解析信息
        return self.__store_model(company, base_info_url, in_time, model)

    # 反馈验证码拦截页面 重新抓取
    def filter_captcha_page(self, item):

        filter_str = json.dumps(item)

        # 如果发现验证码拦截特征值 则进行反馈抓取
        if util.judge_feature(filter_str):
            self.report_crawl_fail(item)

    # 开始解析
    def query_company(self, item):

        # 先判断抓取页面信息中是否有无效拦截页面
        self.filter_captcha_page(item)

        # 获得企业名称
        company = item.get('_id')
        u_time = _in_time = util.get_now_time()
        province = self.province_py_to_zh[self.province]

        in_time = item.get('in_time')
        if in_time is None:
            in_time = _in_time

        # 获得抓取时间戳
        in_time = util.get_change_stamp(in_time)

        data_list = item.get('datalist')
        if data_list is None:
            self.log.info('没有datalist: province = {province} company = {company}'.format(
                company=company, province=self.province))
            return self.CRAWL_NOTHING_FIND

        base_info = data_list.get(Model.base_info)
        if base_info is None:
            self.log.error('没有基本信息: province = {province} company = {company}'.format(
                company=company, province=self.province))
            return self.CRAWL_UN_FINISH

        base_info_url = self.__get_base_info_url(base_info)

        # 解析工商
        gs_flag = self.parse_gs_info(company, data_list, u_time,
                                     _in_time, province, in_time,
                                     base_info_url, item)

        # 解析年报
        nb_flag = self.parse_nb_info(company, data_list, u_time,
                                     _in_time, province, in_time,
                                     base_info_url)

        if gs_flag and nb_flag:
            return self.success_flag

        if not gs_flag and not nb_flag:
            self.log.error('工商信息与年报都解析失败: province = {province} company = {company}'.format(
                company=company, province=self.province))
            return self.CRAWL_UN_FINISH

        if not gs_flag:
            self.log.error('工商信息解析失败: province = {province} company = {company}'.format(
                company=company, province=self.province))
            return self.CRAWL_UN_FINISH

        if not nb_flag:
            self.log.error('年报解析失败: province = {province} company = {company}'.format(
                company=company, province=self.province))
            return self.CRAWL_UN_FINISH

        return self.success_flag

    @staticmethod
    def _get_base_item_info(items):
        base_info_dict = {}
        for item in items:
            item_content = item.text().replace(u'·', u'').strip()
            if len(item_content) == 0:
                continue
            part = item_content.split(u'：', 1)
            k = GsModel.format_base_model(part[0].strip())
            base_info_dict[k] = part[1].strip()
            if k == GsModel.LEGAL_MAN:
                base_info_dict[GsModel.LEGAL_MAN_TYPE] = part[0].strip()

        base_info_dict[GsModel.PERIOD] = u"{0}至{1}".format(
            base_info_dict.get(GsModel.PERIOD_FROM, u''), base_info_dict.get(GsModel.PERIOD_TO, u''))
        return base_info_dict

    # 年报股东信息
    def _get_annual_sharehold_info(self, py_items):
        lst = []
        for item in py_items:
            tds = item.find('td')
            if len(tds) < 8:
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

    # 变更信息tds的获取
    def _get_change_info_td_text(self, tds, start_index=1):
        if tds is None:
            return {}
        length = len(tds)
        # todo 这个是一个暂时的方案 ,北京-中信银行特殊结构
        is_special = False
        sepcial_field = u'详细'
        if sepcial_field in tds.text():
            is_special = True
            if length <= start_index + 2:
                return {}
        else:
            if length <= start_index + 3:
                return {}

        change_model = {
            GsModel.ChangeRecords.CHANGE_ITEM: tds.eq(start_index).text(),
            # 去除多余的字
            GsModel.ChangeRecords.BEFORE_CONTENT: util.format_content(
                tds.eq(start_index + 1).text()) if not is_special else sepcial_field,
            GsModel.ChangeRecords.AFTER_CONTENT: util.format_content(
                tds.eq(start_index + 2).text()) if not is_special else sepcial_field,
            # 日期格式化
            GsModel.ChangeRecords.CHANGE_DATE: tds.eq(start_index + 3).text()
            if not is_special else tds.eq(start_index + 2).text()
        }
        return change_model

    # 出资信息工具类
    def _get_sharehold_info_list_td_text(self, tds, start_index=1):
        if tds is None:
            return {}
        if len(tds) <= start_index + 3:
            return {}
        sub_model = {
            GsModel.ContributorInformation.SHAREHOLDER_NAME: tds.eq(start_index).text(),
            GsModel.ContributorInformation.SHAREHOLDER_TYPE: tds.eq(start_index + 1).text(),
            GsModel.ContributorInformation.CERTIFICATE_TYPE: tds.eq(start_index + 2).text(),
            GsModel.ContributorInformation.CERTIFICATE_NO: tds.eq(start_index + 3).text()
        }
        return sub_model

    # 股东信息 明细解析
    def get_sharehold_info_sub_detail(self, trs, amount_unit=u"元", is_subs_or_paied='subs', start_index=0):
        name = GsModel.ShareholderInformation.SUBSCRIPTION_DETAIL if is_subs_or_paied == 'subs' else GsModel.ShareholderInformation.PAIED_DETAIL
        if trs is None:
            return {name: []}
        lst = []
        for tr in trs:
            tds = tr.find('td')
            if len(tds) <= start_index + 3:
                continue
            if is_subs_or_paied == 'subs':
                sub_model = {
                    GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TYPE: tds.eq(start_index).text(),
                    GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_AMOUNT: util.get_amount_with_unit(
                        tds.eq(start_index + 1).text(), amount_unit),
                    GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_TIME: tds.eq(start_index + 2).text(),
                    GsModel.ShareholderInformation.SubscriptionDetail.SUBSCRIPTION_PUBLISH_TIME: tds.eq(
                        start_index + 3).text(),
                }
            else:
                sub_model = {
                    GsModel.ShareholderInformation.PaiedDetail.PAIED_TYPE: tds.eq(start_index).text(),
                    GsModel.ShareholderInformation.PaiedDetail.PAIED_AMOUNT: tds.eq(
                        start_index + 1).text() + amount_unit,
                    GsModel.ShareholderInformation.PaiedDetail.PAIED_TIME: tds.eq(start_index + 2).text(),
                    GsModel.ShareholderInformation.PaiedDetail.PAIED_PUBLISH_TIME: tds.eq(start_index + 3).text(),
                }
            lst.append(sub_model)
        return {name: lst}  # if len(lst) != 0 else {name:[]}

    # 获取出资 信息详细列表
    def _get_sharehold_detail(self, tables, name_dom="th"):
        shareholder_name = ""
        sub_model = {}
        if tables is None:
            return shareholder_name, sub_model

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
                sub_model[GsModel.ContributorInformation.SUBSCRIPTION_AMOUNT] = util.get_amount_with_unit(
                    tds.eq(steps[1]).text())
                sub_model[GsModel.ContributorInformation.PAIED_AMOUNT] = util.get_amount_with_unit(
                    tds.eq(steps[2]).text())
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

        # 设置原始表中关键字搜索状态
        #  -2 公司名称太短, -1 公司名称不符合规格, 2 剔除搜索过没有信息的关键字 1 代表已经抓完了

    def set_crawl_flag(self, item, crawl_flag, flag, choose_db_type):

        if is_debug:
            return

        _id = item.get('_id')

        # 获得当前时间
        cur_time = util.get_now_time()
        time_key = crawl_flag + '_time'

        # 如果错误 则记录错误次数
        if flag == self.success_flag:
            error_times = 0
        else:
            if self.ERROR_TIMES not in item:
                error_times = 1
            else:
                error_times = item[self.ERROR_TIMES] + 1

        # 打上从未成功过标签
        if error_times >= self.MAX_ERROR_TIMES:
            flag = self.never_success_flag

        # 更新信息
        if choose_db_type == CHOOSE_DB_OLD:
            # self.webpage_db_old.save(self.source_table, item)
            self.webpage_db_old.update(self.source_table, {'_id': _id}, {"$set": {
                time_key: cur_time,
                crawl_flag: flag,
                self.ERROR_TIMES: error_times,
            }})
        elif choose_db_type == CHOOSE_DB_NEW:
            # self.webpage_db_new.save(self.source_table, item)
            self.webpage_db_new.update(self.source_table, {'_id': _id}, {"$set": {
                time_key: cur_time,
                crawl_flag: flag,
                self.ERROR_TIMES: error_times,
            }})

    # 判断是否需要抓取 True 需要抓取 False 不需要抓取
    def check_crawl_flag(self, item):

        # 如果是调试模式则直接返回,不校验状态
        if is_debug:
            return True

        if self.ERROR_TIMES not in item:
            return True

        if item[self.ERROR_TIMES] >= self.MAX_ERROR_TIMES:
            return False

        return True

    def query_offline_task(self, item, choose_db_type):

        if not isinstance(item, dict):
            self.log.info('参数错误: item = {item}'.format(item=item))
            return self.FAIL

        company = item.get('_id', None)
        if company is None:
            self.log.error('没有company_name字段: item = {item}'.format(item=item))
            return self.FAIL

        # 判断是否需要进行抓取
        # if not self.check_crawl_flag(item):
        #     self.log.info('当前状态不进行抓取: province = {province} company = {company}'.format(
        #         province=self.province, company=company))
        #     return self.SUCCESS

        self.log.info('开始解析任务...province = {province} company = {company}'.format(
            province=self.province, company=company))
        try:
            status = self.query_company(item)
        except Exception as e:
            self.log.error('解析异常...')
            self.log.exception(e)
            status = self.CRAWL_UN_FINISH

        try:
            # 设置解析状态标记
            self.set_crawl_flag(item, self.crawl_flag, status, choose_db_type)
        except Exception as e:
            self.log.error('数据解析状态存储异常: company = {company}'.format(company=company))
            self.log.exception(e)

        self.log.info('完成解析任务...province = {province} company = {company} status = {status}'.format(
            province=self.province, company=company, status=status))
        return self.SUCCESS

    def query_online_task(self, company):

        if company is None or company.strip() == '':
            self.log.info('传入参数错误..')
            return self.FAIL

        try:
            choose_db_type = CHOOSE_DB_OLD
            item = self.webpage_db_old.find_one(self.source_table, {'_id': company})
            if item is None:
                item = self.webpage_db_new.find_one(self.source_table, {'_id': company})
                choose_db_type = CHOOSE_DB_NEW

            if item is None:
                self.log.error('没有搜索到需要解析的公司网页库信息: company = {company}'.format(company=company))
                return self.FAIL

        except Exception as e:
            self.log.error('数据查找异常: company = {company}'.format(company=company))
            self.log.exception(e)
            return self.FAIL

        self.log.info('开始解析任务...province = {province} company = {company}'.format(
            province=self.province, company=company))
        try:
            status = self.query_company(item)
        except Exception as e:
            self.log.error('解析异常...')
            self.log.exception(e)
            status = self.CRAWL_UN_FINISH

        # 如果是解析错误的, 要记录下来..
        if status == self.CRAWL_UN_FINISH:
            try:
                # 设置解析状态标记
                self.set_crawl_flag(item, self.crawl_flag, status, choose_db_type)
            except Exception as e:
                self.log.error('数据解析状态存储异常: company = {company}'.format(company=company))
                self.log.exception(e)

        self.log.info('完成解析任务...province = {province} company = {company} status = {status}'.format(
            province=self.province, company=company, status=status))
        return self.SUCCESS
