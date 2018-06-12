#!/usr/bin/env python
# -*- coding:utf-8 -*-
import re

from pyquery import PyQuery

from base.gsxt_base_worker import GsxtBaseWorker
from common import util
from common.global_field import Model

'''
吉林无法识别

2. 包含出资信息 出资信息包含详情页
3. 包含年报信息
4. 添加完成统计信息
5. 完成拓扑信息添加 且已经check
6. 完成列表页名称提取
'''


class GsxtJiLinWorker(GsxtBaseWorker):
    def __init__(self, **kwargs):
        GsxtBaseWorker.__init__(self, **kwargs)

    def get_search_list_html(self, keyword, session):
        param_list = []
        try:
            # keyword = '华煤集团有限公司'
            url = 'http://{host}/'.format(host=self.host)
            content = self.get_captcha_geetest(url, '#txtSearch', '#btnSearch', keyword, '.m-searchresult-inoformation')
            if content is None:
                return param_list, self.SEARCH_ERROR

            jq = PyQuery(content, parser='html')
            if jq.find('span.m-c-red').text() == '0':
                return param_list, self.SEARCH_NOTHING_FIND

            pattern = '.*?\?id=(.*?)&entTypeCode=(.*?)$'
            regex = re.compile(pattern)
            item_list = jq.find('.m-search-list').find('.m-searchresult-box').items()
            for item in item_list:
                a_item = item.find('a')
                href = a_item.attr('href')
                company = a_item.text()
                if href is None or href == '':
                    continue
                if company is None or company == '':
                    continue
                search_name = company.replace(' ', '')
                if search_name == '':
                    continue
                search_list = regex.findall(href)
                if len(search_list) <= 0:
                    continue

                param = {
                    'id': search_list[0][0],
                    'entTypeCode': search_list[0][1],
                    'search_name': search_name,
                }
                seed_code = item.find('.m-pull-left.m-shdm').find('span[data-bind="text: $data.uniSCID"]').text()
                bak_seed_code = item.find('.m-pull-left.m-shdm').find('span[data-bind="text: $data.regNO"]').text()
                if seed_code is not None and seed_code.strip() != '':
                    if len(seed_code.strip()) > 10:
                        param['unified_social_credit_code'] = seed_code
                    else:
                        param['unified_social_credit_code'] = bak_seed_code

                param_list.append(param)
        except Exception as e:
            self.log.exception(e)
            return param_list, self.SEARCH_ERROR

        return param_list, self.SEARCH_SUCCESS if len(param_list) > 0 else self.SEARCH_ERROR

    def get_base_info(self, session, i_d):
        url = 'http://{host}/api/PubBaseInfo/Business/{id}?_={rand}'.format(
            host=self.host, id=i_d, rand=util.get_time_stamp())
        r = self.task_request(session, session.get, url)
        if r is None:
            return
        return url, r.text

    # 获取公司名称
    @staticmethod
    def __get_company_name(text):
        json_data = util.json_loads(text)
        if json_data is None:
            return None

        ent_name = json_data.get('entName', None)
        if ent_name is None:
            return None

        return ent_name.strip()

    # 主要人员
    def get_key_person_info(self, session, i_d, data):
        url = 'http://{host}/api/PubBaseInfo/PriPersons/{id}?_={rand}'.format(
            host=self.host, id=i_d, rand=util.get_time_stamp())
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.key_person_info, url, '', status=self.STATUS_FAIL)
            return

        # 存储数据
        self.append_model(data, Model.key_person_info, url, r.text)

    # 变更信息
    def get_change_info(self, session, i_d, data):
        url = 'http://{host}/api/PubBaseInfo/BaseInfoAlters/{id}?_={rand}'.format(
            host=self.host, id=i_d, rand=util.get_time_stamp())
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.change_info, url, '', status=self.STATUS_FAIL)
            return

        # 存储数据
        self.append_model(data, Model.change_info, url, r.text)

    # 分支机构
    def get_branch_info(self, session, i_d, data):
        url = 'http://{host}/api/PubBaseInfo/Branchs/{id}?_={rand}'.format(
            host=self.host, id=i_d, rand=util.get_time_stamp())
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.branch_info, url, '', status=self.STATUS_FAIL)
            return

        # 存储数据
        self.append_model(data, Model.branch_info, url, r.text)

    # 股东信息
    def get_shareholder_info(self, session, i_d, data):
        url = 'http://{host}/api/PubSelfPubInfo/InvDetails/{id}?_={rand}'.format(
            host=self.host, id=i_d, rand=util.get_time_stamp())
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.shareholder_info, url, '', status=self.STATUS_FAIL)
            return

        # 存储数据
        self.append_model(data, Model.shareholder_info, url, r.text)

    # 获得出资信息
    def get_contributive_info(self, session, i_d, data):
        url = 'http://{host}/api/PubBaseInfo/Invs/{id}?_={rand}'.format(
            host=self.host, id=i_d, rand=util.get_time_stamp())
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.contributive_info, url, '', status=self.STATUS_FAIL)
            return

        # 存储数据
        self.append_model(data, Model.contributive_info, url, r.text)

        json_data = util.json_loads(r.text)
        if json_data is None:
            self.append_model(data, Model.contributive_info, url, r.text, status=self.STATUS_FAIL)
            return

        data_list = json_data.get('data', None)
        if data_list is None:
            return

        for index, item in enumerate(data_list):
            inv_id = item.get('invId', None)
            if inv_id is None:
                continue
            url = 'http://{host}/api/PubBaseInfo/InvDetail/{id}/{invid}'.format(
                host=self.host, id=i_d, invid=inv_id)
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.contributive_info, url, '',
                                  status=self.STATUS_FAIL,
                                  classify=Model.type_detail)
                continue

            self.append_model(data, Model.contributive_info, url, r.text,
                              classify=Model.type_detail)

    def __get_annual_detail_info(self, session, url, year, data):
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.annual_info, url, '',
                              status=self.STATUS_FAIL,
                              classify=Model.type_detail, year=year)
            return

        self.append_model(data, Model.annual_info, url, r.text,
                          classify=Model.type_detail, year=year)

    # 获得年报详细信息
    def get_annual_detail_info(self, session, i_d, anche_id, year, data):
        url = 'http://{host}/api/PubAnnualInfo/Annual/{id}/{anche_id}'.format(
            host=self.host, id=i_d, anche_id=anche_id)
        self.__get_annual_detail_info(session, url, year, data)

        url = 'http://{host}/api/PubAnnualInfo/AnWebSites/{id}/{anche_id}'.format(
            host=self.host, id=i_d, anche_id=anche_id)
        self.__get_annual_detail_info(session, url, year, data)

        url = 'http://{host}/api/PubAnnualInfo/AnForInvestments/{id}/{anche_id}'.format(
            host=self.host, id=i_d, anche_id=anche_id)
        self.__get_annual_detail_info(session, url, year, data)

        url = 'http://{host}/api/PubAnnualInfo/AnAsset/{id}/{anche_id}'.format(
            host=self.host, id=i_d, anche_id=anche_id)
        self.__get_annual_detail_info(session, url, year, data)

        url = 'http://{host}/api/PubAnnualInfo/AnUpdates/{id}/{anche_id}'.format(
            host=self.host, id=i_d, anche_id=anche_id)
        self.__get_annual_detail_info(session, url, year, data)

        url = 'http://{host}/api/PubAnnualInfo/AnSubCapitals/{id}/{anche_id}?_={rand}'.format(
            host=self.host, id=i_d, anche_id=anche_id, rand=util.get_time_stamp())
        self.__get_annual_detail_info(session, url, year, data)

        url = 'http://{host}/api/PubAnnualInfo/AnForGuarantees/{id}/{anche_id}?_={rand}'.format(
            host=self.host, id=i_d, anche_id=anche_id, rand=util.get_time_stamp())
        self.__get_annual_detail_info(session, url, year, data)

        url = 'http://{host}/api/PubAnnualInfo/AnAlterStocks/{id}/{anche_id}?_={rand}'.format(
            host=self.host, id=i_d, anche_id=anche_id, rand=util.get_time_stamp())
        self.__get_annual_detail_info(session, url, year, data)

    # 获取年报信息
    def get_annual_info(self, session, i_d, data):
        url = 'http://{host}/api/PubAnnualInfo/Annuals/{id}?_={rand}'.format(
            host=self.host, id=i_d, rand=util.get_time_stamp())
        r = self.task_request(session, session.get, url)
        if r is None:
            return

        json_data = util.json_loads(r.text)
        if json_data is None:
            return

        data_list = json_data.get('data', None)
        if data_list is None:
            return

        for item in data_list:
            anche_id = item.get('ancheId', None)
            year_info = item.get('year', None)
            if anche_id is None or year_info is None:
                continue

            year_list = re.findall('(\d+)', year_info)
            if len(year_list) <= 0:
                continue
            year = year_list[0]

            # 获得详细年报信息
            self.get_annual_detail_info(session, i_d, anche_id, year, data)

    def get_detail_html_list(self, seed, session, param_list):
        data_list = []
        for item in param_list:
            try:
                i_d = item.get('id', None)
                if i_d is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                search_name = item.get('search_name', None)
                if search_name is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                url, text = self.get_base_info(session, i_d)
                if url is None or text is None:
                    continue

                # 获得公司名称
                company = self.__get_company_name(text)
                if company is None or company == '':
                    self.log.error('公司名称解析失败..item = {item} {text}'.format(
                        text=text, item=item))
                    continue

                # 建立数据模型
                data = self.get_model(company, seed, search_name, self.province)

                # 存储数据
                self.append_model(data, Model.base_info, url, text)

                # 主要人员
                self.get_key_person_info(session, i_d, data)

                # 变更信息
                self.get_change_info(session, i_d, data)

                # 获得分支机构
                self.get_branch_info(session, i_d, data)

                # 获得股东信息
                self.get_shareholder_info(session, i_d, data)

                # 获得出资信息
                self.get_contributive_info(session, i_d, data)

                # # 获得年报信息
                self.get_annual_info(session, i_d, data)

                data_list.append(data)
            except Exception as e:
                self.log.exception(e)
        return self.sent_to_target(data_list)
