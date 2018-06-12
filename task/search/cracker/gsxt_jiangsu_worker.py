#!/usr/bin/env python
# -*- coding:utf-8 -*-
import random
import re
import time

from pyquery import PyQuery

from base.gsxt_base_worker import GsxtBaseWorker
from common import util
from common.global_field import Model
from config.conf import captcha_geetest_conf

'''
1. 搜索没有结果判断功能添加
2. 已抓出资信息
3. 没有抓年报信息
4. 添加统计信息
5. 完成列表页名称提取
'''


class GsxtJiangSuWorker(GsxtBaseWorker):
    def __init__(self, **kwargs):
        GsxtBaseWorker.__init__(self, **kwargs)
        self.proxy_type = self.PROXY_TYPE_DYNAMIC
        # 滑动验证码配置信息
        self.captcha_geetest_conf = captcha_geetest_conf

    # 需要存入无搜索结果
    def get_search_list_html(self, keyword, session):
        param_list = []
        try:
            # 获得代理信息
            url = 'http://{host}/province'.format(host=self.host)
            start_time = time.time()
            json_data, content = self.get_captcha_geetest_full(url, 'input#name', '#popup-submit', keyword,
                                                               'div.listbox', success='div.main.search-result')

            end_time = time.time()
            self.log.info('滑动验证码耗时: {t}s'.format(t=end_time - start_time))
            if json_data is None:
                self.log.error('json_data is None keyword = {keyword}'.format(keyword=keyword))
                return param_list, self.SEARCH_ERROR

            status = json_data.get('status', 1)
            if status == -100:
                self.log.warn('IP 被封, 无法访问 status = -100')
                self.report_session_proxy(session)
                return param_list, self.SEARCH_ERROR

            if status == 100 and content is not None and content.find('无相关数据') != -1:
                return param_list, self.SEARCH_NOTHING_FIND

            if status != 0:
                return param_list, self.SEARCH_ERROR

            if content is None:
                return param_list, self.SEARCH_ERROR

            if content.find('该IP在一天内超过了查询的限定次数') != -1:
                self.log.warn('IP 被封, 无法访问')
                self.report_session_proxy(session)
                return param_list, self.SEARCH_ERROR

            if content.find('无相关数据') != -1:
                return param_list, self.SEARCH_NOTHING_FIND

            jq = PyQuery(content, parser='html')
            item_list = jq.find('div.main.search-result').find('#index').find('.listbox').items()
            for item in item_list:
                a_item = item.find('a')
                href = a_item.attr('href')
                if href is None or href == '':
                    continue
                company = a_item.find('.entnameclass').text()
                if company is None or company == '':
                    continue

                search_name = company.replace(' ', '')
                if search_name == '':
                    continue

                param = {
                    'href': href,
                    'search_name': search_name,
                }

                seed_code = item.find('.zhucehao').find('td').eq(0).find('span').text()
                if seed_code is not None and seed_code.strip() != '':
                    param['unified_social_credit_code'] = seed_code

                param_list.append(param)
        except Exception as e:
            self.log.exception(e)
            return param_list, self.SEARCH_ERROR

        return param_list, self.SEARCH_SUCCESS if len(param_list) > 0 else self.SEARCH_ERROR

    def __get_company_name(self, text):
        try:
            json_data = util.json_loads(text)
            if json_data is None:
                return None

            return json_data.get('CORP_NAME', None)
        except Exception as e:
            self.log.exception(e)
        return None

    def get_year_info_list(self, text):
        jq = PyQuery(text, parser='html')
        item_list = jq.find("#yearreportTable").find('tr').items()
        for item in item_list:
            try:
                if item.text().find('年度报告') == -1:
                    continue
                year_info = item.find('td').eq(1)
                if year_info is None or year_info == '':
                    continue

                year_list = re.findall('(\d+)', year_info.text())
                year = str(year_list[0]) if len(year_list) > 0 else None
                if year is None:
                    continue

                href = item.find('a').attr('href')
                if href is None or href == '':
                    continue

                yield year, 'http://{host}{href}'.format(host=self.host, href=href)
            except Exception as e:
                self.log.exception(e)

    # 基本信息
    def get_base_info(self, session, param_dict):
        # 获取基础信息
        org = param_dict['org']
        i_d = param_dict['id']
        seq_id = param_dict['seqId']
        url = 'http://{host}/ecipplatform/publicInfoQueryServlet.json?pageView=true&org={org}&id={id}&seqId={seqId}&abnormal=&activeTabId=&tmp={rand}'.format(
            org=org, id=i_d, seqId=seq_id, host=self.host, rand=random.randint(1, 99))
        base_info = self.task_request(session, session.get, url)
        if base_info is None:
            self.log.error('获取基础信息页面失败...')
            return None, None

        return url, base_info.text

    # 出资信息
    def get_contributive_info(self, session, param_dict, data):
        org = param_dict['org']
        i_d = param_dict['id']
        seq_id = param_dict['seqId']
        try:
            url = 'http://{host}/ecipplatform/publicInfoQueryServlet.json?queryGdcz=true&abnormal=&activeTabId=&admitMain=08&curPage=1&id={id}&org={org}&pageSize=200&seqId={seqId}&sortName=&sortOrder=&tmp=35'.format(
                id=i_d, seqId=seq_id, org=org, host=self.host)
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.contributive_info, url, '', status=self.STATUS_FAIL)
                return

            self.append_model(data, Model.contributive_info, url, r.text)
        except Exception as e:
            self.log.exception(e)

    # 主要人员
    def get_key_person_info(self, session, param_dict, data):
        org = param_dict['org']
        i_d = param_dict['id']
        seq_id = param_dict['seqId']
        reg_no = param_dict['regNo']
        uni_scid = param_dict['uniScid']
        url = 'http://{host}/ecipplatform/publicInfoQueryServlet.json?queryZyry=true&org={org}&id={id}&seqId={seqId}&abnormal=&activeTabId=&tmp=75&regNo={regNo}&admitMain=08&uniScid={uniScid}'.format(
            id=i_d, seqId=seq_id, org=org, host=self.host, regNo=reg_no, uniScid=uni_scid)
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.key_person_info, url, '', status=self.STATUS_FAIL)
            return
        self.append_model(data, Model.key_person_info, url, r.text)

    # 变更信息
    def get_change_info(self, session, param_dict, data):
        org = param_dict['org']
        i_d = param_dict['id']
        seq_id = param_dict['seqId']
        reg_no = param_dict['regNo']
        uni_scid = param_dict['uniScid']

        post_data = {
            'org': org,
            'id': i_d,
            'seqId': seq_id,
            'abnormal': '',
            'activeTabId': '',
            'tmp': 75,
            'regNo': reg_no,
            'admitMain': '08',
            'uniScid': uni_scid,
            'pageSize': 100,
            'curPage': 1,
            'sortName': '',
            'sortOrder': '',
        }

        url = 'http://{host}/ecipplatform/publicInfoQueryServlet.json?queryBgxx=true'.format(host=self.host)
        r = self.task_request(session, session.post, url, data=post_data)
        if r is None:
            self.append_model(data, Model.change_info, url, '',
                              status=self.STATUS_FAIL,
                              post_data=data)
            return
        self.append_model(data, Model.change_info, url, r.text, post_data=data)

    # 年报信息
    def get_annual_info(self, session, param_dict, data):
        org = param_dict['org']
        i_d = param_dict['id']
        seq_id = param_dict['seqId']
        nb_reg_no = param_dict['nb_reg_no']
        uni_scid = param_dict['uniScid']

        post_data = {
            'org': org,
            'id': i_d,
            'seqId': seq_id,
            'abnormal': '',
            'activeTabId': '',
            'tmp': 53,
            'regNo': nb_reg_no,
            'admitMain': '08',
            'uniScid': uni_scid,
            'econKind': 51,
            'pageSize': 100,
            'curPage': 1,
            'sortName': '',
            'sortOrder': '',
        }
        url = 'http://{host}/ecipplatform/publicInfoQueryServlet.json?queryQynbxxYears=true'.format(host=self.host)
        r = self.task_request(session, session.post, url, data=post_data)
        if r is None:
            return

        result = util.json_loads(r.text)
        if result is None:
            return

        nb_data = result.get('data')
        for nb in nb_data:
            try:
                year = nb.get('REPORT_YEAR')
                nb_id = nb.get('ID')
            except:
                continue
            # 企业年报基本信息
            self.get_annual_base_info(session, nb_id, data, year)

            # 企业年报网点信息
            self.get_annual_website_info(session, nb_id, data, year)

            # 企业年报股东信息
            self.get_annual_shareholder_info(session, nb_id, data, year)

            # 企业年报对外投资信息
            self.get_annual_investment_info(session, nb_id, data, year)

            # 企业年报对外提供保证担保信息
            self.get_annual_assurance_info(session, nb_id, data, year)

            # 企业年报股权变更信息
            self.get_annual_change_info(session, nb_id, data, year)

            # 企业年报修改信息
            self.get_annual_amendant_info(session, nb_id, data, year)

    # 企业年报基本信息和资产状况信息
    def get_annual_base_info(self, session, nb_id, data, year):
        tmp = random.randrange(0, 100)
        url = 'http://{host}/ecipplatform/publicInfoQueryServlet.json?queryQynbxxJbxx=true&id={nb_id}&tmp={tmp}' \
            .format(host=self.host, nb_id=nb_id, tmp=tmp)
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.annual_info, url, '',
                              status=self.STATUS_FAIL,
                              year=year,
                              classify=Model.type_detail)
            return None, None
        self.append_model(data, Model.annual_info, url, r.text,
                          year=year,
                          classify=Model.type_detail)
        return url, r.text

        # 企业年报网点信息

    def get_annual_website_info(self, session, nb_id, data, year):
        tmp = random.randrange(0, 100)
        url = 'http://{host}/ecipplatform/publicInfoQueryServlet.json?queryQynbxxWzwd=true&id={nb_id}&tmp={tmp}' \
            .format(host=self.host, nb_id=nb_id, tmp=tmp)
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.annual_info, url, '',
                              status=self.STATUS_FAIL,
                              year=year,
                              classify=Model.type_detail)
            return None, None
        self.append_model(data, Model.annual_info, url, r.text,
                          year=year,
                          classify=Model.type_detail)
        return url, r.text

        # 企业年报股东信息

    def get_annual_shareholder_info(self, session, nb_id, data, year):
        tmp = random.randrange(0, 100)
        post_data = {
            'id': nb_id,
            'tmp': tmp,
            'pageSize': 100,
            'curPage': 1,
            'sortName': '',
            'sortOrder': ''
        }
        url = 'http://{host}/ecipplatform/publicInfoQueryServlet.json?queryQynbxxGdcz=true' \
            .format(host=self.host)
        r = self.task_request(session, session.post, url, data=post_data)
        if r is None:
            self.append_model(data, Model.annual_info, url, '',
                              status=self.STATUS_FAIL,
                              year=year,
                              classify=Model.type_detail)
            return None, None
        self.append_model(data, Model.annual_info, url, r.text,
                          year=year,
                          classify=Model.type_detail)
        return url, r.text

        # 企业年报对外投资信息

    def get_annual_investment_info(self, session, nb_id, data, year):
        tmp = random.randrange(0, 100)
        url = 'http://{host}/ecipplatform/publicInfoQueryServlet.json?queryQynbxxDwtz=true&id={nb_id}&tmp={tmp}' \
            .format(host=self.host, nb_id=nb_id, tmp=tmp)
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.annual_info, url, '',
                              status=self.STATUS_FAIL,
                              year=year,
                              classify=Model.type_detail)
            return None, None
        self.append_model(data, Model.annual_info, url, r.text,
                          year=year,
                          classify=Model.type_detail)
        return url, r.text

    # 企业年报对外提供保证担保信息
    def get_annual_assurance_info(self, session, nb_id, data, year):
        tmp = random.randrange(0, 100)
        post_data = {
            'id': nb_id,
            'tmp': tmp,
            'pageSize': 100,
            'curPage': 1,
            'sortName': '',
            'sortOrder': ''
        }
        url = 'http://{host}/ecipplatform/publicInfoQueryServlet.json?queryQynbxxDwdb=true' \
            .format(host=self.host)
        r = self.task_request(session, session.post, url, data=post_data)
        if r is None:
            self.append_model(data, Model.annual_info, url, '',
                              status=self.STATUS_FAIL,
                              year=year,
                              classify=Model.type_detail)
            return None, None
        self.append_model(data, Model.annual_info, url, r.text,
                          year=year,
                          classify=Model.type_detail)
        return url, r.text

    # 企业年报股权变更信息
    def get_annual_change_info(self, session, nb_id, data, year):
        tmp = random.randrange(0, 100)
        post_data = {
            'id': nb_id,
            'tmp': tmp,
            'pageSize': 100,
            'curPage': 1,
            'sortName': '',
            'sortOrder': ''
        }
        url = 'http://{host}/ecipplatform/publicInfoQueryServlet.json?queryQynbxxGqbg=true' \
            .format(host=self.host)
        r = self.task_request(session, session.post, url, data=post_data)
        if r is None:
            self.append_model(data, Model.annual_info, url, '',
                              status=self.STATUS_FAIL,
                              year=year,
                              classify=Model.type_detail)
            return None, None
        self.append_model(data, Model.annual_info, url, r.text,
                          year=year,
                          classify=Model.type_detail)
        return url, r.text

    # 企业年报修改信息
    def get_annual_amendant_info(self, session, nb_id, data, year):
        tmp = random.randrange(0, 100)
        post_data = {
            'id': nb_id,
            'tmp': tmp,
            'pageSize': 100,
            'curPage': 1,
            'sortName': '',
            'sortOrder': ''
        }
        url = 'http://{host}/ecipplatform/publicInfoQueryServlet.json?queryQynbxxXgxx=true' \
            .format(host=self.host)
        r = self.task_request(session, session.post, url, data=post_data)
        if r is None:
            self.append_model(data, Model.annual_info, url, '',
                              status=self.STATUS_FAIL,
                              year=year,
                              classify=Model.type_detail)
            return None, None
        self.append_model(data, Model.annual_info, url, r.text,
                          year=year,
                          classify=Model.type_detail)
        return url, r.text

    def get_shareholder_info(self, session, param_dict, data):
        org = param_dict['org']
        i_d = param_dict['id']
        seq_id = param_dict['seqId']
        try:
            url = 'http://{host}/ecipplatform/publicInfoQueryServlet.json?queryQyjsxxGdcz=true&abnormal=&activeTabId=&admitMain=08&curPage=1&id={id}&org={org}&pageSize=100&seqId={seqId}&sortName=&sortOrder=&tmp=31'.format(
                id=i_d, seqId=seq_id, org=org, host=self.host)
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.shareholder_info, url, '', status=self.STATUS_FAIL)
                return
            self.append_model(data, Model.shareholder_info, url, r.text)
        except Exception as e:
            self.log.exception(e)

    def get_branch_info(self, session, param_dict, data):
        org = param_dict['org']
        i_d = param_dict['id']
        seq_id = param_dict['seqId']
        reg_no = param_dict['regNo']
        uni_scid = param_dict['uniScid']
        url = 'http://{host}/ecipplatform/publicInfoQueryServlet.json?queryFzjg=true&org={org}&id={id}&seqId={seqId}&abnormal=&activeTabId=&tmp=75&regNo={regNo}&admitMain=08&uniScid={uniScid}'.format(
            id=i_d, seqId=seq_id, org=org, host=self.host, regNo=reg_no, uniScid=uni_scid)
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.branch_info, url, '', status=self.STATUS_FAIL)
            return
        self.append_model(data, Model.branch_info, url, r.text)

    def parse_param(self, href):
        # /ecipplatform/jiangsu.jsp?org=8AD843CF9D91B3B2BB82982272E267AB&id=CF61AECE82AEA4651C259CEDA4126DA0&seqId=8A0E429E8B038E7017660C378B89542C&activeTabId=
        param_dict = {}
        try:
            sub_url_list = href.split('?')
            if len(sub_url_list) != 2:
                return None

            sub_url = sub_url_list[1]
            sub_list = sub_url.split('&')
            if len(sub_list) < 3:
                return None

            for item in sub_list:
                item_list = item.split('=')
                if len(item_list) < 2:
                    continue

                param_dict[item_list[0]] = item_list[1]

            if 'org' not in param_dict or 'id' not in param_dict or 'seqId' not in param_dict:
                return None

        except Exception as e:
            self.log.exception(e)
            return None

        return param_dict

    @staticmethod
    def get_keyword_info(base_text):
        # "REG_NO":"913202052500830484", "UNI_SCID":"97F2A486B9901B971CCF8E9FA95B14A3"
        json_data = util.json_loads(base_text)
        if json_data is None:
            return None, None
        reg_no = json_data.get('REG_NO', None)
        uni_scid = json_data.get('UNI_SCID', None)
        nb_reg_no = json_data.get('REG_NO_EN', None)

        return reg_no, uni_scid, nb_reg_no

    def get_detail_html_list(self, seed, session, param_list):
        # 保存企业名称
        data_list = []
        for item in param_list:
            try:
                href = item.get('href', None)
                if href is None:
                    self.log.error('参数存储异常: item = {item}'.format(item=item))
                    continue

                search_name = item.get('search_name', None)
                if search_name is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                param_dict = self.parse_param(href)
                if param_dict is None:
                    self.log.error('参数信息错误: href = {href}'.format(href=href))
                    continue

                # 基本信息
                url, base_text = self.get_base_info(session, param_dict)
                if url is None or base_text is None:
                    continue

                # 获得公司名称
                company = self.__get_company_name(base_text)
                if company is None or company == '':
                    self.log.error('公司名称解析失败..param_dict = {param_dict} {text}'.format(
                        text=base_text, param_dict=param_dict))
                    continue

                # 建立数据模型
                data = self.get_model(company, seed, search_name, self.province)

                # 存储数据
                self.append_model(data, Model.base_info, url, base_text)

                # 出资信息
                self.get_contributive_info(session, param_dict, data)

                reg_no, uni_scid, nb_reg_no = self.get_keyword_info(base_text)
                if reg_no is not None and uni_scid is not None:
                    param_dict['regNo'] = reg_no
                    param_dict['uniScid'] = uni_scid

                    # 主要人员信息
                    self.get_key_person_info(session, param_dict, data)

                    # 分支机构
                    self.get_branch_info(session, param_dict, data)

                    # 变更信息
                    self.get_change_info(session, param_dict, data)

                    # 获得年报信息
                    if nb_reg_no is not None:
                        param_dict['nb_reg_no'] = nb_reg_no
                        self.get_annual_info(session, param_dict, data)

                # 股东信息
                self.get_shareholder_info(session, param_dict, data)

                data_list.append(data)
            except Exception as e:
                self.log.exception(e)

        return self.sent_to_target(data_list)
