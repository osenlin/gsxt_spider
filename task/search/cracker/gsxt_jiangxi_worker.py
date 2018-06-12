#!/usr/bin/env python
# -*- coding:utf-8 -*-
import random
import re

from pyquery import PyQuery

from base.gsxt_base_worker import GsxtBaseWorker
from common import util
from common.global_field import Model
from config.conf import encry_jx_cq_conf

'''
1. 搜索没有结果判断功能添加
2. 包含出资信息 出资信息中有详情页
3. 包含年报信息
4. 增加拓扑信息 已经check
5. 完成列表页名称提取
'''


class GsxtJiangXiWorker(GsxtBaseWorker):
    def __init__(self, **kwargs):
        GsxtBaseWorker.__init__(self, **kwargs)

    def get_search_list_html(self, keyword, session):
        param_list = []
        try:
            url = 'http://{host}/index.jsp'.format(host=self.host)
            content = self.get_captcha_geetest(url, '#searchkey', '#serach', keyword,
                                               '.sea_007',
                                               success='.sea2')
            if content is None:
                return param_list, self.SEARCH_ERROR

            jq = PyQuery(content, parser='html')
            tip_text = jq.find('div.sea_02').find('span').text()
            if tip_text == '0':
                return param_list, self.SEARCH_NOTHING_FIND

            param_set = set()
            pattern = 'toEntname\(\'(.*?)\',\'(.*?)\'\)'
            regex = re.compile(pattern)
            item_list = jq.find('div.sea_03').find('a').items()
            for item in item_list:
                sub_url = item.attr('href')
                if sub_url is None or sub_url == '':
                    onclick = item.attr('onclick')
                    if onclick is None or onclick == '':
                        continue
                    search_list = regex.findall(onclick)
                    if len(search_list) <= 0:
                        continue
                    script = "toEntname('{href}','{pripid}')".format(
                        href=search_list[0][0], pripid=search_list[0][1])
                    sub_url = self.get_encry_pripid_detail(encry_jx_cq_conf['url'], script)
                    if sub_url is None:
                        return param_list, self.SEARCH_ERROR

                if sub_url in param_set:
                    continue

                company = item.find('.sea_04').find('.sea_007').text()
                if company is None or company == '':
                    continue

                search_name = company.replace(' ', '')
                if search_name == '':
                    continue

                param = {
                    'sub_url': sub_url,
                    'search_name': search_name,
                }

                seed_code = item.find('.sea_08').find('.float-left.sea_09').find('.sea_11').text()
                if seed_code is not None and seed_code.strip() != '':
                    param['unified_social_credit_code'] = seed_code

                param_set.add(sub_url)
                param_list.append(param)
        except Exception as e:
            self.log.exception(e)
            return param_list, self.SEARCH_ERROR

        return param_list, self.SEARCH_SUCCESS if len(param_list) > 0 else self.SEARCH_ERROR

    @staticmethod
    def __get_company_name(text):
        result = util.json_loads(text)
        if result is None:
            return None

        search_list = result.get('ENTNAME', None)
        if search_list is not None:
            return search_list.strip()

        return None

    # 基本信息
    def get_base_info(self, session, pri_pid):
        url = 'http://{host}/baseinfo/queryenterpriseinfoByRegnore.do' \
              '?pripid={pripid}' \
            .format(host=self.host, pripid=pri_pid)
        r = self.task_request(session, session.get, url)
        if r is None:
            return None, None

        return url, r.text

    # 主要人员
    def get_key_person_info(self, session, pri_pid, data):
        rand = util.get_random_num()
        url = 'http://{host}/epriperson/queryPerson.do?' \
              'pripid={pripid}&randommath={randommath}' \
            .format(host=self.host, pripid=pri_pid, randommath=rand)
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.key_person_info, url, '', status=self.STATUS_FAIL)
            return
        self.append_model(data, Model.key_person_info, url, r.text)

    # 获得分支机构信息
    def get_branch_info(self, session, pri_pid, data):
        rand = util.get_random_num()
        url = 'http://{host}/ebrchinfo/getqueryEBrchinfo.do?' \
              'pripid={pripid}&randommath={randommath}' \
            .format(host=self.host, pripid=pri_pid, randommath=rand)
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.branch_info, url, '', status=self.STATUS_FAIL)
            return

        self.append_model(data, Model.branch_info, url, r.text)

    # 清算信息
    def get_liquidation_info(self, session, pri_pid, data):
        rand = util.get_random_num()
        url = 'http://{host}/eliqmbr/getqueryeliqmbr.do?' \
              'pripid={pripid}&randommath={randommath}' \
            .format(host=self.host, pripid=pri_pid, randommath=rand)
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.liquidation_info, url, '', status=self.STATUS_FAIL)
            return

        self.append_model(data, Model.liquidation_info, url, r.text)

    # 变更信息
    def get_change_info(self, session, pri_pid, data):
        page = 1
        total_page = 1
        while page <= total_page:
            url = 'http://{host}/gtalterrecoder/getquerygtalterrecoder.do' \
                  '?pripid={pripid}&randommath={randommath}&currentPage={page}' \
                .format(host=self.host, pripid=pri_pid, randommath=util.get_random_num(), page=page)
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.change_info, url, '', status=self.STATUS_FAIL)
                return

            json_data = util.json_loads(r.text)
            if json_data is None:
                self.append_model(data, Model.change_info, url, r.text, status=self.STATUS_FAIL)
                return

            page_info = json_data.get('page', None)
            if page_info is None:
                self.append_model(data, Model.change_info, url, r.text, status=self.STATUS_FAIL)
                return

            total_page = page_info.get('totalPage', None)
            if total_page is None:
                self.append_model(data, Model.change_info, url, r.text, status=self.STATUS_FAIL)
                return

            total_page = int(total_page)
            if total_page == 0:
                total_page = 1

            self.append_model(data, Model.change_info, url, r.text)

            page += 1

    # 股东信息
    def get_shareholder_info(self, session, pri_pid, data):
        page = 1
        total_page = 1
        while page <= total_page:
            url = 'http://{host}/ansubcapital/queryAnsubcapitaltrue.do' \
                  '?pripid={pripid}&randommath={randommath}&currentPage={page}' \
                .format(host=self.host, pripid=pri_pid, randommath=util.get_random_num(), page=page)
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.shareholder_info, url, '', status=self.STATUS_FAIL)
                return

            json_data = util.json_loads(r.text)
            if json_data is None:
                self.append_model(data, Model.shareholder_info, url, r.text, status=self.STATUS_FAIL)
                return

            page_info = json_data.get('page', None)
            if page_info is None:
                self.append_model(data, Model.shareholder_info, url, r.text, status=self.STATUS_FAIL)
                return

            total_page = page_info.get('totalPage', None)
            if total_page is None:
                self.append_model(data, Model.shareholder_info, url, r.text, status=self.STATUS_FAIL)
                return

            total_page = int(total_page)
            if total_page == 0:
                total_page = 1

            self.append_model(data, Model.shareholder_info, url, r.text)
            page += 1

    # 获得年报信息
    def get_annual_info(self, session, pri_pid, data):
        rand = util.get_random_num()
        url = 'http://{host}/anbaseinfo/queryBaseinfoReport.do' \
              '?pripid={pripid}&randommath={randommath}&currentPage=1' \
            .format(host=self.host, pripid=pri_pid, randommath=rand)
        r = self.task_request(session, session.get, url)
        if r is None:
            return

        result = util.json_loads(r.text)
        if result is None:
            return

        len_year = len(result.get('data'))
        page_num = None
        if result.get('page', None) is not None:
            page_num = result.get('page').get('totalPage', None)

        for page in xrange(page_num):
            for i in xrange(len_year):
                try:
                    year = result.get('data')[i]['ANCHEYEAR']
                except:
                    continue
                # 企业年报基本信息
                self.get_annual_base_info(session, pri_pid, data, year)

                # 企业年报网点信息
                self.get_annual_website_info(session, pri_pid, data, year)

                # 企业年报股东信息
                self.get_annual_shareholder_info(session, pri_pid, data, year)

                # 企业年报对外投资信息
                self.get_annual_investment_info(session, pri_pid, data, year)

                # 企业年报对外提供保证担保信息
                self.get_annual_assurance_info(session, pri_pid, data, year)

                # 企业年报股权变更信息
                self.get_annual_change_info(session, pri_pid, data, year)

                # 企业年报修改信息
                self.get_annual_amendant_info(session, pri_pid, data, year)

                # 企业年报企业基本状况
                self.get_annual_status_info(session, pri_pid, data, year)

    # 企业年报基本信息
    def get_annual_base_info(self, session, pri_pid, data, year):
        rand = util.get_random_num()
        url = 'http://{host}/anbaseinfo/getquerbaseinfo.do' \
              '?pripid={pripid}&year={year}&randommath={randommath}' \
            .format(host=self.host, pripid=pri_pid, randommath=rand, year=year)
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.annual_info, url, '',
                              status=self.STATUS_FAIL,
                              year=year,
                              classify=Model.type_detail)
            return
        self.append_model(data, Model.annual_info, url, r.text,
                          year=year,
                          classify=Model.type_detail)

    # 企业年报网点信息
    def get_annual_website_info(self, session, pri_pid, data, year):
        rand = util.get_random_num()
        url = 'http://{host}/anwebsiteinfo/queryAnwebsiteinfo.do' \
              '?pripid={pripid}&year={year}&randommath={randommath}' \
            .format(host=self.host, pripid=pri_pid, randommath=rand, year=year)
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
    def get_annual_shareholder_info(self, session, pri_pid, data, year):
        rand = util.get_random_num()
        url = 'http://{host}/ansubcapital/queryAnsubcapital.do' \
              '?pripid={pripid}&year={year}&randommath={randommath}&showCount=100' \
            .format(host=self.host, pripid=pri_pid, randommath=rand, year=year)
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

    # 企业年报对外投资信息
    def get_annual_investment_info(self, session, pri_pid, data, year):
        rand = util.get_random_num()
        url = 'http://{host}/anforinvestment/queryAnforinvestment.do' \
              '?pripid={pripid}&year={year}&randommath={randommath}' \
            .format(host=self.host, pripid=pri_pid, randommath=rand, year=year)
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

    # 企业年报对外担保
    def get_annual_assurance_info(self, session, pri_pid, data, year):
        rand = util.get_random_num()
        url = 'http://{host}/anforguaranteeinfo/queryAnforguaranteeinfo.do' \
              '?pripid={pripid}&year={year}&randommath={randommath}&showCount=100' \
            .format(host=self.host, pripid=pri_pid, randommath=rand, year=year)
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

    # 企业年报股权变更
    def get_annual_change_info(self, session, pri_pid, data, year):
        rand = util.get_random_num()
        url = 'http://{host}/analterstockinfo/queryAnalterstockinfo.do' \
              '?pripid={pripid}&year={year}&randommath={randommath}&showCount=100' \
            .format(host=self.host, pripid=pri_pid, randommath=rand, year=year)
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

    # 企业年报修改信息
    def get_annual_amendant_info(self, session, pri_pid, data, year):
        rand = util.get_random_num()
        url = 'http://{host}/anupdateinfo/queryAnupdateinfo.do' \
              '?pripid={pripid}&year={year}&randommath={randommath}&showCount=100' \
            .format(host=self.host, pripid=pri_pid, randommath=rand, year=year)
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

    # 企业年报企业基本状况
    def get_annual_status_info(self, session, pripid, data, year):
        url = 'http://{host}/page/nzgsfr/report.jsp?year={year}&pripid={pripid}' \
            .format(host=self.host, pripid=pripid, year=year)
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

    # 出资信息
    def get_contributive_info(self, session, pri_pid, data):
        page = 1
        total_page = 1
        while page <= total_page:
            url = 'http://{host}/einvperson/getqueryeInvPersonService.do' \
                  '?pripid={pripid}&randommath={randommath}&currentPage={page}' \
                .format(host=self.host, pripid=pri_pid, randommath=util.get_random_num(), page=page)
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.contributive_info, url, '',
                                  status=self.STATUS_FAIL, classify=Model.type_list)
                return

            json_data = util.json_loads(r.text)
            if json_data is None:
                self.append_model(data, Model.contributive_info, url, r.text,
                                  status=self.STATUS_FAIL, classify=Model.type_list)
                return

            page_info = json_data.get('page', None)
            if page_info is None:
                self.append_model(data, Model.contributive_info, url, r.text,
                                  status=self.STATUS_FAIL, classify=Model.type_list)
                return

            total_page = page_info.get('totalPage', None)
            if total_page is None:
                self.append_model(data, Model.contributive_info, url, r.text,
                                  status=self.STATUS_FAIL, classify=Model.type_list)
                return

            total_page = int(total_page)
            if total_page == 0:
                total_page = 1

            self.append_model(data, Model.contributive_info, url, r.text,
                              classify=Model.type_list)

            show_count = page_info.get('showCount', None)
            if show_count is None:
                return

            # 解析详细信息
            data_info = json_data.get('data', None)
            if data_info is not None:
                for index, item in enumerate(data_info):
                    invid = item.get('INVID', None)
                    if invid is None:
                        continue
                    url = 'http://{host}/einvperson/queryInfo?invid={invid}&random={rand}'.format(
                        host=self.host, invid=invid, rand=random.randint(10, 100))
                    r = self.task_request(session, session.get, url)
                    if r is None:
                        self.append_model(data, Model.contributive_info, url, '',
                                          status=self.STATUS_FAIL,
                                          classify=Model.type_detail)
                        continue
                    self.append_model(data, Model.contributive_info, url, r.text,
                                      classify=Model.type_detail)

            page += 1

    def get_key_word(self, sub_url):
        # /page/nzgsfr/informationinfo.jsp?pripid=MzYwODA1MjAxNDA4MTEwMDI1MjIwMwu002Cu002C&searchtype=qyxy
        try:
            sub_list = sub_url.split('?')[-1].split('&')
            for item in sub_list:
                key, value = item.split('=')
                if key == 'pripid':
                    return value
        except Exception as e:
            self.log.exception(e)

        return None

    def get_pripid_info(self, item):
        sub_url = item.get('sub_url', None)
        if sub_url is None:
            href = item.get('href', None)
            pri_pid = item.get('pripid', None)
            if href is None or pri_pid is None:
                self.log.error('参数错误: item = {item}'.format(item=item))
                return None

            script = "toEntname('{href}','{pripid}')".format(
                href=href, pripid=pri_pid)
            sub_url = self.get_encry_pripid_detail(encry_jx_cq_conf['url'], script)
            if sub_url is None:
                return None

        keyword = self.get_key_word(sub_url)
        if keyword is None:
            self.log.error('解析参数错误: sub_url = {url}'.format(url=sub_url))
            return None

        return keyword

    def get_detail_html_list(self, seed, session, param_list):
        # 保存企业名称
        data_list = []
        for item in param_list:
            try:
                pri_pid = self.get_pripid_info(item)
                # pri_pid = 'MzYwMjAwMjAwODA4MjUwMDAwMDA4NAu002Cu002C'
                if pri_pid is None:
                    continue

                search_name = item.get('search_name', None)
                if search_name is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                url, text = self.get_base_info(session, pri_pid)
                if url is None:
                    continue

                # 获得公司名称
                company = self.__get_company_name(text)
                if company is None or company == '':
                    self.log.error('公司名称解析失败..search_name = {name}'.format(name=search_name))
                    continue

                # 建立数据模型
                data = self.get_model(company, seed, search_name, self.province)

                # 存储数据
                self.append_model(data, Model.base_info, url, text)

                # 主要人员
                self.get_key_person_info(session, pri_pid, data)

                # 分支机构
                self.get_branch_info(session, pri_pid, data)

                # 出资信息
                self.get_contributive_info(session, pri_pid, data)

                # 清算信息
                self.get_liquidation_info(session, pri_pid, data)

                # 变更信息
                self.get_change_info(session, pri_pid, data)

                # 股东信息
                self.get_shareholder_info(session, pri_pid, data)

                # 年报信息
                self.get_annual_info(session, pri_pid, data)

                data_list.append(data)
            except Exception as e:
                self.log.exception(e)

        return self.sent_to_target(data_list)
