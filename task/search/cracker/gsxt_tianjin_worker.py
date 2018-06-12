#!/usr/bin/env python
# -*- coding:utf-8 -*-

import json

from pyquery import PyQuery

from base.gsxt_base_worker import GsxtBaseWorker
from common import util
from common.global_field import Model

'''
1. 验证码破解方式
2. 没有抓年报信息
3. 没有抓出资信息
4. 添加完成统计信息
6. 完成列表页名称提取
'''

'''
分析案例: 铁道第三勘察设计院集团有限公司
'''


class GsxtTianJinWorker(GsxtBaseWorker):
    def __init__(self, **kwargs):
        GsxtBaseWorker.__init__(self, **kwargs)
        self.proxy_type = self.PROXY_TYPE_STATIC

    # 需要存入无搜索结果
    def get_search_list_html(self, keyword, session):
        param_list = []
        # return [{
        #     'href': 'http://tj.gsxt.gov.cn/%7BgXcD8zGo1H8bwKZcUGPs227sD2CkCOh3AO_FSG4fqLdGl3Yj57Ri0HNqxvOj5cxsOCZ4vefKWVukM00052YKhxVpaXzvilmmb6jYgs7tf2_tiXI3CjLR418g4-Y7kcJ58SZ9BHatMxFWxiirrOvX_A-1489737284575%7D',
        #     'search_name': '铁道第三勘察设计院集团有限公司'}], self.SEARCH_SUCCESS
        ## 获取他的关键词 到公司列表
        try:
            # host的值要改
            # 这个url要改
            # http://tj.gsxt.gov.cn/corp-query-search-1.html

            url = 'http://{host}/index.html'.format(host=self.host)
            content = self.get_captcha_geetest(url, '#keyword', '#btn_query',
                                               keyword, 'div.search_result.g9')
            if content is None:
                return param_list, self.SEARCH_ERROR

            jq = PyQuery(content, parser='html')
            if jq.find('.search_result_span1').text() == '0':
                return param_list, self.SEARCH_NOTHING_FIND

            item_list = jq.find('.main-layout').find('a').items()
            for item in item_list:

                # 公司名
                company = item.find('h1').text().strip()
                if company is None or company == '':
                    continue

                search_name = company.replace(' ', '')
                if search_name == '':
                    continue

                # uid,company,状态就够了
                uuid = item.attr('href')
                href = "http://{host}{uuid}".format(host=self.host, uuid=uuid)
                status = item.find('.wrap-corpStatus').text()

                param = {
                    'href': href,
                    'search_name': search_name,
                }
                if status is not None and status != '':
                    param['status'] = status

                seed_code = item.find('div.f14.g9.pt10').find('.div-map2').find('span.g3').text()
                if seed_code is not None and seed_code.strip() != '':
                    param['unified_social_credit_code'] = seed_code

                param_list.append(param)
        except Exception as e:
            self.log.exception(e)
            return param_list, self.SEARCH_ERROR

        return param_list, self.SEARCH_SUCCESS if len(param_list) > 0 else self.SEARCH_ERROR

    def __get_company_name(self, text):
        try:
            name = PyQuery(text, parser='html').find('.overview').find('#entName').text()
            if name is not None and name.strip() != '':
                return name.strip()

            name = PyQuery(text, parser='html').find('h1.fullName').text()
            if name is not None and name.strip() != '':
                return name.strip()

            return None
        except Exception as e:
            self.log.exception(e)

        return None

    # 基本信息
    def get_base_info(self, session, url):
        # 获取基础信息
        base_info = self.task_request(session, session.get, url)
        if base_info is None:
            self.log.error('获取基础信息页面失败...')
            return None, None
        return url, base_info.text

    def get_contributive_info(self, session, data, base_text):
        sub_url = util.get_match_value('var shareholderUrl = "', '";', base_text)
        url = "http://{0}{1}".format(self.host, sub_url)

        cur_page = 1
        total_page = 1

        while cur_page <= total_page:
            post_data = {
                'draw': 1,
                'start': (cur_page - 1) * 5,
                'length': 5,
            }
            r = self.task_request(session, session.post, url, data=post_data)
            if r is None:
                self.append_model(data, Model.contributive_info, url, '', status=self.STATUS_FAIL)
                return

            self.append_model(data, Model.contributive_info, url, r.text, post_data=post_data)

            # 从 那个contributive_info的页面里面拿 详细信息
            json_data = util.json_loads(r.text)
            if json_data is None:
                return

            # 这里不成功
            json_array = json_data.get('data', [])
            for json_item in json_array:
                inv_id = json_item.get('invId', '')
                detail_url = "http://{0}/corp-query-entprise-info-shareholderDetail-{1}.html".format(self.host, inv_id)
                detail_r = self.task_request(session, session.get, detail_url)
                if detail_r is None:
                    self.append_model(data, Model.contributive_info, detail_url, '', status=self.STATUS_FAIL,
                                      classify=Model.type_detail)
                    continue
                self.append_model(data, Model.contributive_info, detail_url, detail_r.text,
                                  classify=Model.type_detail)
            if total_page <= 1:
                total_page = json_data.get('totalPage', 1)
            cur_page += 1

    def get_annual_info(self, session, data, base_text):

        annual_info_url = util.get_match_value('anCheYearInfo = "', '";', base_text)
        annual_info_url = "http://{0}{1}".format(self.host, annual_info_url)
        ent_type = PyQuery(base_text, parser='html').find('#entType').attr('value')
        host_file = util.get_match_value('annRepDetailUrl = "', '";', base_text)

        annual_list_r = self.task_request(session, session.get, annual_info_url)
        if annual_list_r is None:
            self.append_model(data, Model.annual_info, annual_info_url, '', status=self.STATUS_FAIL)
            return

        self.append_model(data, Model.annual_info, annual_info_url, annual_list_r.text)
        json_data_arr = util.json_loads(annual_list_r.text)
        if json_data_arr is None:
            return

        for json_item in json_data_arr:
            an_che_id = json_item.get('anCheId', '')
            an_che_year = json_item.get('anCheYear', '')

            # 进行拼接url并且存储,发请求,获得年报
            if an_che_id is not None and an_che_id != '' and an_che_year is not None and an_che_year != '':
                url = "http://{0}{1}?anCheId={2}&entType={3}&anCheYear={4}".format(self.host, host_file, an_che_id,
                                                                                   ent_type,
                                                                                   an_che_year)
                # r = self.task_request(session, session.get, url)
                # if r is None:
                #     # self.append_model(data, Model.annual_info, url, '',
                #     #                   status=self.STATUS_FAIL,
                #     #                   year=an_che_year, classify=Model.type_detail)
                #     continue

                # 基本信息
                # self.append_model(data, Model.annual_info, url, r.text,
                #                   year=an_che_year,
                #                   classify=Model.type_detail)

                session.headers = {
                    'Host': '{host}'.format(host=self.host),
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:51.0) Gecko/20100101 Firefox/51.0',
                    'Accept': 'application/json, text/javascript, */*; q=0.01',
                    'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
                    'Accept-Encoding': 'gzip, deflate',
                    'X-Requested-With': 'XMLHttpRequest',
                    'Referer': url,
                    'Connection': 'keep-alive',
                    'Content-Length': '0'
                }

                # 现在对年报其他信息获取
                all_web_info = "/corp-query-entprise-info-vAnnualReportBaseInfoForJs-"
                other_info_url = "http://{0}{1}{2}.html".format(self.host, all_web_info, an_che_id)
                other_info_r = self.task_request(session, session.get, other_info_url)
                if other_info_r is None or other_info_r.text == '':
                    continue

                # 1.网站 2.股东及出资信息 3.对外投资信息 4.企业资产状况信息
                # 5.对外提供保证担保信息 6.股权变更信息 7.修改信息 8.基本信息
                # webSiteInfoUrl 网站信息
                # baseinfoUrl 基本信息
                # vAnnualReportAlterstockinfoUrl 股权变更
                # sponsorUrl 股东出资
                # alterUrl 修改信息
                # forInvestmentUrl 对外投资
                # forGuaranteeinfoUrl 对外担保
                # vAnnualReportBranchProduction  企业资产状况信息

                # key_word_list = ['webSiteInfoUrl', 'baseinfoUrl', 'vAnnualReportAlterstockinfoUrl',
                #                  'vAnnualReportBranchProductionUrl', 'sponsorUrl', 'alterUrl',
                #                  'forInvestmentUrl', 'forGuaranteeinfoUrl']

                json_all_url_dict = json.loads(other_info_r.text)  # 得到是一个字典
                for web_k, web_v in json_all_url_dict.iteritems():
                    # if web_k in key_word_list:
                    annual_item_url = "http://{0}{1}".format(self.host, web_v)
                    annual_item_r = self.task_request(session, session.get, annual_item_url)
                    if annual_item_r is None:
                        self.append_model(data, Model.annual_info, annual_item_url, '',
                                          status=self.STATUS_FAIL,
                                          year=an_che_year, classify=Model.type_detail)
                        continue
                    self.append_model(data, Model.annual_info, annual_item_url, annual_item_r.text,
                                      year=an_che_year, classify=Model.type_detail)

    # 主要人员信息
    def get_key_person(self, session, data, base_text):
        key_person_url = util.get_match_value('keyPersonUrl = "', '";', base_text)
        key_person_url = "http://{0}{1}".format(self.host, key_person_url)
        key_person_r = self.task_request(session, session.get, key_person_url)
        if key_person_r is None:
            self.append_model(data, Model.key_person_info, key_person_url, '',
                              status=self.STATUS_FAIL)
        else:
            self.append_model(data, Model.key_person_info, key_person_url, key_person_r.text)

    # 获得分支机构
    def get_branch_info(self, session, data, base_text):
        # 基本信息中 分支机构信息查看全部
        branch_url = util.get_match_value('branchUrl = "', '";', base_text)
        branch_url = "http://{0}{1}".format(self.host, branch_url)
        branch_r = self.task_request(session, session.get, branch_url)
        if branch_r is None:
            self.append_model(data, Model.branch_info, branch_url, '', status=self.STATUS_FAIL)
        else:
            self.append_model(data, Model.branch_info, branch_url, branch_r.text)

    # 股东信息
    def get_shareholder_info(self, session, data, base_text):
        sub_url = util.get_match_value('insInvinfoUrl = "', '"', base_text)
        url = "http://{0}{1}".format(self.host, sub_url)
        cur_page = 1
        total_page = 1

        while cur_page <= total_page:
            post_data = {
                'draw': 1,
                'start': (cur_page - 1) * 5,
                'length': 5,
            }
            r = self.task_request(session, session.post, url, data=post_data)
            if r is None:
                self.append_model(data, Model.shareholder_info, url, '', status=self.STATUS_FAIL)
                return

            self.append_model(data, Model.shareholder_info, url, r.text, post_data=post_data)

            # 从 那个contributive_info的页面里面拿 详细信息
            json_data = util.json_loads(r.text)
            if json_data is None:
                return

            if total_page <= 1:
                total_page = json_data.get('totalPage', 1)
            cur_page += 1

    # 变更信息
    def get_change_info(self, session, data, base_text):
        sub_url = util.get_match_value('alterInfoUrl = "', '";', base_text)
        url = "http://{0}{1}".format(self.host, sub_url)

        cur_page = 1
        total_page = 1
        while cur_page <= total_page:
            post_data = {
                'draw': 1,
                'start': (cur_page - 1) * 5,
                'length': 5,
            }
            r = self.task_request(session, session.post, url, data=post_data)
            if r is None:
                self.append_model(data, Model.change_info, url, '', status=self.STATUS_FAIL)
                return

            self.append_model(data, Model.change_info, url, r.text, post_data=post_data)

            json_data = util.json_loads(r.text)
            if json_data is None:
                return

            if total_page <= 1:
                total_page = json_data.get('totalPage', 1)
            cur_page += 1

    # 股权出质登记信息
    def get_equity_pledged_info(self, session, data, base_text):
        sub_url = util.get_match_value('stakQualitInfoUrl = "', '"', base_text)
        url = "http://{0}{1}".format(self.host, sub_url)
        cur_page = 1
        total_page = 1

        while cur_page <= total_page:
            post_data = {
                'draw': 1,
                'start': (cur_page - 1) * 5,
                'length': 5,
            }
            r = self.task_request(session, session.post, url, data=post_data)
            if r is None:
                self.append_model(data, Model.equity_pledged_info, url, '', status=self.STATUS_FAIL)
                return

            self.append_model(data, Model.equity_pledged_info, url, r.text, post_data=post_data)

            json_data = util.json_loads(r.text)
            if json_data is None:
                return

            if total_page <= 1:
                total_page = json_data.get('totalPage', 1)
            cur_page += 1

    # 股权变更信息
    def get_change_shareholding_info(self, session, data, base_text):
        sub_url = util.get_match_value('insAlterstockinfoUrl = "', '"', base_text)
        url = "http://{0}{1}".format(self.host, sub_url)
        cur_page = 1
        total_page = 1

        while cur_page <= total_page:
            post_data = {
                'draw': 1,
                'start': (cur_page - 1) * 5,
                'length': 5,
            }
            r = self.task_request(session, session.post, url, data=post_data)
            if r is None:
                self.append_model(data, Model.change_shareholding_info, url, '', status=self.STATUS_FAIL)
                return

            self.append_model(data, Model.change_shareholding_info, url, r.text, post_data=post_data)

            json_data = util.json_loads(r.text)
            if json_data is None:
                return

            if total_page <= 1:
                total_page = json_data.get('totalPage', 1)
            cur_page += 1

    def get_detail_html_list(self, seed, session, param_list):
        # 保存企业名称
        data_list = []
        for item in param_list:
            try:
                detail_url = item.get('href', None)
                if detail_url is None:
                    self.log.error('参数存储异常: item = {item}'.format(item=item))
                    continue

                search_name = item.get('search_name', None)
                if search_name is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                # 基本信息 股东信息 变更信息
                url, base_text = self.get_base_info(session, detail_url)
                if base_text is None:
                    continue

                # 获得公司名称
                company = self.__get_company_name(base_text)
                if company is None or company == '':
                    self.log.error('公司名称解析失败..url = {url}'.format(url=detail_url))
                    continue

                # 建立数据模型
                data = self.get_model(company, seed, search_name, self.province)
                # 存储数据
                # 基本信息
                self.append_model(data, Model.base_info, url, base_text)

                # 主要人员
                self.get_key_person(session, data, base_text)

                # 分支机构
                self.get_branch_info(session, data, base_text)

                # 股东信息
                self.get_shareholder_info(session, data, base_text)

                # 变更信息
                self.get_change_info(session, data, base_text)

                # 股东出资信息
                self.get_contributive_info(session, data, base_text)

                # 年报信息
                self.get_annual_info(session, data, base_text)

                # 股权出质登记信息
                self.get_equity_pledged_info(session, data, base_text)

                # 股权变更信息
                self.get_change_shareholding_info(session, data, base_text)

                data_list.append(data)
            except Exception as e:
                self.log.exception(e)

        return self.sent_to_target(data_list)
