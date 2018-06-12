# -*- coding: utf-8 -*-
import re

from pyquery import PyQuery

from base.gsxt_base_worker import GsxtBaseWorker
from common import util
from common.global_field import Model

'''
1. 搜索没有结果判断功能添加
2. 包含出资信息 出资信息在基本信息里面 有出资详情页
3. 包含年报信息
4. 添加统计信息
5. 添加完成拓扑信息 已经check
6. 完成列表页名称提取
'''


class GsxtHuNanWorker(GsxtBaseWorker):
    def __init__(self, **kwargs):
        GsxtBaseWorker.__init__(self, **kwargs)
        pattern = '<script>window\.location\.href=\'(.*?)\';</script>'
        self.wscckey_regex = re.compile(pattern)
        self.url = 'http://{host}/notice'.format(host=self.host)
        self.pattern = 'window\.open\(\'http://hn\.gsxt\.gov\.cn/notice/notice/view\?uuid=(.*?)&tab=01\'\)'
        self.input_selector = '#keyword'
        self.search_selector = '#buttonSearch'
        self.result_selector = 'div.contentA div.contentA1 p'
        self.success_selector = None
        self.proxy_type = self.PROXY_TYPE_DYNAMIC

    def get_search_list_html(self, keyword, session):
        param_list = []
        try:
            content = self.get_captcha_geetest(self.url,
                                               self.input_selector,
                                               self.search_selector,
                                               keyword,
                                               self.result_selector,
                                               success=self.success_selector)
            if content is None:
                return param_list, self.SEARCH_ERROR

            # 这个IP已经被封禁
            if util.judge_feature(content):
                self.report_session_proxy(session)
                return param_list, self.SEARCH_ERROR

            jq = PyQuery(content, parser='html')
            if jq.find('div.contentA1').find('p').find('span').text() == '0':
                return param_list, self.SEARCH_NOTHING_FIND

            regex = re.compile(self.pattern)
            item_list = jq.find('.tableContent.page-item').items()
            param_set = set()
            for item in item_list:
                try:
                    onclick = item.attr('onclick')
                    if onclick is None or onclick == '':
                        continue

                    search_list = regex.findall(onclick)
                    if len(search_list) <= 0:
                        continue

                    td = item.find('table').find('thead').find('td')

                    # 获取状态
                    status = td.find('i').text()

                    # 获得企业名
                    td.find('i').remove()
                    td.find('b').remove()
                    search_name = td.text()
                    if search_name is None:
                        continue

                    search_name = search_name.replace(' ', '')
                    if search_name == '':
                        continue

                    if search_name in param_set:
                        continue

                    seed_code = None
                    code_text = item.find('th.icon1').text()
                    if code_text is not None and code_text.strip() != '':
                        part = code_text.split('：')
                        if len(part) >= 2:
                            seed_code = part[1]

                    param_set.add(search_name)

                    param = {
                        'uuid': search_list[0],
                        'search_name': search_name,
                    }
                    if status is not None and status != '':
                        param['status'] = status
                    if seed_code is not None and seed_code.strip() != '':
                        param['unified_social_credit_code'] = seed_code

                    param_list.append(param)
                except Exception as e:
                    self.log.exception(e)
        except Exception as e:
            self.log.exception(e)
            return param_list, self.SEARCH_ERROR

        return param_list, self.SEARCH_SUCCESS if len(param_list) > 0 else self.SEARCH_ERROR

    @staticmethod
    def __get_company_name(text):
        search_list = re.findall('<li class=\"titleB\"><em title=\".*?\">(.*?)</em>', text)
        if len(search_list) > 0:
            return search_list[0].strip()

        return None

    def get_shareholder_info(self, session, uuid, data):
        url = 'http://{host}/notice/notice/view?uuid={uuid}&tab=02'.format(
            host=self.host, uuid=uuid)
        r = self.task_request_wscckey(session, session.get, url)
        if r is None:
            self.append_model(data, Model.shareholder_info, url, '', status=self.STATUS_FAIL)
            return None, None

        self.append_model(data, Model.shareholder_info, url, r.text)
        return url, r.text

    def get_base_info(self, session, uuid):
        url = 'http://{host}/notice/notice/view?uuid={uuid}&tab=01'.format(
            host=self.host, uuid=uuid)

        session.headers["Host"] = self.host
        session.headers["Referer"] = "http://{host}/notice/search/ent_info_list".format(host=self.host)
        session.headers["DNT"] = "1"

        # 基本信息
        r = self.task_request_wscckey(session, session.get, url)
        if r is None:
            return None, None

        return url, r.text

    # 主要人员
    def get_key_person_info(self, session, uuid, data):
        url = 'http://{host}/notice/notice/moreMember?uuid={uuid}'.format(
            host=self.host, uuid=uuid)
        r = self.task_request_wscckey(session, session.get, url)
        if r is None:
            self.append_model(data, Model.key_person_info, url, '', status=self.STATUS_FAIL)
            return None, None

        self.append_model(data, Model.key_person_info, url, r.text)
        return url, r.text

    # 获得分支机构信息
    def get_branch_info(self, session, uuid, data):
        url = 'http://{host}/notice/notice/moreBranch?uuid={uuid}'.format(
            host=self.host, uuid=uuid)
        r = self.task_request_wscckey(session, session.get, url)
        if r is None:
            self.append_model(data, Model.branch_info, url, '', status=self.STATUS_FAIL)
            return None, None

        self.append_model(data, Model.branch_info, url, r.text)
        return url, r.text

    # 获得年报信息
    def get_annual_info(self, session, text, data):
        if text == '' or text is None:
            return

        try:
            jq = PyQuery(text, parser='html')
            item_list = jq.find('tr').items()
            for item in item_list:
                if item.text().find('年度报告') == -1:
                    continue

                year_info = item.find('td').eq(1).text()
                year_list = re.findall('\d+', year_info)
                if len(year_list) <= 0:
                    continue

                year = str(year_list[0])

                url = item.find('a').attr('href')
                if url is None or url == '':
                    continue

                if url.find('view_annual') == -1:
                    continue

                r = self.task_request_wscckey(session, session.get, url)
                if r is None:
                    self.append_model(data, Model.annual_info, url, '',
                                      status=self.STATUS_FAIL,
                                      year=year, classify=Model.type_detail)
                    continue
                self.append_model(data, Model.annual_info, url, r.text,
                                  year=year, classify=Model.type_detail)
        except Exception as e:
            self.log.exception(e)

    # 出资信息
    def get_contributive_info(self, session, text, data):
        pattern = 'ajaxReqInvestor\(\'(.*?)\'\)'
        try:
            jq = PyQuery(text, parser='html')
            table_g = jq.find('#layout-01_01_02').find('.content1').find('.tableG')
            item_list = table_g.find('tr')

            # 遍历每一项
            for index in xrange(1, item_list.length - 1):

                text = item_list.eq(index).find('a').attr('onclick')
                if text is None or text == '':
                    continue

                search_list = re.findall(pattern, text)
                if len(search_list) <= 0:
                    continue

                url = 'http://{host}/notice/notice/view_investor?uuid={uuid}'.format(
                    host=self.host, uuid=search_list[0])

                r = self.task_request_wscckey(session, session.get, url)
                if r is None:
                    self.append_model(data, Model.contributive_info, url, '',
                                      status=self.STATUS_FAIL,
                                      classify=Model.type_detail)
                    continue
                self.append_model(data, Model.contributive_info, url, r.text,
                                  classify=Model.type_detail)
        except Exception as e:
            self.log.exception(e)

    def task_request_wscckey(self, session, requester, url,
                             retry=3,
                             **kwargs):
        r = self.task_request(session, requester, url,
                              retry=retry,
                              **kwargs)
        if r is None:
            return None

        if util.judge_feature(r.text):
            self.log.error('出现验证码拦截页面: url = {url}'.format(url=url))
            self.report_session_proxy(session)
            return None

        search_list = self.wscckey_regex.findall(r.text)
        if len(search_list) <= 0:
            return r

        return self.task_request(session, requester, search_list[0],
                                 retry=retry,
                                 **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        # 保存企业名称
        data_list = []
        for item in param_list:
            try:
                uuid = item.get('uuid', None)
                if uuid is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                search_name = item.get('search_name', None)
                if search_name is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                # 基本信息 出资信息
                url, base_text = self.get_base_info(session, uuid)
                if url is None:
                    continue

                if '链接已经超时失效' in base_text:
                    self.log.error('链接失效: search_name = {search_name}'.format(search_name=search_name))
                    continue

                # 获得公司名称
                company = self.__get_company_name(base_text)
                if company is None or company == '':
                    self.log.error('公司名称解析失败..uuid = {uuid}'.format(
                        uuid=uuid))
                    continue

                # 建立数据模型
                data = self.get_model(company, seed, search_name, self.province)

                # 存储数据
                self.append_model(data, Model.base_info, url, base_text)

                # 出资信息
                self.append_model(data, Model.contributive_info, url, base_text)

                # 变更信息
                self.append_model(data, Model.change_info, url, base_text)

                # 主要人员
                self.get_key_person_info(session, uuid, data)

                # 分支机构
                self.get_branch_info(session, uuid, data)

                # 出资信息
                self.get_contributive_info(session, base_text, data)

                # 股东信息
                url, text = self.get_shareholder_info(session, uuid, data)

                # 年报信息
                self.get_annual_info(session, text, data)

                data_list.append(data)
            except Exception as e:
                self.log.exception(e)

        return self.sent_to_target(data_list)
