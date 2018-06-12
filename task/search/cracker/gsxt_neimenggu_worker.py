#!/usr/bin/env python
# -*- coding:utf-8 -*-
import re

from pyquery import PyQuery

from base.gsxt_base_worker import GsxtBaseWorker
from common import util
from common.global_field import Model

'''
1. 搜索没有结果判断功能添加
2. 包含出资信息 有出资详情, 全部在json里面
3. 包含年报信息
4. 添加完成统计信息, (广东 和 内蒙古可能会有很多年报抓取失败, 因为年报股东信息总页码信息)
5. 添加完成拓扑信息
6. 完成列表页名称提取
'''


class GsxtNeiMengGuWorker(GsxtBaseWorker):
    def __init__(self, **kwargs):
        GsxtBaseWorker.__init__(self, **kwargs)
        self.url = 'http://{host}'.format(host=self.host)
        self.sub = ''

    def get_search_list_html(self, keyword, session):
        param_list = []
        try:
            # return [{'url': 'https://www.szcredit.org.cn/GJQYCredit/GSZJGSPTS/QYGS.aspx?rid=440301001012006060501478',
            #          'search_name': '招商银行股份有限公司',
            #          'unified_social_credit_code': '9144030010001686XA'}], self.SEARCH_SUCCESS

            content = self.get_captcha_geetest(self.url, '#content', '#search', keyword, '.mianBodyStyle')
            if content is None:
                return param_list, self.SEARCH_ERROR

            jq = PyQuery(content, parser='html')
            if jq.find('div.textStyle').find('span').text() == '0':
                return param_list, self.SEARCH_NOTHING_FIND

            item_list = jq.find('.mianBodyStyle').find('.clickStyle').items()
            for item in item_list:
                a_href = item.find('a')
                href = a_href.attr('href')
                if href is None or href == '':
                    continue

                if href.find('..') != -1:
                    url = 'http://{host}{sub}'.format(host=self.host, sub=self.sub) + href[2:]
                else:
                    url = href

                company = a_href.text()
                if company is None or company == '':
                    continue

                search_name = company.replace(' ', '')
                if search_name == '':
                    continue

                seed_code = None
                code_text = item.find('.textStyle').find('td').eq(0).text()
                if code_text is not None and code_text.strip() != '':
                    part = code_text.split('：')
                    if len(part) >= 2:
                        seed_code = part[1]

                param = {
                    'url': url.strip(),
                    'search_name': search_name,
                }

                if seed_code is not None and seed_code.strip() != '':
                    param['unified_social_credit_code'] = seed_code

                param_list.append(param)
        except Exception as e:
            self.log.exception(e)
            return param_list, self.SEARCH_ERROR

        return param_list, self.SEARCH_SUCCESS if len(param_list) > 0 else self.SEARCH_ERROR

    @staticmethod
    def get_company_name_new(text):
        if text == '':
            return None
        jq = PyQuery(text, parser='html')
        company = jq.find('.conpanyInfo').find('li').eq(0).find('span').eq(0).text()
        return company.strip()

    @staticmethod
    def get_key_word_new(text):
        if text == '':
            return

        jq = PyQuery(text, parser='html')
        ent_no = jq.find('#entNo').attr('value')
        ent_type = jq.find('#entType').attr('value')
        reg_org = jq.find('#regOrg').attr('value')

        key_info = {
            'entNo': ent_no,
            'entType': ent_type,
            'regOrg': reg_org
        }
        return key_info

    # 出资信息
    def get_contributive_info_new(self, host, session, data, key_info):

        url = 'http://{host}{sub}//invInfo/invInfoList?pageNo=1&entNo={entNo}&regOrg={regOrg}'.format(
            entNo=key_info['entNo'], regOrg=key_info['regOrg'], host=host, sub=self.sub)
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.contributive_info, url, '', status=self.STATUS_FAIL)
            return

        json_data = util.json_loads(r.text)
        if json_data is None:
            self.append_model(data, Model.contributive_info, url, r.text, status=self.STATUS_FAIL)
            return

        json_list = json_data.get('list', None)
        if json_list is None:
            self.append_model(data, Model.contributive_info, url, r.text, status=self.STATUS_FAIL)
            return

        total_page = json_list.get('totalPages', -1)
        if total_page < 0:
            self.append_model(data, Model.contributive_info, url, r.text, status=self.STATUS_NOT_EXIST)
            return

        # 数据可用,先发送
        self.append_model(data, Model.contributive_info, url, r.text)

        # for page in xrange(2, total_page + 1):
        #     url = 'http://{host}{sub}//invInfo/invInfoList?pageNo={page}&entNo={entNo}&regOrg={regOrg}'.format(
        #         entNo=key_info['entNo'], regOrg=key_info['regOrg'], page=page, host=host, sub=self.sub)
        #     r = self.task_request(session, session.get, url)
        #     if r is None:
        #         self.append_model(data, Model.contributive_info, url, '', status=self.STATUS_FAIL)
        #         continue
        #
        #     self.append_model(data, Model.contributive_info, url, r.text)

        json_list = json_list.get('list', None)
        if json_list is None:
            return

        for index, item in enumerate(json_list):
            inv_no = item.get('invNo', None)
            ent_no = item.get('entNo', None)

            url = 'http://{host}{sub}/GSpublicity/invInfoDetails.html?invNo={invNo}&entNo={entNo}&regOrg={regOrg}'.format(
                host=host, invNo=inv_no, entNo=ent_no, regOrg=key_info['regOrg'], sub=self.sub)
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.contributive_info, url, '',
                                  status=self.STATUS_FAIL,
                                  classify=Model.type_detail)
                continue

            self.append_model(data, Model.contributive_info, url, r.text,
                              classify=Model.type_detail)

    # 获得主要人员信息
    def get_key_person_info_new(self, host, session, data, key_info):
        url = 'http://{host}{sub}//vip/vipList.html?entNo={entNo}&regOrg={regOrg}'.format(
            host=host, entNo=key_info['entNo'], regOrg=key_info['regOrg'], sub=self.sub)
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.key_person_info, url, '', status=self.STATUS_FAIL)
            return

        self.append_model(data, Model.key_person_info, url, r.text)

    # 获得分支机构信息
    def get_branch_info_new(self, host, session, data, key_info):
        url = 'http://{host}{sub}//CipBraInfo/cipBraInfoList.html?entNo={entNo}&regOrg={regOrg}'.format(
            host=host, entNo=key_info['entNo'], regOrg=key_info['regOrg'], sub=self.sub)
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.branch_info, url, '', status=self.STATUS_FAIL)
            return
        self.append_model(data, Model.branch_info, url, r.text)

    # 股东信息
    def get_shareholder_info_new(self, host, session, data, key_info):
        url = 'http://{host}{sub}//REIInvInfo/REIInvInfoList?pageNo=1&entNo={entNo}&regOrg={regOrg}'.format(
            entNo=key_info['entNo'], regOrg=key_info['regOrg'], host=host, sub=self.sub)
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.shareholder_info, url, '', status=self.STATUS_FAIL)
            return

        json_data = util.json_loads(r.text)
        if json_data is None:
            self.append_model(data, Model.shareholder_info, url, r.text, status=self.STATUS_FAIL)
            return

        json_list = json_data.get('list', None)
        if json_list is None:
            self.append_model(data, Model.shareholder_info, url, r.text, status=self.STATUS_FAIL)
            return

        total_page = json_list.get('totalPages', -1)
        if total_page < 0:
            self.append_model(data, Model.shareholder_info, url, r.text, status=self.STATUS_FAIL)
            return

        # 数据可用,先发送
        self.append_model(data, Model.shareholder_info, url, r.text)

        # for page in xrange(2, total_page + 1):
        #     url = 'http://{host}{sub}//REIInvInfo/REIInvInfoList?pageNo={page}&entNo={entNo}&regOrg={regOrg}'.format(
        #         entNo=key_info['entNo'], regOrg=key_info['regOrg'], page=page, host=host, sub=self.sub)
        #     r = self.task_request(session, session.get, url)
        #     if r is None:
        #         self.append_model(data, Model.shareholder_info, url, '', status=self.STATUS_FAIL)
        #         continue
        #
        #     self.append_model(data, Model.shareholder_info, url, r.text)

    # 获取年报信息
    def get_annual_info_new(self, host, session, data, text, key_info):
        search_list = re.findall('onclick="reportview\((.*?),(.*?)\)"', text)
        for year, enttype in search_list:
            url = 'http://{host}{sub}//BusinessAnnals/annualReport.html?entNo={entNo}&reportYear={year}&entityType={enttype}&regOrg={regOrg}'.format(
                host=host, entNo=key_info['entNo'], year=year, regOrg=key_info['regOrg'], sub=self.sub, enttype=enttype)
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.annual_info, url, '',
                                  status=self.STATUS_FAIL,
                                  year=year, classify=Model.type_detail)
                continue
            self.append_model(data, Model.annual_info, url, r.text,
                              year=year, classify=Model.type_detail)

            # 最多翻10页
            for page in xrange(1, 10):
                url = 'http://{host}{sub}//capital/capitalList?pageNo={page}&entNo={entNo}&regOrg={regOrg}&reportYear={year}'.format(
                    host=host, page=page, entNo=key_info['entNo'], year=year, regOrg=key_info['regOrg'], sub=self.sub)
                r = self.task_request(session, session.get, url)
                if r is None:
                    self.append_model(data, Model.annual_info, url, '',
                                      status=self.STATUS_FAIL,
                                      year=year, classify=Model.type_detail)
                    continue

                if len(r.text) <= 15:
                    break

                self.append_model(data, Model.annual_info, url, r.text,
                                  year=year, classify=Model.type_detail)

    # 变更信息
    def get_change_info_new(self, host, session, data, key_info):
        url = 'http://{host}{sub}//EntChaInfo/EntChatInfoList?pageNo=1&entNo={entNo}&regOrg={regOrg}'.format(
            entNo=key_info['entNo'], regOrg=key_info['regOrg'], host=host, sub=self.sub)
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.change_info, url, '', status=self.STATUS_FAIL)
            return

        json_data = util.json_loads(r.text)
        if json_data is None:
            self.append_model(data, Model.change_info, url, r.text, status=self.STATUS_FAIL)
            return

        json_list = json_data.get('list', None)
        if json_list is None:
            self.append_model(data, Model.change_info, url, r.text, status=self.STATUS_FAIL)
            return

        total_page = json_list.get('totalPages', -1)
        if total_page < 0:
            self.append_model(data, Model.change_info, url, r.text, status=self.STATUS_NOT_EXIST)
            return

        # 数据可用,先发送
        self.append_model(data, Model.change_info, url, r.text)
        # for page in xrange(2, total_page + 1):
        #     url = 'http://{host}{sub}//EntChaInfo/EntChatInfoList?pageNo={page}&entNo={entNo}&regOrg={regOrg}'.format(
        #         entNo=key_info['entNo'], regOrg=key_info['regOrg'], page=page, host=host, sub=self.sub)
        #     r = self.task_request(session, session.get, url)
        #     if r is None:
        #         self.append_model(data, Model.change_info, url, '', status=self.STATUS_FAIL)
        #         continue
        #
        #     self.append_model(data, Model.change_info, url, r.text)

    # 股权出质细节
    def get_equity_pledged_info_detail(self, page_text, session, data, param_dict, host):
        json_data = util.json_loads(page_text)
        if json_data is None:
            return

        json_list = json_data.get('list', None)
        if json_list is None:
            return

        json_list_list = json_list.get('list', None)
        if json_list_list is None:
            return

        if param_dict.get('regOrg') is None:
            return

        for index, item in enumerate(json_list_list):
            sto_ple_no = item.get('stoPleNo', None)
            if sto_ple_no is None:
                return

            url = 'http://{host}{sub}/GSpublicity/curStoPleXQ.html?stoPleNo={stoPleNo}&type1&bizSeq=null&regOrg={regOrg}'.format(
                host=host, sub=self.sub, stoPleNo=sto_ple_no, regOrg=param_dict['regOrg']
            )
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.equity_pledged_info, url, '', status=self.STATUS_FAIL,
                                  classify=Model.type_detail)
                continue
            self.append_model(data, Model.equity_pledged_info, url, r.text, classify=Model.type_detail)

    # 工商详情页统一接口
    def task_request_detail(self, info, page_text, session, data, param_dict, host):
        if info == Model.equity_pledged_info:  # 股权出质登记信息详情
            self.get_equity_pledged_info_detail(page_text, session, data, param_dict, host)

    # 工商请求统一方法
    def task_request_go(self, info, param_dict, session, data, host):
        # 20个元素
        query_dict = {
            Model.equity_pledged_info: 'StoPleInfo/StoPleInfoList',  # 股权出质登记信息
            Model.change_shareholding_info: 'GuQuan/GuQuanChangeList',  # 股权变更信息
        }

        ent_no = param_dict.get('entNo', None)
        reg_org = param_dict.get('regOrg', None)
        ent_type = param_dict.get('entType', None)
        if ent_no is None or reg_org is None or ent_type is None:
            self.log.error("参数报错,entNo和regOrg")

        query = query_dict.get(info)
        url = 'http://{host}{sub}/{query}?pageNo=1&entNo={entNo}&regOrg={regOrg}'.format(host=host,
                                                                                         sub=self.sub,
                                                                                         entNo=ent_no,
                                                                                         regOrg=reg_org,
                                                                                         query=query)
        r = self.task_request(session, session.get, url)
        if r is None:
            return

        ###进行翻页
        json_data = util.json_loads(r.text)
        if json_data is None:
            self.append_model(data, info, url, r.text, status=self.STATUS_FAIL)
            return

        json_list = json_data.get('list', None)
        if json_list is None:
            self.append_model(data, info, url, r.text, status=self.STATUS_FAIL)
            return

        total_page = json_list.get('totalPages', -1)
        if total_page < 0:
            self.append_model(data, info, url, r.text, status=self.STATUS_NOT_EXIST)
            return

        # 数据可用,先发送
        self.append_model(data, info, url, r.text)
        self.task_request_detail(info, r.text, session, data, param_dict, host)

        ###进行翻页
        # for page in xrange(2, total_page + 1):
        #     url = 'http://{host}{sub}//{query}?pageNo={page}&entNo={entNo}&regOrg={regOrg}'.format(
        #         entNo=ent_no, regOrg=reg_org, page=page, host=host, sub=self.sub, query=query)
        #     r = self.task_request(session, session.get, url)
        #     if r is None:
        #         self.append_model(data, info, url, '', status=self.STATUS_FAIL)
        #         continue
        #
        #     self.append_model(data, info, url, r.text)
        #     self.task_request_detail(info, r.text, session, data, param_dict, host)

    # 新版方式
    def get_detail_html_list_new(self, host, search_name, seed, session, detail_url):

        session.headers["Host"] = host
        session.headers["Referer"] = "http://{host}/aiccips/CheckEntContext/showCheck.html".format(host=self.host)
        session.headers["DNT"] = "1"

        # 获得基本信息
        r = self.task_request(session, session.get, detail_url)
        if r is None:
            return None

        company = self.get_company_name_new(r.text)
        if company is None or company == '':
            return None

        data = self.get_model(company, seed, search_name, self.province)

        # 保存基础信息
        self.append_model(data, Model.base_info, detail_url, r.text)

        # 获得关键字段
        key_info = self.get_key_word_new(r.text)

        # 变更信息
        self.get_change_info_new(host, session, data, key_info)

        # 获得出资信息
        self.get_contributive_info_new(host, session, data, key_info)

        # 获得主要人员信息
        self.get_key_person_info_new(host, session, data, key_info)

        # 获得分支机构信息
        self.get_branch_info_new(host, session, data, key_info)

        # 获得股东信息
        self.get_shareholder_info_new(host, session, data, key_info)

        # 年报信息
        self.get_annual_info_new(host, session, data, r.text, key_info)

        # 股权出质登记信息
        self.task_request_go(Model.equity_pledged_info, key_info, session, data, host)

        # 股权变更信息
        self.task_request_go(Model.change_shareholding_info, key_info, session, data, host)

        return data

    def get_detail_html_list(self, seed, session, param_list):

        data_list = []
        for item in param_list:
            try:
                detail_url = item.get('url', None)
                if detail_url is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                search_name = item.get('search_name', None)
                if search_name is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                data = self.get_detail_html_list_new(self.host, search_name, seed, session, detail_url)
                if data is not None:
                    data_list.append(data)
            except Exception as e:
                self.log.exception(e)

        return self.sent_to_target(data_list)
