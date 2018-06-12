#!/usr/bin/env python
# -*- coding:utf-8 -*-
import re
import time

from pyquery import PyQuery

from base.gsxt_base_worker import GsxtBaseWorker
from common import util
from common.global_field import Model
from config.conf import encry_zj_conf

'''
验证码无法识别
1. 没有抓年报
2. 没有抓出资信息
4. 已添加统计信息
'''


class GsxtZheJiangWorker(GsxtBaseWorker):
    def __init__(self, **kwargs):
        GsxtBaseWorker.__init__(self, **kwargs)
        self.proxy_type = self.PROXY_TYPE_DYNAMIC

    def get_search_list_html(self, keyword, session):
        param_list = []
        try:
            session.headers = {
                "Host": "gsxt.zjaic.gov.cn",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:50.0) Gecko/20100101 Firefox/50.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Referer": "http://zj.gsxt.gov.cn/client/entsearch/list?isOpanomaly=&pubType=1&searchKeyWord=0B46FE9E9DBAF27F&currentPage=2",
            }

            # 先获得加密关键字信息
            script = "strEnc('{keyword}','a','b','c')".format(keyword=keyword)
            search_key_word = self.get_encry_pripid_detail(encry_zj_conf['url'], script)
            if search_key_word is None:
                return param_list, self.SEARCH_ERROR

            search_url = 'http://{host}/client/entsearch/list?isOpanomaly=&pubType=1&searchKeyWord={searchkey}'.format(
                host=self.host, searchkey=search_key_word)

            r = self.task_request(session, session.get, url=search_url)
            if r is None:
                return param_list, self.SEARCH_ERROR

            content = r.text
            if content is None:
                return param_list, self.SEARCH_ERROR

            # 这个IP已经被封禁
            if util.judge_feature(content):
                self.report_session_proxy(session)
                return param_list, self.SEARCH_ERROR

            jq = PyQuery(content, parser='html')

            # 先判断有多少数据
            if jq.find('h3.title').find('span.light').text() == '0':
                return param_list, self.SEARCH_NOTHING_FIND

            item_list = jq.find('div.mod.enterprise-info').find('.enterprise-info-list').find('li').items()
            for item in item_list:
                a_info = item.find('a')
                if a_info is None or len(a_info) <= 0:
                    continue

                href = a_info.attr('href')
                if href is None or href == '':
                    continue

                a_info.find('span[class=tip]').remove()
                a_info.find('i').remove()
                company = a_info.text()
                search_name = company.replace(' ', '')
                if search_name == '':
                    return None

                param = {
                    'Referer': search_url,
                    'href': href,
                    'search_name': search_name,
                }

                seed_code = None
                code_text = item.find('.item-text').find('.code').text()
                if code_text is not None and code_text.strip() != '':
                    part = code_text.split(':')
                    if len(part) >= 2:
                        seed_code = part[1]

                if seed_code is not None and seed_code.strip() != '':
                    param['unified_social_credit_code'] = seed_code

                param_list.append(param)
        except Exception as e:
            self.log.exception(e)
            return param_list, self.SEARCH_ERROR

        return param_list, self.SEARCH_SUCCESS if len(param_list) > 0 else self.SEARCH_ERROR

    # 过滤验证码拦截页面
    def filter_request(self, session, requester, url, retry=3, **kwargs):
        time.sleep(1)
        r = self.task_request(session, requester, url, retry=retry, **kwargs)
        if r is None:
            return None

        if util.judge_feature(r.text):
            self.log.error('出现验证码拦截页面: url = {url}'.format(url=url))
            self.report_session_proxy(session)
            return None

        return r

    def __get_company_name(self, text):
        try:
            result = PyQuery(text, parser='html').find('span[class="fl mr5"]').eq(0).text()
            return result
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
    def get_base_info(self, session, url):
        # 获取基础信息
        base_info = self.filter_request(session, session.get, url)
        if base_info is None:
            self.log.error('获取基础信息页面失败...')
            return None
        return base_info.text

    # 出资信息
    def get_contributive_info(self, session, url, data, total_data):
        try:
            r = self.filter_request(session, session.post, url=url, data=data)
            if r is None:
                self.append_model(total_data, Model.contributive_info, url, '',
                                  status=self.STATUS_FAIL,
                                  post_data=data)
                return False
            self.append_model(total_data, Model.contributive_info, url, r.text, post_data=data)

            self.get_contributive_info_detail(session, r.text, total_data)
        except Exception as e:
            self.log.exception(e)
            return False
        return True

    # 出资信息详情页
    def get_contributive_info_detail(self, session, text, total_data):
        detail_text = util.json_loads(text)
        detail_data = detail_text.get('data')
        for data in detail_data:
            data_id = data.get('id')
            url = 'http://{host}/midinv/findMidInvById?midInvId={midInvId}'.format(host=self.host, midInvId=data_id)
            r = self.filter_request(session, session.get, url=url)
            if r is not None:
                self.append_model(total_data, Model.contributive_info, url, r.text, classify=Model.type_detail)
            else:
                self.append_model(total_data, Model.contributive_info, url, '',
                                  status=self.STATUS_FAIL,
                                  classify=Model.type_detail)

    # 主要人员
    def get_key_person_info(self, session, url, data, totol_data):
        r = self.filter_request(session, session.post, url=url, data=data)
        if r is None:
            self.append_model(totol_data, Model.key_person_info, url, '',
                              status=self.STATUS_FAIL,
                              post_data=data)
            return False
        self.append_model(totol_data, Model.key_person_info, url, r.text, post_data=data)
        return True

    # 变更信息
    def get_change_info(self, session, url, data, totol_data):
        r = self.filter_request(session, session.post, url=url, data=data)
        if r is None:
            self.append_model(totol_data, Model.change_info, url, '',
                              status=self.STATUS_FAIL,
                              post_data=data)
            return False
        self.append_model(totol_data, Model.change_info, url, r.text, post_data=data)
        return True

    # def get_annual_info(self, session, text, data, totol_data):
    #     # 暂时不处理 跟解析沟通一下
    #     for year, url in self.get_year_info_list(text):
    #         r = self.filter_request(session, session.get, url)
    #         if r is None:
    #             continue
    #         self.append_model(totol_data, Model.annual_info, url, r.text, year=year, classify=Model.type_detail)

    # 分支机构
    def get_branch_info(self, session, url, data, totol_data):
        r = self.filter_request(session, session.post, url=url, data=data)
        if r is None:
            self.append_model(totol_data, Model.branch_info, url, '',
                              status=self.STATUS_FAIL,
                              post_data=data)
            return False
        self.append_model(totol_data, Model.branch_info, url, r.text, post_data=data)
        return True

    # 股东信息
    def get_shareholder_info(self, session, url, total_data):
        r = self.filter_request(session, session.get, url=url)
        if r is None:
            self.append_model(total_data, Model.shareholder_info, url, '',
                              status=self.STATUS_FAIL)
            return False
        self.append_model(total_data, Model.shareholder_info, url, r.text)
        return True

    def get_annual_info(self, session, href, annual_url, data, total_data):
        encry_pri_pid = util.get_match_value('docId=', '&classFlag', href)
        if encry_pri_pid is None:
            return False
        r = self.filter_request(session, session.post, url=annual_url, data=data)
        if r is None:
            return False
        r_text = util.json_loads(r.text)
        if r_text is None:
            return False
        r_data = r_text.get('data')
        if r_data is None:
            return False
        for data in r_data:
            year = data.get('year')
            year_id = data.get('anCheID')
            if year is None or year_id is None:
                continue

            post_data1 = {'anCheID': year_id}
            post_data2 = {'start': '0', 'length': '100', 'params[anCheID]': year_id}
            # 基本信息
            base_info_url = 'http://{host}/entinfo/yrinfo?year={year}&encryPriPID={encry_pri_pid}&classFlag=1'.format(
                host=self.host, year=year, encry_pri_pid=encry_pri_pid)
            r = self.filter_request(session, session.get, base_info_url)
            if r is not None:
                self.append_model(total_data, Model.annual_info, base_info_url, r.text,
                                  year=year,
                                  classify=Model.type_detail)
            else:
                self.append_model(total_data, Model.annual_info, base_info_url, '',
                                  status=self.STATUS_FAIL,
                                  year=year,
                                  classify=Model.type_detail)
                return False

            # 网站信息
            web_info_url = 'http://{host}/pub/WebsiteInfo/publist.json?_t={rand}'.format(
                host=self.host, rand=util.get_time_stamp())
            r = self.filter_request(session, session.post, web_info_url,
                                    data=post_data1)
            if r is not None:
                self.append_model(total_data, Model.annual_info, web_info_url, r.text, post_data=post_data1,
                                  year=year,
                                  classify=Model.type_detail)
            else:
                self.append_model(total_data, Model.annual_info, web_info_url, '', post_data=post_data1,
                                  status=self.STATUS_FAIL,
                                  year=year,
                                  classify=Model.type_detail)
                return False

            # 股东信息
            shareholder_info_url = 'http://{host}/pub/subcapitalInfo/publist.json?_t={rand}'.format(
                host=self.host, rand=util.get_time_stamp())
            r = self.filter_request(session, session.post, shareholder_info_url,
                                    data=post_data2)
            if r is not None:
                self.append_model(total_data, Model.annual_info, shareholder_info_url, r.text, post_data=post_data2,
                                  year=year,
                                  classify=Model.type_detail)
            else:
                self.append_model(total_data, Model.annual_info, shareholder_info_url, '', post_data=post_data2,
                                  status=self.STATUS_FAIL,
                                  year=year,
                                  classify=Model.type_detail)
                return False

            # 对外投资
            investment_info_url = 'http://{host}/pub/forinvestMentInfo/publist.json?_t={rand}'.format(
                host=self.host, rand=util.get_time_stamp())
            r = self.filter_request(session, session.post, investment_info_url,
                                    data=post_data1)
            if r is not None:
                self.append_model(total_data, Model.annual_info, investment_info_url, r.text, post_data=post_data1,
                                  year=year,
                                  classify=Model.type_detail)
            else:
                self.append_model(total_data, Model.annual_info, investment_info_url, '', post_data=post_data1,
                                  status=self.STATUS_FAIL,
                                  year=year,
                                  classify=Model.type_detail)
                return False

            # 资产状况 在基本信息里

            # 担保信息
            assurance_info_url = 'http://{host}/pub/GuaranteeInfo/publist.json?_t={rand}'.format(
                host=self.host, rand=util.get_time_stamp())
            r = self.filter_request(session, session.post, assurance_info_url,
                                    data=post_data2)
            if r is not None:
                self.append_model(total_data, Model.annual_info, assurance_info_url, r.text, post_data=post_data2,
                                  year=year,
                                  classify=Model.type_detail)
            else:
                self.append_model(total_data, Model.annual_info, assurance_info_url, '', post_data=post_data2,
                                  status=self.STATUS_FAIL,
                                  year=year,
                                  classify=Model.type_detail)
                return False

            # 股权变更
            change_info_url = 'http://{host}/pub/alterStockInfo/publist.json?_t={rand}'.format(
                host=self.host, rand=util.get_time_stamp())
            r = self.filter_request(session, session.post, change_info_url,
                                    data=post_data2)
            if r is not None:
                self.append_model(total_data, Model.annual_info, change_info_url, r.text, post_data=post_data2,
                                  year=year,
                                  classify=Model.type_detail)
            else:
                self.append_model(total_data, Model.annual_info, change_info_url, '', post_data=post_data2,
                                  status=self.STATUS_FAIL,
                                  year=year,
                                  classify=Model.type_detail)
                return False

            # 修改记录
            amendant_info_url = 'http://{host}/pub/updateinfo/publist.json?_t={rand}'.format(
                host=self.host, rand=util.get_time_stamp())
            r = self.filter_request(session, session.post, amendant_info_url,
                                    data=post_data2)
            if r is not None:
                self.append_model(total_data, Model.annual_info, amendant_info_url, r.text, post_data=post_data2,
                                  year=year,
                                  classify=Model.type_detail)
            else:
                self.append_model(total_data, Model.annual_info, amendant_info_url, '', post_data=post_data2,
                                  status=self.STATUS_FAIL,
                                  year=year,
                                  classify=Model.type_detail)
                return False
        return True

    def get_detail_html_list(self, seed, session, param_list):
        # 保存企业名称
        data_list = []
        for item in param_list:
            try:
                href = item.get('href', None)
                referer = item.get('Referer', None)
                if href is None or referer is None:
                    self.log.error('参数存储异常: item = {item}'.format(item=item))
                    continue

                url = 'http://{host}/client/entsearch/{href}'.format(host=self.host, href=href)
                search_name = item.get('search_name', None)
                if search_name is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                session.headers = {
                    "Host": self.host,
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:50.0) Gecko/20100101 Firefox/50.0",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive",
                    "Referer": referer,
                }
                # 基本信息
                base_text = self.get_base_info(session, url)
                if base_text is None:
                    continue
                if base_text.strip() == '':
                    continue

                # 页面不正确
                pri_pid = PyQuery(base_text, parser='html').find('#priPID').attr('value')
                if pri_pid is None:
                    continue

                # 获得公司名称
                company = self.__get_company_name(base_text)
                if company is None or company == '':
                    self.log.error('公司名称解析失败..item = {item} {text}'.format(
                        text=base_text, item=item))
                    continue
                # 建立数据模型
                data = self.get_model(company, seed, search_name, self.province)
                # yearreport_url = 'http://{host}/entinfo/list.json?_t={rand}'.format(
                #    host=self.host, rand=util.get_time_stamp())
                # yearreport_data = {
                #     'params[priPID]': pri_pid
                # }
                contributive_url = 'http://{host}/midinv/list.json?_t={rand}'.format(
                    host=self.host, rand=util.get_time_stamp())
                contributive_data = {
                    'params[priPID]': pri_pid,
                    'start': '0',
                    'length': '1000'
                }
                member_url = 'http://{host}/midmember/list.json?_t={rand}'.format(
                    host=self.host, rand=util.get_time_stamp())
                member_data = {
                    'priPID': pri_pid,
                }
                branch_url = 'http://{host}/midbranch/list.json?_t={rand}'.format(
                    host=self.host, rand=util.get_time_stamp())
                branch_data = {
                    'priPID': pri_pid,
                }
                change_url = 'http://{host}/midaltitem/list.json?_t={rand}'.format(
                    host=self.host, rand=util.get_time_stamp())
                change_data = {
                    'params[priPID]': pri_pid,
                    'start': '0',
                    'length': '1000'
                }
                shareholder_url = 'http://{host}/im/pub/investalter/investmentListJSON?_t={rand}&pageNum=0&' \
                                  'priPID={priPID}&length={length}&params%5BpageNum%5D=0'. \
                    format(host=self.host, rand=util.get_time_stamp(), priPID=pri_pid, length='1000')

                annual_url = 'http://{host}/entinfo/list.json?_t={rand}'.format(
                    host=self.host, rand=util.get_time_stamp())
                annual_data = {
                    'params[priPID]': pri_pid,
                    'start': '0',
                    'length': '10'
                }

                # 存储数据
                self.append_model(data, Model.base_info, url, base_text)

                time.sleep(0.5)
                # 出资信息
                if not self.get_contributive_info(session, contributive_url, contributive_data, data):
                    self.log.warn('出资信息抓取失败....pripid = {pripid}'.format(pripid=pri_pid))
                    continue
                time.sleep(0.5)
                # 主要人员信息
                if not self.get_key_person_info(session, member_url, member_data, data):
                    self.log.warn('主要人员抓取失败....pripid = {pripid}'.format(pripid=pri_pid))
                    continue
                time.sleep(0.5)
                # 分支机构
                if not self.get_branch_info(session, branch_url, branch_data, data):
                    self.log.warn('分支机构抓取失败....pripid = {pripid}'.format(pripid=pri_pid))
                    continue
                time.sleep(0.5)
                # 变更信息
                if not self.get_change_info(session, change_url, change_data, data):
                    self.log.warn('变更信息抓取失败....pripid = {pripid}'.format(pripid=pri_pid))
                    continue
                time.sleep(0.5)
                # 股东信息
                if not self.get_shareholder_info(session, shareholder_url, data):
                    self.log.warn('股东信息抓取失败....pripid = {pripid}'.format(pripid=pri_pid))
                    continue
                time.sleep(0.5)
                # 获得年报信息
                if not self.get_annual_info(session, href, annual_url, annual_data, data):
                    self.log.warn('年报信息抓取失败....pripid = {pripid}'.format(pripid=pri_pid))
                    continue

                data_list.append(data)
            except Exception as e:
                self.log.exception(e)

        return self.sent_to_target(data_list)
