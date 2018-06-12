#!/usr/bin/env python
# -*- coding:utf-8 -*-
import re

from pyquery import PyQuery

from base.gsxt_base_worker import GsxtBaseWorker
from common import util
from common.global_field import Model

'''
1. 搜索没有结果判断功能添加
2. 包含出资信息 有出资详情页
3. 包含年报信息
4. 添加完成统计信息
5. 完成添加拓扑信息  已经check
6. 完成宁夏列表页抓取测试
7. 宁夏注册码解析未测试, 验证码无法识别
'''


class GsxtNingXiaWorker(GsxtBaseWorker):
    def __init__(self, **kwargs):
        GsxtBaseWorker.__init__(self, **kwargs)

    def get_search_list_html(self, keyword, session):
        param_list = []
        try:
            url = 'http://{host}'.format(host=self.host)
            content = self.get_captcha_geetest(url, '#selectValue', '#popup-submit',
                                               keyword, 'div.main_list_txt.w1003')
            if content is None:
                return param_list, self.SEARCH_ERROR

            jq = PyQuery(content, parser='html')
            if jq.find('.list_txt01').find('span').text() == '0':
                return param_list, self.SEARCH_NOTHING_FIND

            item_list = jq.find('div.main_list_txt.w1003').find('#qylist').find('li').items()
            for item in item_list:
                href_item = item.find('a')
                href = href_item.attr('href')
                if href is None or href == '':
                    continue
                if href == 'javascript:;':
                    continue

                company = href_item.text()
                if company is None or company == '':
                    continue

                search_name = company.replace(' ', '')
                if search_name == '':
                    continue

                param = {
                    'href': href,
                    'search_name': search_name,
                }
                seed_code = item.find('.list_infor.clearfix.mgt15').find('.fl').find('span').text()
                if seed_code is not None and seed_code.strip() != '':
                    param['unified_social_credit_code'] = seed_code

                param_list.append(param)
        except Exception as e:
            self.log.exception(e)
            return param_list, self.SEARCH_ERROR

        return param_list, self.SEARCH_SUCCESS if len(param_list) > 0 else self.SEARCH_ERROR

    # 获得基本信息
    def get_base_info(self, session, href):
        url = 'http://{host}/{href}'.format(host=self.host, href=href)
        r = self.task_request(session, session.get, url)
        if r is None:
            return None, None

        return url, r.text

    # 解析公司名称
    @staticmethod
    def get_company_name(text):
        pattern = '<span>企业名称：</span>(.*?)</td>'
        search_list = re.findall(pattern, text.encode('utf-8'))
        if len(search_list) > 0:
            return search_list[0].strip()
        return None

    # 解析参数
    def get_param_dict(self, url):
        'gsbaseInfoAction_baseInfo.action?qylx=1222&nbxh=64030020120328000046344&qylxFlag=1&zch=9164030059622766XM'
        href = url.split('?')[-1]
        param_list = href.split('&')
        param_dict = {}
        for item in param_list:
            key, value = item.split('=')
            param_dict[key] = value
        return param_dict

    # 获得股东信息
    def get_shareholder_info(self, session, param_dict, data):
        url = 'http://{host}/gsbaseInfoAction_gdczGtInfo.action?randomNum={rand}&nbxh={nbxh}&qylx={qylx}&menustring=1'.format(
            host=self.host, rand=util.get_random_num(), nbxh=param_dict['nbxh'], qylx=param_dict['qylx'])
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.shareholder_info, url, '', status=self.STATUS_FAIL)
            return

        self.append_model(data, Model.shareholder_info, url, r.text)

    # 变更信息
    def get_change_info(self, session, param_dict, data):
        url = 'http://{host}/gsbaseInfoAction_bgInfo.action?randomNum={rand}&nbxh={nbxh}&qylx={qylx}&menustring=1'.format(
            host=self.host, rand=util.get_random_num(), nbxh=param_dict['nbxh'], qylx=param_dict['qylx'])
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.change_info, url, '', status=self.STATUS_FAIL)
            return

        try:
            page_num = int(PyQuery(r.text, parser='html').find('#countPage').attr('value'))
        except Exception as e:
            self.log.exception(e)
            page_num = 1

        if page_num == 0:
            self.append_model(data, Model.change_info, url, r.text, status=self.STATUS_NOT_EXIST)
            return

        self.append_model(data, Model.change_info, url, r.text)

        for page in xrange(2, page_num + 1):
            url = 'http://{host}/gsbaseInfoAction_bgInfo.action?randomNum={rand}&nbxh={nbxh}&qylx={qylx}&menustring=1&currPage={page}'.format(
                host=self.host, rand=util.get_random_num(), nbxh=param_dict['nbxh'], qylx=param_dict['qylx'], page=page)
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.change_info, url, '', status=self.STATUS_FAIL)
                return
            self.append_model(data, Model.change_info, url, r.text)

    # 主要人员
    def get_key_person_info(self, session, param_dict, data):
        url = 'http://{host}/gsbaseInfoAction_zzryMoreInfo.action?nbxh={nbxh}'.format(
            host=self.host, rand=util.get_random_num(), nbxh=param_dict['nbxh'], qylx=param_dict['qylx'])
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.key_person_info, url, '', status=self.STATUS_FAIL)
            return

        self.append_model(data, Model.key_person_info, url, r.text)

    # 分支机构
    def get_branch_info(self, session, param_dict, data):
        url = 'http://{host}/gsbaseInfoAction_fzjgMoreInfo.action?nbxh={nbxh}'.format(
            host=self.host, nbxh=param_dict['nbxh'])
        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.branch_info, url, '', status=self.STATUS_FAIL)
            return

        self.append_model(data, Model.branch_info, url, r.text)

    # 出资详情
    def get_contributive_info_detail(self, session, text, data):
        # 'detail('20120711000127686','1222','股东及出资详细信息')'
        pattern = 'detail\(\'(.*?)\',\'(.*?)\',\'.*?\'\)'
        search_list = re.findall(pattern, text)
        if len(search_list) <= 0:
            return

        length = len(search_list)
        for index, item in enumerate(search_list):
            url = 'http://{host}/gsbaseInfoAction_gdczDetailInfo.action?xh={xh}&qylx={qylx}'.format(
                host=self.host, xh=item[0], qylx=item[1])
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.contributive_info, url, '',
                                  status=self.STATUS_FAIL,
                                  classify=Model.type_detail)
                continue

            self.append_model(data, Model.contributive_info, url, r.text,
                              classify=Model.type_detail)

    # 出资信息
    def get_contributive_info(self, session, param_dict, data):
        try:
            url = 'http://{host}/gsbaseInfoAction_gdczInfo.action?randomNum={rand}&nbxh={nbxh}&qylx={qylx}&menustring=1'.format(
                host=self.host, rand=util.get_random_num(), nbxh=param_dict['nbxh'], qylx=param_dict['qylx'])
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.contributive_info, url, '', status=self.STATUS_FAIL)
                return

            try:
                page_num = int(PyQuery(r.text, parser='html').find('#countPage').attr('value'))
            except Exception as e:
                self.log.exception(e)
                page_num = 1

            if page_num == 0:
                self.append_model(data, Model.contributive_info, url, r.text, status=self.STATUS_NOT_EXIST)
                return

            self.append_model(data, Model.contributive_info, url, r.text)

            # 获得出资详情
            self.get_contributive_info_detail(session, r.text, data)

            for page in xrange(2, page_num + 1):
                url = 'http://{host}/gsbaseInfoAction_gdczInfo.action?randomNum={rand}&nbxh={nbxh}&qylx={qylx}&menustring=1&currPage={page}'.format(
                    host=self.host, rand=util.get_random_num(), nbxh=param_dict['nbxh'], qylx=param_dict['qylx'],
                    page=page)
                r = self.task_request(session, session.get, url)
                if r is None:
                    self.append_model(data, Model.contributive_info, url, '', status=self.STATUS_FAIL)
                    return

                self.append_model(data, Model.contributive_info, url, r.text)

                # 获得出资详情
                self.get_contributive_info_detail(session, r.text, data)

        except Exception as e:
            self.log.exception(e)

    # 获得年报信息
    def get_annual_info(self, session, param_dict, data):
        url = 'http://{host}/gsbaseInfoAction_qynbInfo.action?randomNum={rand}&nbxh={nbxh}&qylx={qylx}&menustring=4'.format(
            host=self.host, rand=util.get_random_num(), nbxh=param_dict['nbxh'], qylx=param_dict['qylx'])
        r = self.task_request(session, session.get, url)
        if r is None:
            return

        pattern = 'qynbBase\(\'(.*?)\',\'(.*?)\',\'(.*?)\'\)'
        find_list = re.findall(pattern, r.text)
        if len(find_list) <= 0:
            return

        for nb_item in find_list:
            nbxh = nb_item[0]
            year = nb_item[1]
            qylx = nb_item[2]
            url = 'http://{host}/gsQynbAction_qynbBaseInfo.action?nbxh={nbxh}&anCheYear={year}&qylxFlag=2&qylx={qylx}'.format(
                host=self.host, nbxh=nbxh, year=year, qylx=qylx)
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.annual_info, url, '',
                                  status=self.STATUS_FAIL,
                                  year=year, classify=Model.type_detail)
                continue

            # 基本信息
            self.append_model(data, Model.annual_info, url, r.text,
                              year=year,
                              classify=Model.type_detail)
            # nbxh ,year,qylx
            # 年报其他信息抓取
            item_list = PyQuery(r.text, parser='html').find('iframe').items()
            for item in item_list:
                src = item.attr('src')
                if src is None or src == '':
                    continue
                url = 'http://{host}{src}'.format(host=self.host, src=src)
                r = self.task_request(session, session.get, url)
                if r is None:
                    self.append_model(data, Model.annual_info, url, '',
                                      status=self.STATUS_FAIL,
                                      year=year, classify=Model.type_detail)
                    continue
                self.append_model(data, Model.annual_info, url, r.text,
                                  year=year, classify=Model.type_detail)

                ###服务器翻页,怎么办
                jq = PyQuery(r.text, parser='html')
                somepagenum = jq.find('#countPage').attr('value')

                if somepagenum is not None:
                    if somepagenum > 1:
                        pagenum = int(somepagenum)
                        # print somepagenum
                        i = 2
                        while i <= pagenum:
                            url = 'http://{host}{src}&currPage={pagenum}'.format(host=self.host, src=src, pagenum=i)
                            # print url
                            i += 1
                            r_item = self.task_request(session, session.get, url)
                            if r_item is None:
                                self.append_model(data, Model.annual_info, url, '',
                                                  status=self.STATUS_FAIL,
                                                  year=year, classify=Model.type_detail)

                            self.append_model(data, Model.annual_info, url, r_item.text,
                                              year=year, classify=Model.type_detail)

    def get_detail_html_list(self, seed, session, param_list):

        data_list = []
        for item in param_list:
            try:
                href = item.get('href', None)
                if href is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                if href == 'javascript:;':
                    self.log.warn('href = {href}'.format(href=href))
                    continue

                search_name = item.get('search_name', None)
                if search_name is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                # 获取基本信息
                url, base_text = self.get_base_info(session, href)
                if url is None or base_text is None:
                    continue

                # 获得公司名称
                company = self.get_company_name(base_text)
                if company is None or company == '':
                    self.log.error('获取公司名称失败: item = {item}'.format(item=item))
                    continue

                # 建立数据模型
                data = self.get_model(company, seed, search_name, self.province)

                # 存储数据
                self.append_model(data, Model.base_info, url, base_text)

                # 解析参数
                param_dict = self.get_param_dict(url)

                # 股东信息
                self.get_shareholder_info(session, param_dict, data)

                # 变更信息
                self.get_change_info(session, param_dict, data)

                # 主要人员
                self.get_key_person_info(session, param_dict, data)

                # 分支机构
                self.get_branch_info(session, param_dict, data)

                # 出资信息
                self.get_contributive_info(session, param_dict, data)

                # 年报信息
                self.get_annual_info(session, param_dict, data)

                data_list.append(data)
            except Exception as e:
                self.log.exception(e)

        return self.sent_to_target(data_list)
