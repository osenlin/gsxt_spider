# !/usr/bin/env python
# -*- coding:utf-8 -*-
import json
import re

from pyquery import PyQuery

from base.gsxt_base_worker import GsxtBaseWorker
from common import util
from common.global_field import Model

'''
1. 搜索没有结果判断功能添加
2. 完成年报抓取
3. 出资信息已抓取  出资信息没有详情页
4. 添加统计信息
5. 拓扑信息添加完成
6. 完成列表页名称提取
'''


class GsxtGuiZhouWorker(GsxtBaseWorker):
    def __init__(self, **kwargs):
        GsxtBaseWorker.__init__(self, **kwargs)

    def get_search_list_html(self, keyword, session):
        param_list = []
        try:
            url = 'http://{host}'.format(host=self.host)
            json_data, content = self.get_captcha_geetest_full(url, '#q', '#search_s', keyword,
                                                               '#list > div:nth-child(1) > dt > a',
                                                               success="#list")
            if content is None:
                return param_list, self.SEARCH_ERROR

            # 增加无搜索结果过滤
            if content.find('您输入的查询条件未查询到相关记录') != -1:
                return param_list, self.SEARCH_NOTHING_FIND
            # javascript:showDetail\("(.*?)","(.*?)","(.*?)","(.*?)","(.*?)","(.*?)"\);
            pattern = 'javascript:showDetail\("(.*?)","(.*?)","(.*?)","(.*?)","(.*?)","(.*?)"\);'
            regex = re.compile(pattern)

            div_list = PyQuery(content, parser='html').find('dl#list.list').find('div').items()
            for item in div_list:
                a_href = item.find('dt').find('a').attr('onclick')
                search_list = regex.findall(a_href)
                if len(search_list) <= 0:
                    continue

                param = {
                    'nbxh': search_list[0][0],
                    'qymc': search_list[0][1],
                    'zch': search_list[0][2],
                    'ztlx': search_list[0][3],
                    'qylx': search_list[0][4],
                    'search_name': search_list[0][1],
                }

                seed_code = item.find('dd').find('span').eq(0).text()
                if seed_code is not None and seed_code.strip() != '':
                    param['unified_social_credit_code'] = seed_code

                param_list.append(param)

            # 解析cookies
            if len(param_list) > 0:
                cookies = json_data.get('cookies')
                if isinstance(cookies, list):
                    for cookie in cookies:
                        session.cookies[cookie['name']] = cookie['value']

        except Exception as e:
            self.log.exception(e)
            return param_list, self.SEARCH_ERROR

        return param_list, self.SEARCH_SUCCESS if len(param_list) > 0 else self.SEARCH_ERROR

    def __get_company_name(self, text):
        try:
            result_json = json.loads(text)
            company = result_json['data'][0]['qymc']
            return company
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

    def get_sczt_path(self, ztlx, qylx):
        path = ""
        if ztlx == "1":
            if qylx.find("12") == 0:
                path = "gfgs"
            elif qylx.find("1") == 0:
                path = "nzgs"
            elif qylx.find("2") == 0:
                path = "nzgsfgs"
            elif qylx.find("3") == 0:
                path = "nzqyfr"
            elif qylx == "4310":
                path = "nzyyfz"
            elif (qylx == "4000") or qylx.find("41") == 0 or (qylx.find("42") == 0) or (
                        qylx.find("43") == 0) or (
                        qylx.find("44") == 0) or (qylx.find("46") == 0) or (qylx.find("47") == 0):
                path = "nzyy"
            elif qylx.find("453") == 0:
                path = "nzhh"
            elif qylx == "4540":
                path = "grdzgs"
            elif qylx.find("455") == 0:
                path = "nzhhfz"
            elif qylx == "4560":
                path = "grdzfzjg"
            elif (qylx.find("50") == 0) or (qylx.find("51") == 0) or \
                    (qylx.find("52") == 0) or (qylx.find("53") == 0) or \
                    (qylx.find("60") == 0) or (qylx.find("61") == 0) or \
                    (qylx.find("62") == 0) or (qylx.find("63") == 0):
                path = "wstz"
            elif (((qylx.find("58") == 0) or (qylx.find("68") == 0)
                   or (qylx.find("70") == 0) or (qylx.find("71") == 0)
                   or (qylx == '7310') or (qylx == "7390"))
                  and (qylx != "5840") and (qylx != "6840")):
                path = "wstzfz"
            elif (qylx.find("54") == 0) or (qylx.find("64") == 0):
                path = "wzhh"
            elif (qylx.find("5840") == 0) or (qylx.find("6840") == 0):
                path = "wzhhfz"
            elif qylx == "7200":
                path = "czdbjg"
            elif qylx == "7300":
                path = "wgqycsjyhd"
            elif qylx == "9100":
                path = "nmzyhzs"
            elif qylx == "9200":
                path = "nmzyhzsfz"
        elif ztlx == "2":
            path = "gtgsh"
        return path

    # 基本信息
    def get_base_info(self, session, url, nbxh):
        # 获取基础信息
        base_info_data = {
            'c': '0',
            'nbxh': nbxh,
            't': '5',
        }

        base_info = self.task_request(session, session.post, url=url, data=base_info_data)

        if base_info is None:
            self.log.error('获取基础信息页面失败...')
            return None
        # base_info data 为空,那么执行下面
        data_json = json.loads(base_info.text)
        if data_json is None:
            return None

        data_list = data_json.get('data')
        if not isinstance(data_list, list):
            return None

        if len(data_list) <= 0:
            return None

        return base_info.text
        # 下面这个也为空,则返回base_info 不然就返回其他东西

    # 主要人员
    def get_key_person_info(self, session, url, post_data, data):
        r = self.task_request(session, session.post, url=url, data=post_data)
        if r is None:
            self.append_model(data, Model.key_person_info, url + '#' + Model.key_person_info, '',
                              status=self.STATUS_FAIL,
                              post_data=post_data)
            return
        self.append_model(data, Model.key_person_info, url + '#' + Model.key_person_info, r.text,
                          post_data=post_data)

    # 变更信息
    def get_change_info(self, session, url, post_data, data):
        r = self.task_request(session, session.post, url=url, data=post_data)
        if r is None:
            self.append_model(data, Model.change_info, url + '#' + Model.change_info, '',
                              status=self.STATUS_FAIL,
                              post_data=post_data)
            return

        self.append_model(data, Model.change_info, url + '#' + Model.change_info, r.text,
                          post_data=post_data)

    # 获取年报信息
    def get_annual_info(self, session, url, post_data, data):
        r = self.task_request(session, session.post, url, data=post_data)
        if r is None:
            return

        json_data = util.json_loads(r.text)
        if json_data is None:
            return

        data_list = json_data.get('data', None)
        if data_list is None:
            return

        url = 'http://{host}/2016/grdzgs/query!searchNbxx.shtml'.format(host=self.host)
        for item in data_list:
            year = item.get('nd', None)
            if year is None:
                continue
            lsh = item.get('lsh', None)
            if lsh is None:
                continue
            nbxh = item.get('nbxh', None)
            if nbxh is None:
                continue
            param_list = [
                {
                    'c': '0',
                    't': '67',
                    'nbxh': nbxh,
                    'lsh': lsh,
                },
                {
                    'c': '0',
                    't': '68',
                    'nbxh': nbxh,
                    'lsh': lsh,
                },
                {
                    'c': '0',
                    't': '14',
                    'nbxh': nbxh,
                    'lsh': lsh,
                },
                {
                    'c': '0',
                    't': '16',
                    'nbxh': nbxh,
                    'lsh': lsh,
                },
                {
                    'c': '0',
                    't': '15',
                    'nbxh': nbxh,
                    'lsh': lsh,
                },
                {
                    'c': '0',
                    't': '18',
                    'nbxh': nbxh,
                    'lsh': lsh,
                },
                {
                    'c': '0',
                    't': '24',
                    'nbxh': nbxh,
                    'lsh': lsh,
                },
                {
                    'c': '0',
                    't': '41',
                    'nbxh': nbxh,
                    'lsh': lsh,
                },
                {
                    'c': '0',
                    't': '19',
                    'nbxh': nbxh,
                    'lsh': lsh,
                },
                {
                    'c': '0',
                    't': '39',
                    'nbxh': nbxh,
                    'lsh': lsh,
                },
            ]
            for param in param_list:
                r = self.task_request(session, session.post, url, data=param)
                if r is None:
                    self.append_model(data, Model.annual_info, url + '#' + Model.annual_info, '',
                                      status=self.STATUS_FAIL,
                                      year=year,
                                      post_data=param, classify=Model.type_detail)
                    continue
                self.append_model(data, Model.annual_info, url + '#' + Model.annual_info, r.text,
                                  year=year,
                                  post_data=param,
                                  classify=Model.type_detail)

    def get_shareholder_info(self, session, url, post_data, data):
        try:
            r = self.task_request(session, session.post, url=url, data=post_data)
            if r is None:
                self.append_model(data, Model.shareholder_info, url + '#' + Model.shareholder_info, '',
                                  status=self.STATUS_FAIL,
                                  post_data=post_data)
                return

            self.append_model(data, Model.shareholder_info, url + '#' + Model.shareholder_info, r.text,
                              post_data=post_data)
        except Exception as e:
            self.log.exception(e)

    def get_branch_info(self, session, url, post_data, data):
        r = self.task_request(session, session.post, url=url, data=post_data)
        if r is None:
            self.append_model(data, Model.branch_info, url + '#' + Model.branch_info, '',
                              status=self.STATUS_FAIL,
                              post_data=post_data)
            return
        self.append_model(data, Model.branch_info, url + '#' + Model.branch_info, r.text,
                          post_data=post_data)

    def get_contributive_info(self, session, url, post_data, data):
        try:
            r = self.task_request(session, session.post, url=url, data=post_data)
            if r is None:
                self.append_model(data, Model.contributive_info, url + '#' + Model.contributive_info, '',
                                  status=self.STATUS_FAIL,
                                  post_data=post_data)
                return
            self.append_model(data, Model.contributive_info, url + '#' + Model.contributive_info, r.text,
                              post_data=post_data)

            json_data = util.json_loads(r.text)
            if json_data is None:
                return

            array_data = json_data.get('data', None)
            if array_data is None:
                return

            detail_url = 'http://{host}/2016/frame/query!searchTzr.shtml'.format(host=self.host)
            for index, data_item in enumerate(array_data):
                nbxh = data_item.get('nbxh', None)
                czmc = data_item.get('czmc', None)
                post_item = {
                    'c': 2,
                    't': 4,
                    'nbxh': nbxh,
                    'czmc': czmc,
                }
                r = self.task_request(session, session.post, url=detail_url, data=post_item)
                if r is None:
                    continue
                self.append_model(data, Model.contributive_info, detail_url, r.text,
                                  post_data=post_item,
                                  classify=Model.type_detail)
        except Exception as e:
            self.log.exception(e)

    def get_detail_html_list(self, seed, session, param_list):
        # 保存企业名称
        data_list = []
        for item in param_list:
            try:

                nbxh = item.get('nbxh', None)
                if nbxh is None:
                    self.log.error('参数存储异常: item = {item}'.format(item=item))
                    continue

                search_name = item.get('search_name', None)
                if search_name is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                ztlx = item.get('ztlx', None)
                if ztlx is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                qylx = item.get('qylx', None)
                if qylx is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                flag = self.get_sczt_path(ztlx, qylx)

                url = 'http://{host}/2016/{flag}/query!searchData.shtml'.format(
                    host=self.host, flag=flag)

                session.headers['Host'] = self.host
                session.headers['Accept'] = 'application/json, text/javascript, */*'
                session.headers['Accept-Language'] = 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3'
                session.headers['Accept-Encoding'] = 'gzip, deflate'
                session.headers['Content-Type'] = 'application/x-www-form-urlencoded'
                session.headers['X-Requested-With'] = 'XMLHttpRequest'
                session.headers[
                    'Referer'] = 'http://{host}/2016/{flag}/jcxx_1.jsp?k={nbxh}&ztlx={ztlx}&qylx={qylx}&a_iframe=jcxx_1'.format(
                    host=self.host, flag=flag, nbxh=nbxh, ztlx=ztlx, qylx=qylx)
                session.headers['Connection'] = 'keep-alive'
                # session.headers[
                #     'Cookie'] = 'UM_distinctid=15ab624b74d5ab-087147752ed8e6-1d3a6853-fa000-15ab624b74e452; Hm_lvt_cdb4bc83287f8c1282df45ed61c4eac9=1499133460,1499830591,1500100264,1500451692; _gscu_1078311511=00813999bxziyw80; _gscbrs_1078311511=1; JSESSIONID=3LSmZ2db1ppLBnyBfFsvv3Q1qGFnVzHVjTn5DvtX5hGFpkDnxQJ2!1578889163!-459977327; CNZZDATA2123887=cnzz_eid%3D1494418206-1483699047-%26ntime%3D1500941221; SERVERID=a8996613bfda32ff76278a7c0597f7d3|1500945971|1500945948'
                # 基本信息

                # 在哪里 base_info_data 个体户和工商用户不同啊
                #
                base_text = self.get_base_info(session, url, nbxh)
                if base_text is None:
                    continue

                # 获得公司名称
                company = self.__get_company_name(base_text)
                if company is None or company == '':
                    self.log.error('公司名称解析失败..item = {item} {text}'.format(
                        text=base_text, item=item))
                    continue

                # 建立数据模型
                data = self.get_model(company, seed, search_name, self.province)

                # 存储数据
                self.append_model(data, Model.base_info, url + '#' + Model.base_info, base_text,
                                  post_data=item)

                member_data = {
                    'c': '0',
                    'nbxh': nbxh,
                    't': '8'
                }

                branch_data = {
                    'c': '0',
                    'nbxh': nbxh,
                    't': '9'
                }

                contributive_data = {
                    'c': '2',
                    'nbxh': nbxh,
                    't': '3'
                }
                change_data = {
                    'c': '0',
                    'nbxh': nbxh,
                    't': '3'
                }
                investor_data = {
                    'c': '0',
                    'nbxh': nbxh,
                    't': '40'
                }
                nb_data = {
                    'c': '0',
                    't': '13',
                    'nbxh': nbxh,
                }

                # 主要人员信息
                self.get_key_person_info(session, url, member_data, data)

                # 分支机构
                self.get_branch_info(session, url, branch_data, data)

                # 变更信息
                self.get_change_info(session, url, change_data, data)

                # 股东信息
                self.get_shareholder_info(session, url, investor_data, data)

                # 出资信息
                self.get_contributive_info(session, url, contributive_data, data)

                # 年报信息
                self.get_annual_info(session, url, nb_data, data)

                data_list.append(data)
            except Exception as e:
                self.log.exception(e)
        return self.sent_to_target(data_list)
