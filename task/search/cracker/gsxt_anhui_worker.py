#!/usr/bin/env python
# -*- coding:utf-8 -*-
import re
import time

from pyquery import PyQuery

from base.gsxt_base_worker import GsxtBaseWorker
from common import util
from common.global_field import Model

'''
1. 搜索没有结果判断功能添加
2. 包含出资信息 有出资详情
3. 包含年报信息
4. 添加完统计信息
5. 添加完成拓扑信息 已经check
6. 完成列表页名称提取
'''


class GsxtAnHuiWorker(GsxtBaseWorker):
    def __init__(self, **kwargs):
        GsxtBaseWorker.__init__(self, **kwargs)
        self.invest_pattern = 'seeInvest\(\'(.*?)\',\'.*?\'\)'
        self.invest_search_obj = re.compile(self.invest_pattern)
        self.proxy_type = self.PROXY_TYPE_STATIC

    @staticmethod
    def find_selector(selector, _id):
        if selector is None or selector == '':
            return None

        seed_code = None
        name_text = selector.find('.gggscpnametext').find('.tongyi').text()
        if name_text is not None:
            name_text = name_text.replace(u':',u'：')
            part = name_text.split('：')
            if len(part) >= 2:
                seed_code = part[1]

        title = selector.find('.gggscpnametitle')
        company = title.find('.qiyeEntName').text()
        if company is None:
            return None

        search_name = company.replace(' ', '')
        if search_name == '':
            return None

        status = title.find('.qiyezhuangtai.fillet').text()
        description = title.find('#{_id}'.format(_id=_id)).text()

        param = {'id': _id, 'search_name': search_name}
        if status is not None and status != '':
            param['status'] = status
        if description is not None and description != '':
            param['description'] = description

        # 统一社会信用号
        if seed_code is not None and seed_code.strip() != '':
            param['unified_social_credit_code'] = seed_code

        return param

    # 需要存入无搜索结果
    def get_search_list_html(self, keyword, session):
        param_list = []
        try:
            content = self.get_search_list_content(keyword, session)
            if content is None:
                return param_list, self.SEARCH_ERROR

            if content.find('查询条件中含有非法字符') != -1:
                self.log.warn('company = {company} 查询条件中含有非法字符'.format(
                    company=keyword))
                return param_list, self.SEARCH_KEYWORD_INVALID

            if content.find('查询条件长度不能小于2个字符且不能大于60个字符') != -1:
                self.log.warn('company = {company} 查询条件长度不能小于2个字符且不能大于60个字符'.format(
                    company=keyword))
                return param_list, self.SEARCH_KEYWORD_INVALID

            jq = PyQuery(content, parser='html')
            tip = jq.find('#searchtipsu1').find('span').eq(1).text()
            if tip == '0':
                return param_list, self.SEARCH_NOTHING_FIND

            item_list = jq.find('#gggscpnamebox').items()
            for item in item_list:
                _id = item.attr("data-label")
                if _id is None or _id == '':
                    continue

                param = self.find_selector(item, _id)
                if param is None:
                    continue

                param_list.append(param)

        except Exception as e:
            self.log.exception(e)
            return param_list, self.SEARCH_ERROR

        return param_list, self.SEARCH_SUCCESS if len(param_list) > 0 else self.SEARCH_ERROR

    @staticmethod
    def __get_company_name(text):
        jq = PyQuery(text, parser='html')
        return jq.find('#zhizhao').find('.xinxi').find('tr').eq(0).find('td').eq(1).find('span').text().strip()

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
    def get_base_info(self, session, i_d):
        # 获取基础信息
        url = 'http://{host}/business/JCXX.jspx?id={ID}&date={date}'.format(
            host=self.host, ID=i_d, date=util.get_gm_other_time())
        base_info = self.filter_request(session, session.get, url)
        if base_info is None:
            self.log.error('获取基础信息页面失败...')
            return None, None

        return url, base_info.text

    # 出资信息总页码数
    def get_page_num(self, text, tag):
        try:
            jq = PyQuery(text, parser='html')
            page_text = jq.find(tag).find('ul').find('li').eq(1).text()
            search_list = re.findall('\d+', page_text)
            if len(search_list) > 0:
                return int(search_list[0])
        except Exception as e:
            self.log.exception(e)
            return 0
        return 0

    # 出资信息
    def get_contributive_info(self, session, text, i_d, data):
        # 获得页码信息
        total_page_num = self.get_page_num(text, '#paging') + 1
        if total_page_num == 1:
            total_page_num += 1

        for page in xrange(1, total_page_num):
            try:
                url = 'http://{host}/business/QueryInvList.jspx?pno={page}&order=0&mainId={i_d}'.format(
                    host=self.host, page=page, i_d=i_d)

                r = self.filter_request(session, session.get, url)
                if r is None:
                    self.append_model(data, Model.contributive_info, url, '', status=self.STATUS_FAIL)
                    continue

                self.append_model(data, Model.contributive_info, url, r.text)

                index = 0
                tr_list = PyQuery(r.text, parser='html').find('.detailsListGDCZ').find('tr')
                item_list = tr_list.items()
                for item in item_list:
                    index += 1
                    onclick = item.find('a').attr('onclick')
                    if onclick is None or onclick == '':
                        continue
                    invest_list = self.invest_search_obj.findall(onclick.encode('utf-8'))
                    for invest_id in invest_list:
                        url = 'http://{host}/queryInvDetailAction.jspx?invId={i_d}'.format(
                            host=self.host, i_d=invest_id)
                        r = self.filter_request(session, session.get, url)
                        if r is None:
                            self.append_model(data, Model.contributive_info, url, '', status=self.STATUS_FAIL,
                                              classify=Model.type_detail)
                            continue

                        self.append_model(data, Model.contributive_info, url, r.text,
                                          classify=Model.type_detail)
            except Exception as e:
                self.log.exception(e)

    def get_key_person_info(self, session, i_d, data):
        url = 'http://{host}/business/loadMoreMainStaff.jspx?uuid={i_d}&order=1'.format(
            host=self.host, i_d=i_d)
        r = self.filter_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.key_person_info, url, '', status=self.STATUS_FAIL)
            return

        self.append_model(data, Model.key_person_info, url, r.text)

    def get_change_info(self, session, text, i_d, data):
        # 获得页码信息
        page_num = self.get_page_num(text, '#bgxx') + 1
        if page_num == 1:
            page_num += 1

        for i in xrange(1, page_num):
            try:
                url = 'http://{host}/business/QueryAltList.jspx?pno={page}&order=0&mainId={i_d}'.format(
                    host=self.host, page=i, i_d=i_d)
                r = self.filter_request(session, session.get, url)
                if r is None:
                    self.append_model(data, Model.change_info, url, '', status=self.STATUS_FAIL)
                    continue
                self.append_model(data, Model.change_info, url, r.text)
            except Exception as e:
                self.log.exception(e)

    def get_annual_info(self, session, text, data):
        for year, url in self.get_year_info_list(text):
            r = self.filter_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.annual_info, url, '',
                                  status=self.STATUS_FAIL,
                                  year=year, classify=Model.type_detail)
                continue
            self.append_model(data, Model.annual_info, url, r.text,
                              year=year, classify=Model.type_detail)

    def get_shareholder_info(self, session, text, i_d, data):
        # 获得页码信息
        page_num = self.get_page_num(text, '#invInfo') + 1
        if page_num == 1:
            page_num += 1

        for i in xrange(1, page_num):
            try:
                url = 'http://{host}/business/invInfoPage.jspx?pno={page}&order=0&mainId={i_d}'.format(
                    host=self.host, page=i, i_d=i_d)
                r = self.filter_request(session, session.get, url)
                if r is None:
                    self.append_model(data, Model.shareholder_info, url, '', status=self.STATUS_FAIL)
                    continue

                self.append_model(data, Model.shareholder_info, url, r.text)
            except Exception as e:
                self.log.exception(e)

    def get_branch_info(self, session, i_d, data):
        url = 'http://{host}//business/loadMoreChildEnt.jspx?uuid={i_d}&order=1'.format(
            host=self.host, i_d=i_d)
        r = self.filter_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.branch_info, url, '', status=self.STATUS_FAIL)
            return
        self.append_model(data, Model.branch_info, url, r.text)

    # 过滤验证码拦截页面
    def filter_request(self, session, requester, url, **kwargs):
        time.sleep(1)
        r = self.task_request(session, requester, url, **kwargs)
        if r is None:
            return None

        if util.judge_feature(r.text):
            self.log.error('出现验证码拦截页面: url = {url}'.format(url=url))
            self.report_session_proxy(session)
            return None

        return r

    def get_detail_html_list(self, seed, session, item_list):
        # 保存企业名称
        data_list = []
        for item in item_list:
            try:
                i_d = item.get('id', None)
                if i_d is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                search_name = item.get('search_name', None)
                if search_name is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                # 基本信息
                url, base_text = self.get_base_info(session, i_d)
                if url is None or base_text is None:
                    continue

                if base_text.find('无效用户') != -1:
                    self.log.error('数据获取失败, 无效用户, id = {id} {text}'.format(
                        id=i_d, text=base_text))
                    continue

                # 获得公司名称
                company = self.__get_company_name(base_text)
                if company is None or company == '':
                    self.log.error('公司名称解析失败..id = {id} text = {text}'.format(
                        id=i_d, text=base_text))
                    continue

                # 建立数据模型
                data = self.get_model(company, seed, search_name, self.province)

                # 存储数据
                self.append_model(data, Model.base_info, url, base_text)

                # 出资信息
                self.get_contributive_info(session, base_text, i_d, data)

                # 主要人员信息
                self.get_key_person_info(session, i_d, data)

                # 变更信息
                self.get_change_info(session, base_text, i_d, data)

                # 股东信息
                self.get_shareholder_info(session, base_text, i_d, data)

                # 分支机构
                self.get_branch_info(session, i_d, data)

                # 获得年报信息
                self.get_annual_info(session, base_text, data)

                data_list.append(data)
            except Exception as e:
                self.log.exception(e)

        return self.sent_to_target(data_list)
