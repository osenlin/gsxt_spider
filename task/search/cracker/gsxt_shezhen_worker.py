#!/usr/bin/env python
# coding: utf-8
import json
import re
import time

from pyquery import PyQuery

from base.gsxt_base_worker import GsxtBaseWorker
from common.global_field import Model

'''
1. 搜索没有结果判断功能添加
2. 包含出资信息 有出资详情, 全部在json里面
3. 包含年报信息
4. 添加完成统计信息
5. 完成列表页名称提取

解析问题:
变更信息数目不正确 有可能是翻页逻辑有BUG
深圳市钜盛华股份有限公司
'''


class GsxtShenZhenWorker(GsxtBaseWorker):
    def __init__(self, **kwargs):
        GsxtBaseWorker.__init__(self, **kwargs)
        self.ssl_type = 'https'
        self.proxy_type = self.PROXY_TYPE_DYNAMIC

    @staticmethod
    def get_company_name_old(text):
        if text == '':
            return None
        jq = PyQuery(text, parser='html')
        company = jq.find('#CompanyInfo_EntName1').text()
        if company is None or company.strip() == '':
            company = jq.find('#wucYYZZInfo_qymc').text()
        if company is None:
            return None

        return company.strip()

    # 出资信息.attr('data')
    def get_contributive_info_detail_old(self, session, text):
        page_detail = []

        if text == '':
            return page_detail

        jq = PyQuery(text, parser='html')
        item_list = jq.find('.item_box').find('td').items()
        for item in item_list:
            if item.attr('data') == 'gdjczInfo':
                info = item.attr('info')
                url = 'https://www.szcredit.org.cn/GJQYCredit/GSZJGSPTS/Detail/wucTZRInfoDetail.aspx?id=gdjczInfo&recordid={id}'.format(
                    id=info)
                r = self.task_request(session, session.get, url)
                if r is None:
                    page_detail.append({
                        'url': url,
                        'status': self.STATUS_FAIL,
                        'text': '',
                    })
                    continue

                page_detail.append({
                    'url': url,
                    'status': self.STATUS_SUCCESS,
                    'text': r.text,
                })
        return page_detail

    def get_turn_page_info_old(self, text):
        if text == '':
            return None, None, None, None

        jq = PyQuery(text, parser='html')
        __VIEWSTATEGENERATOR = jq.find('#__VIEWSTATEGENERATOR').attr('value')
        if __VIEWSTATEGENERATOR == '' or __VIEWSTATEGENERATOR is None:
            return None, None, None, None
        __EVENTVALIDATION = jq.find('#__EVENTVALIDATION').attr('value')
        if __EVENTVALIDATION == '' or __EVENTVALIDATION is None:
            return None, None, None, None
        __VIEWSTATE = jq.find('#__VIEWSTATE').attr('value')
        if __VIEWSTATE == '' or __VIEWSTATE is None:
            return None, None, None, None

        txtrid = jq.find('#txtrid').attr('value')
        if txtrid == '' or txtrid is None:
            return None, None, None, None

        return txtrid, __VIEWSTATE, __VIEWSTATEGENERATOR, __EVENTVALIDATION

    def get_split_page_info_old(self, text):
        text_list = text.split('</div>')
        if len(text_list) <= 0:
            return None, None

        __VIEWSTATE = None
        __EVENTVALIDATION = None

        text = text_list[-1].strip()
        text_list = text.split('|')
        length = len(text_list)

        for index, item in enumerate(text_list):
            if item == '__VIEWSTATE':
                if index + 1 < length:
                    __VIEWSTATE = text_list[index + 1]
            if item == '__EVENTVALIDATION':
                if index + 1 < length:
                    __EVENTVALIDATION = text_list[index + 1]

        return __VIEWSTATE, __EVENTVALIDATION

    # 变更信息
    def get_field_turn_page_info_old(self, session, url, text, manager, arget, type_info, func=None):

        page_list = []
        page_detail = []

        if text == '':
            return page_list, page_detail

        # 获取表单信息
        txtrid, __VIEWSTATE, __VIEWSTATEGENERATOR, __EVENTVALIDATION = self.get_turn_page_info_old(text)
        if txtrid is None or __VIEWSTATE is None or __VIEWSTATEGENERATOR is None or __EVENTVALIDATION is None:
            return page_list, page_detail

        page = 1
        page_num = 1
        while page <= page_num:
            if page == 1:
                ScriptManager1 = manager + 'Label' + str(page)
                __EVENTTARGET = arget + 'Label' + str(page)
            else:
                ScriptManager1 = manager + 'lbtnNextPage'
                __EVENTTARGET = arget + 'lbtnNextPage'
            post_data = {
                'ScriptManager1': ScriptManager1,
                'TakeEntID': '',
                'txtrid': txtrid,
                'fromMail': '',
                'layerhid': 0,
                '__EVENTTARGET': __EVENTTARGET,
                '__EVENTARGUMENT': '',
                '__EVENTVALIDATION': __EVENTVALIDATION,
                '__VIEWSTATE': __VIEWSTATE,
                '__VIEWSTATEGENERATOR': __VIEWSTATEGENERATOR,
                '__ASYNCPOST': 'true',
            }
            page += 1
            r = self.task_request(session, session.post, url, data=post_data, timeout=60)
            if r is None:
                page_list.append({
                    'url': url + '#' + type_info,
                    'status': self.STATUS_FAIL,
                    'text': '',
                    'post_data': json.dumps(post_data),
                })
                continue

            if r.text.find('236|error|500|回发或回调参数无效') != -1:
                self.log.error('翻页错误: {text}'.format(text=r.text))
                break

            page_list.append({
                'url': url + '#' + type_info,
                'status': self.STATUS_SUCCESS,
                'text': r.text,
                'post_data': json.dumps(post_data),
            })

            # 获得详情信息
            if func is not None:
                page_detail.extend(func(session, r.text))

            # 获得总页码数
            pattern = '共查询到.*?条信息.*?共(.*?)页'
            search_list = re.findall(pattern, text.encode('utf-8'))
            if len(search_list) <= 0:
                break

            page_num = int(search_list[0].strip())
            if page <= page_num:
                # 获取表单信息
                __VIEWSTATE, __EVENTVALIDATION = self.get_split_page_info_old(r.text)
                if __VIEWSTATE is None or __EVENTVALIDATION is None:
                    break
        return page_list, page_detail

    # 获取年报信息
    def get_annual_info_old(self, session, data, text):
        if text == '':
            return
        jq = PyQuery(text, parser='html')
        item_list = jq.find('div.item_box').find('tr').items()
        for item in item_list:
            if item.text().find('年度报告') == -1:
                continue

            year_info = item.find('td').eq(1).text()
            year_list = re.findall('\d+', year_info)
            if len(year_list) <= 0:
                continue

            year = str(year_list[0])

            href = item.find('a').attr('href')
            if href is None or href == '':
                continue
            url = 'https://www.szcredit.org.cn/GJQYCredit/GSZJGSPTS/' + href
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.annual_info, url, '',
                                  status=self.STATUS_FAIL,
                                  year=year, classify=Model.type_detail)
                continue
            self.append_model(data, Model.annual_info, url, r.text, year=year, classify=Model.type_detail)

    # 旧版方式
    def get_detail_html_list_old(self, seed, search_name, session, detail_url):

        # 深圳站点封禁严重 休眠10s
        time.sleep(5)

        # 重新申请https代理
        session.proxies = self.get_random_proxy()
        session.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, sdch, br',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Host': 'www.szcredit.org.cn',
            'Referer': 'http://gd.gsxt.gov.cn/aiccips/CheckEntContext/showCheck.html',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': self.get_user_agent(),
        }
        # 获得基本信息
        detail_url = detail_url.strip()
        r = self.task_request(session, session.get, detail_url, timeout=120)
        if r is None:
            return None

        # self.log.info(r.text)
        if '非法访问' in r.text:
            self.log.warn('非法访问, 被封禁..')
            return None

        if '您的查询过于频繁' in r.text:
            self.log.warn('您的查询过于频繁, 被封禁..')
            return None

        if '请您输入正确的参数进行查询' in r.text:
            self.log.warn('请您输入正确的参数进行查询, 被封禁..')
            return None

        # self.log.info(r.text)
        # html = gzip.decompress(r.text)
        try:
            text = unicode(r.content, 'gbk').encode('utf-8')
        except Exception as e:
            self.log.error('编码格式转换失败')
            self.log.exception(e)
            text = r.text
        # text = r.text.decode('gb2312').encode("utf-8")

        company = self.get_company_name_old(text)
        if company is None or company == '':
            self.log.error('公司名称解析失败: search_name = {search_name}'.format(search_name=search_name))
            return None

        data = self.get_model(company, seed, search_name, self.province)

        # 基础信息
        self.append_model(data, Model.base_info, detail_url + '#' + Model.base_info, text)

        # 出资信息
        page_list, page_detail = self.get_field_turn_page_info_old(session, detail_url, text,
                                                                   'UpdatePanel2|wucTZRInfo$TurnPageBar1$',
                                                                   'wucTZRInfo$TurnPageBar1$',
                                                                   Model.contributive_info,
                                                                   self.get_contributive_info_detail_old)
        # 如果没有解析到多页出资信息 则从基本信息页面进行解析
        if len(page_list) <= 0:
            self.append_model(data, Model.contributive_info, detail_url + '#' + Model.contributive_info, text)
            page_detail = self.get_contributive_info_detail_old(session, text)
        else:
            self.append_model_list(data, Model.contributive_info, page_list)
        if len(page_detail) > 0:
            self.append_model_list(data, Model.contributive_info, page_detail, classify=Model.type_detail)

        # 主要人员
        self.append_model(data, Model.key_person_info, detail_url + '#' + Model.key_person_info, text)

        # 分支结构
        self.append_model(data, Model.branch_info, detail_url + '#' + Model.branch_info, text)

        # 股东信息
        # self.append_model(data, Model.shareholder_info, detail_url + '#' + Model.shareholder_info, text)
        page_list, page_detail = self.get_field_turn_page_info_old(session, detail_url, text,
                                                                   'UpdatePanel1|wucGDJCZXX$TurnPageBar1$',
                                                                   'wucGDJCZXX$TurnPageBar1$',
                                                                   Model.shareholder_info)
        if len(page_list) <= 0:
            self.append_model(data, Model.shareholder_info, detail_url + '#' + Model.shareholder_info, text)
        else:
            self.append_model_list(data, Model.shareholder_info, page_list)

        # 变更信息
        page_list, page_detail = self.get_field_turn_page_info_old(session, detail_url, text,
                                                                   'UpdatePanel5|wucAlterItem$TurnPageBar1$',
                                                                   'wucAlterItem$TurnPageBar1$',
                                                                   Model.change_info)
        if len(page_list) <= 0:
            self.append_model(data, Model.change_info, detail_url + '#' + Model.change_info, text)
        else:
            self.append_model_list(data, Model.change_info, page_list)

        # 股权变更
        page_list, page_detail = self.get_field_turn_page_info_old(session, detail_url, text,
                                                                   'UpdatePanel3|wucGQBGXX$TurnPageBar1$',
                                                                   'wucGQBGXX$TurnPageBar1$',
                                                                   Model.change_shareholding_info)
        if len(page_list) <= 0:
            self.append_model(data, Model.change_shareholding_info, detail_url + '#' + Model.change_shareholding_info,
                              text)
        else:
            self.append_model_list(data, Model.change_shareholding_info, page_list)

        # todo 股权出质登记信息 未抓取

        # 年报信息
        self.get_annual_info_old(session, data, text)

        return data
