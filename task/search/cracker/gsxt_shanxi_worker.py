# -*- coding: utf-8 -*-
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
5. 添加完成拓扑信息
6. 完成列表页解析
7. 陕西无法访问 注册码解析功能未添加
'''


class GsxtShanXiWorker(GsxtBaseWorker):
    def __init__(self, **kwargs):
        GsxtBaseWorker.__init__(self, **kwargs)

    def get_search_list_html(self, keyword, session):
        param_list = []
        try:
            url = 'http://{host}/ztxy.do?method=index&random={rand}'.format(
                host=self.host, rand=util.get_time_stamp())
            content = self.get_captcha_geetest(url, '#entname', '#popup-submit', keyword, 'p.result_desc')
            if content is None:
                return param_list, self.SEARCH_ERROR

            # pripid,enttype,zt,type
            # openView('6100000000020342','11','K','1')
            jq = PyQuery(content, parser='html')
            if jq.find('p.result_desc').text().find('您搜索的条件无查询结果') != -1:
                return param_list, self.SEARCH_NOTHING_FIND

            pattern = 'openView\(\'(.*?)\',\'(.*?)\',\'(.*?)\',\'(.*?)\'\)'
            regex = re.compile(pattern)
            item_list = jq.find('.result_item').items()
            for item in item_list:
                onclick = item.attr('onclick')
                if onclick is None or onclick == '':
                    continue

                search_list = regex.findall(onclick)
                if len(search_list) <= 0:
                    continue

                company = item.find('#mySpan').attr('title')
                if company is None or company == '':
                    continue

                search_name = company.replace(' ', '')
                if search_name == '':
                    continue

                status = item.find('.status.diaoxiao').text()
                if status is None or status == '':
                    status = item.find('.status.cunxu').text()

                data = {
                    'pripid': search_list[0][0],
                    'enttype': search_list[0][1],
                    'zt': search_list[0][2],
                    'type': search_list[0][3],
                    'search_name': search_name,
                }
                if status is not None and status != '':
                    data['status'] = status

                param_list.append(data)
        except Exception as e:
            self.log.exception(e)
            return param_list, self.SEARCH_ERROR

        return param_list, self.SEARCH_SUCCESS if len(param_list) > 0 else self.SEARCH_ERROR

    # 基本信息
    def get_base_info(self, session, pri_pid):
        url = 'http://{host}/ztxy.do?method=qyinfo_jcxx&pripid={pripid}&random=201608111029'.format(
            host=self.host, pripid=pri_pid)
        r = self.task_request(session, session.get, url)
        if r is None:
            return None, None

        return url, r.text

    # 获取公司名称
    @staticmethod
    def __get_company_name(text):
        pattern = '<td.*?><strong>企业名称：</strong>(.*?)</td>'
        search_list = re.findall(pattern, text.encode('utf-8'))
        if len(search_list) > 0:
            return search_list[0].strip()

        pattern = '<td.*?><strong>名称：</strong>(.*?)</td>'
        search_list = re.findall(pattern, text.encode('utf-8'))
        if len(search_list) > 0:
            return search_list[0].strip()

        return None

    # 主要人员
    def get_key_person_info(self, session, pri_pid, data):
        url = 'http://{host}/ztxy.do?method=showAllzyry&maent.pripid={pripid}&random={rand}'.format(
            host=self.host, pripid=pri_pid, rand=util.get_time_stamp())

        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.key_person_info, url, '', status=self.STATUS_FAIL)
            return

        self.append_model(data, Model.key_person_info, url, r.text)

    # 分支机构
    def get_branch_info(self, session, pri_pid, data):
        url = 'http://{host}/ztxy.do?method=showAllfzjg&maent.pripid={pripid}&random={rand}'.format(
            host=self.host, pripid=pri_pid, rand=util.get_time_stamp())

        r = self.task_request(session, session.get, url)
        if r is None:
            self.append_model(data, Model.branch_info, url, '', status=self.STATUS_FAIL)
            return

        self.append_model(data, Model.branch_info, url, r.text)

    def get_contributive_info(self, session, base_text, data):
        pattern = 'showRyxx\(\'(.*?)\',\'(.*?)\',\'(.*?)\'\)'
        search_list = re.findall(pattern, base_text)
        length = len(search_list)
        if length <= 0:
            return

        for index, item in enumerate(search_list):
            url = 'http://{host}/ztxy.do?method=frInfoDetail&maent.xh={xh}&maent.pripid={pripid}&isck={issck}&random={rand}'.format(
                host=self.host, xh=item[0], pripid=item[1], issck=item[2], rand=util.get_time_stamp())
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.contributive_info, url, '',
                                  status=self.STATUS_FAIL,
                                  classify=Model.type_detail)
                continue

            self.append_model(data, Model.contributive_info, url, r.text,
                              classify=Model.type_detail)

    # 获得年报信息
    def get_annual_info(self, session, text, data):
        pattern = 'showNbDetail\(\'(.*?)\',\'(.*?)\'\);'
        search_list = re.findall(pattern, text)
        if len(search_list) <= 0:
            return

        for item in search_list:
            url = 'http://{host}/ztxy.do?method=qyinfo_nnbxx&pripid={pripid}&nd={year}&random={rand}'.format(
                host=self.host, pripid=item[0], year=item[1], rand=util.get_time_stamp())
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.annual_info, url, '',
                                  status=self.STATUS_FAIL,
                                  year=item[1], classify=Model.type_detail)
                continue
            self.append_model(data, Model.annual_info, url, r.text,
                              year=item[1],
                              classify=Model.type_detail)

    def get_detail_html_list(self, seed, session, param_list):
        data_list = []
        for item in param_list:
            try:
                pri_pid = item.get('pripid', None)
                if pri_pid is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                search_name = item.get('search_name', None)
                if search_name is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                url, base_text = self.get_base_info(session, pri_pid)
                if url is None or base_text is None:
                    continue

                # 获得公司名称
                company = self.__get_company_name(base_text)
                if company is None or company == '':
                    self.log.error('公司名称解析失败..item = {item} {text}'.format(
                        text=base_text, item=item))
                    continue

                # 建立数据模型
                data = self.get_model(company, seed, search_name, self.province)

                # 保存基础信息
                self.append_model(data, Model.base_info, url, base_text)
                self.append_model(data, Model.contributive_info, url, base_text)

                # 主要人员
                self.get_key_person_info(session, pri_pid, data)

                # 获得分支机构
                self.get_branch_info(session, pri_pid, data)

                # 获得出资信息
                self.get_contributive_info(session, base_text, data)

                # 获得年报信息
                self.get_annual_info(session, base_text, data)

                data_list.append(data)
            except Exception as e:
                self.log.exception(e)

        return self.sent_to_target(data_list)
