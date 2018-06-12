# -*- coding:utf-8 -*-
import re

from pyquery import PyQuery

from base.gsxt_base_worker import GsxtBaseWorker
from common.global_field import Model

'''
1. 搜索没有结果判断功能添加
2. 包含出资信息 出资信息在基本信息里面
3. 包含年报信息
4. 添加统计信息
5. 拓扑信息已添加 已完成check
6. 完成列表页名称提取
'''


class GsxtGanSuWorker(GsxtBaseWorker):
    def __init__(self, **kwargs):
        GsxtBaseWorker.__init__(self, **kwargs)

    def get_search_list_html(self, keyword, session):
        # keyword = '甘肃金隆宜路桥建设（集团）有限公司'
        # keyword = '甘肃中石油昆仑燃气有限公司'
        # return [{
        #     'pripid': '1310833',
        #     'entcate': 'compan',
        # }], self.SEARCH_SUCCESS

        param_list = []
        try:
            # keyword = 'fdsfasljfjsadlf'
            url = 'http://{host}/gsxygs/'.format(host=self.host)
            content = self.get_captcha_geetest(url, '#text_query', '#input_img', keyword, 'div#u5_img.queryTip')
            if content is None:
                return param_list, self.SEARCH_ERROR

            jq = PyQuery(content, parser='html')
            if jq.find('div#u5_img.queryTip').find('span').eq(1).text() == '0':
                return param_list, self.SEARCH_NOTHING_FIND

            pattern = 'detail\(this,\'(.*?)\',\'(.*?)\'\);'
            regex = re.compile(pattern)
            item_list = jq.find('.ent').items()
            for item in item_list:
                ent_info = item.find('.entInfo')
                div = ent_info.find('.ent-inblock.entName')
                onclick = div.attr('onclick')
                if onclick is None or onclick == '':
                    continue

                company = div.find('strong').text()
                if company is None or company == '':
                    continue

                search_list = regex.findall(onclick)
                if len(search_list) <= 0:
                    continue

                search_name = company.replace(' ', '')
                if search_name == '':
                    continue

                param = {
                    'pripid': search_list[0][0],
                    'entcate': search_list[0][1],
                    'search_name': search_name,
                }

                # 解析统一社会信用号
                seed_code = None
                code_text = item.find('.opInfo').find('.ent-inblock.regno').text()
                if code_text is not None and code_text != '':
                    part = code_text.split('：')
                    if len(part) >= 2:
                        seed_code = part[1]

                status = ent_info.find('span.ent-inblock.label').text()
                if status is not None and status != '':
                    param['status'] = status

                if seed_code is not None and seed_code.strip() != '':
                    param['unified_social_credit_code'] = seed_code

                param_list.append(param)
        except Exception as e:
            self.log.exception(e)
            return param_list, self.SEARCH_ERROR

        return param_list, self.SEARCH_SUCCESS if len(param_list) > 0 else self.SEARCH_ERROR

    @staticmethod
    def __get_company_name(text):
        jq = PyQuery(text, parser='html')
        search_list = jq.find('#entNameFont').text()
        if len(search_list) > 0:
            return search_list.strip()

        return None

    # 基本信息
    def get_base_info(self, session, pri_pid, ent_cate):
        url = 'http://{host}/gsxygs/pubSearch/basicView'.format(host=self.host)

        post_data = {
            'pripid': pri_pid,
            'entcate': ent_cate,
            'queryType': 'query',
            'entname': '',
            'geetest_challenge': '',
            'geetest_validate': '',
            'geetest_seccode': '',
        }

        # 基本信息
        r = self.task_request(session, session.post, url, data=post_data)
        if r is None:
            return None, None, None

        return url, r.text, post_data

    # 获得年报信息
    def get_annual_info(self, session, data, text):
        if text == '' or text is None:
            return
        try:
            pattern = 'ancheClick\(\'(.*?)\',\'(.*?)\',\'(.*?)\'\);'
            regex = re.compile(pattern)
            search_list = regex.findall(text)
            if len(search_list) <= 0:
                return

            for item in search_list:
                url = 'http://{host}/gsxygs/anche/ancheInfo'.format(host=self.host)
                post_data = {
                    'ancheId': item[0],
                    'entcate': item[1],
                    'ancheyear': item[2],
                }
                r = self.task_request(session, session.post, url, data=post_data)
                if r is None:
                    self.append_model(data, Model.annual_info, url, '',
                                      status=self.STATUS_FAIL,
                                      year=item[2], classify=Model.type_detail,
                                      post_data=post_data)
                    continue

                self.append_model(data, Model.annual_info, url, r.text,
                                  year=item[2],
                                  classify=Model.type_detail,
                                  post_data=post_data)
        except Exception as e:
            self.log.exception(e)

    def get_key_person_info(self, session, data, pri_pid, ent_cate):
        url = 'http://{host}/gsxygs/pubSearch/personView'.format(host=self.host)
        post_data = {
            'pripid': pri_pid,
            'entcate': ent_cate,
        }
        r = self.task_request(session, session.post, url, data=post_data)
        if r is None:
            self.append_model(data, Model.key_person_info, url, '',
                              status=self.STATUS_FAIL,
                              post_data=post_data)
            return
        self.append_model(data, Model.key_person_info, url, r.text,
                          post_data=post_data)

    # 获得分支结构信息
    def get_branch_info(self, session, data, pri_pid, ent_cate):
        # fzjgForm
        url = 'http://{host}/gsxygs/pubSearch/fzjgView'.format(host=self.host)
        post_data = {
            'pripid': pri_pid,
            'entcate': ent_cate,
        }
        r = self.task_request(session, session.post, url, data=post_data)
        if r is None:
            self.append_model(data, Model.branch_info, url, '',
                              status=self.STATUS_FAIL,
                              post_data=post_data)
            return

        self.append_model(data, Model.branch_info, url, r.text,
                          post_data=post_data)

    # 出资信息
    def get_contributive_info(self, session, data, base_text):
        try:
            pattern = 'invDetail\(\'(.*?)\'\);'
            regex = re.compile(pattern)

            jq = PyQuery(base_text, parser='html')
            item_list = jq.find('#invTab').find('tr')
            for index in xrange(1, item_list.length):
                onclick = item_list.eq(index).find('a').attr('onclick')
                if onclick is None or onclick == '':
                    continue
                search_list = regex.findall(onclick)
                if len(search_list) <= 0:
                    continue

                url = 'http://{host}/gsxygs/pubSearch/gqczrDetail?invid={invid}'.format(
                    host=self.host, invid=search_list[0])
                r = self.task_request(session, session.get, url)
                if r is None:
                    self.append_model(data, Model.contributive_info, url, '',
                                      status=self.STATUS_FAIL,
                                      classify=Model.type_detail)
                    continue

                self.append_model(data, Model.contributive_info, url, r.text,
                                  classify=Model.type_detail)

        except Exception as e:
            self.log.exception(e)

    # 动产抵押信息抓取  样例企业 甘肃皇台酒业股份有限公司
    def get_chattel_mortgage_info(self, session, data, base_url, base_text):

        # 先存储动产抵押列表页信息
        self.append_model(data, Model.chattel_mortgage_info, base_url, base_text)

        jq = PyQuery(base_text, parser='html')
        tr_list = jq.find("#moveTab").find('tr')
        if tr_list is None:
            return

        length = len(tr_list)
        for i in xrange(1, length):
            a_field = tr_list.eq(i).find('a')
            if a_field is None or len(a_field) <= 0:
                continue

            onclick = a_field.attr("onclick")
            if onclick is None or onclick.strip() == '':
                continue

            temp_list = onclick.split('\'')
            if temp_list is None or len(temp_list) < 2:
                continue

            temp_list = temp_list[1].split('\'')
            if temp_list is None or len(temp_list) <= 0:
                continue

            morreg_id = temp_list[0]

            url = 'http://{host}/gsxygs/pubSearch/dcdyDetail?morreg_id={m_id}'.format(
                host=self.host, m_id=morreg_id)
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.chattel_mortgage_info, url, '',
                                  status=self.STATUS_FAIL,
                                  classify=Model.type_detail)
                continue
            self.append_model(data, Model.chattel_mortgage_info, url, r.text,
                              classify=Model.type_detail)


            # pattern = "dcdyDetail\('(.*?)','(.*?)'\);"
            # regex = re.compile(pattern)
            # search_list = regex.findall(base_text)
            # if len(search_list) <= 0:
            #     return
            #
            # for item in search_list:
            #     url = 'http://{host}/gsxygs/pubSearch/dcdyDetail?morreg_id={m_id}'.format(
            #         host=self.host, m_id=item[0])
            #     r = self.task_request(session, session.get, url)
            #     if r is None:
            #         self.append_model(data, Model.chattel_mortgage_info, url, '',
            #                           status=self.STATUS_FAIL,
            #                           classify=Model.type_detail)
            #         continue
            #     self.append_model(data, Model.chattel_mortgage_info, url, r.text,
            #                       classify=Model.type_detail)

    # 股权质押信息抓取 甘肃宏良皮业股份有限公司
    def get_equity_pledged_info(self, session, data, base_url, base_text):
        # 先存储股权质押列表页信息
        self.append_model(data, Model.equity_pledged_info, base_url, base_text)

        jq = PyQuery(base_text, parser='html')
        tr_list = jq.find("#stockTab").find('tr')
        if tr_list is None:
            return

        length = len(tr_list)
        for i in xrange(1, length):
            a_field = tr_list.eq(i).find('a')
            if a_field is None or len(a_field) <= 0:
                continue

            onclick = a_field.attr("onclick")
            if onclick is None or onclick.strip() == '':
                continue

            temp_list = onclick.split('\'')
            if temp_list is None or len(temp_list) < 2:
                continue

            temp_list = temp_list[1].split('\'')
            if temp_list is None or len(temp_list) <= 0:
                continue

            imporgid = temp_list[0]

            url = 'http://{host}/gsxygs/pubSearch/gqczChange?imporgid={m_id}'.format(
                host=self.host, m_id=imporgid)
            r = self.task_request(session, session.get, url)
            if r is None:
                self.append_model(data, Model.equity_pledged_info, url, '',
                                  status=self.STATUS_FAIL,
                                  classify=Model.type_detail)
                continue
            self.append_model(data, Model.equity_pledged_info, url, r.text,
                              classify=Model.type_detail)

    def get_detail_html_list(self, seed, session, param_list):
        # 保存企业名称
        data_list = []
        for item in param_list:
            try:
                pri_pid = item.get('pripid', None)
                ent_cate = item.get('entcate', None)
                if pri_pid is None or ent_cate is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                search_name = item.get('search_name', None)
                if search_name is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                url, base_text, post_data = self.get_base_info(session, pri_pid, ent_cate)
                if url is None:
                    continue

                # 获得公司名称
                company = self.__get_company_name(base_text)
                if company is None or company == '':
                    self.log.error('公司名称解析失败..pripid = {pripid} encate = {encate}'.format(
                        pripid=pri_pid, encate=ent_cate, text=base_text))
                    continue

                # 建立数据模型
                data = self.get_model(company, seed, search_name, self.province)

                # 存储数据
                self.append_model(data, Model.base_info, url, base_text,
                                  post_data=post_data)

                # 出资信息列表页
                self.append_model(data, Model.contributive_info, url, base_text)

                # 出资信息
                self.get_contributive_info(session, data, base_text)

                # 获取变更信息
                self.append_model(data, Model.change_info, url, base_text)

                # 获取股东信息
                self.append_model(data, Model.shareholder_info, url, base_text)

                # 列入经营异常名录信息
                self.append_model(data, Model.abnormal_operation_info, url, base_text)

                # 动产抵押登记信息
                self.get_chattel_mortgage_info(session, data, url, base_text)

                # 股权质押信息
                self.get_equity_pledged_info(session, data, url, base_text)

                # 获得主要人员信息
                self.get_key_person_info(session, data, pri_pid, ent_cate)

                # 获得分支结构信息
                self.get_branch_info(session, data, pri_pid, ent_cate)

                # 年报信息
                self.get_annual_info(session, data, base_text)

                data_list.append(data)
            except Exception as e:
                self.log.exception(e)

        return self.sent_to_target(data_list)
