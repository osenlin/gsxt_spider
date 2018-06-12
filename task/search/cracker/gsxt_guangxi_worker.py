# -*- coding: utf-8 -*-
import re

from pyquery import PyQuery

from base.gsxt_base_worker import GsxtBaseWorker
from common import util
from common.global_field import Model

'''
1. 搜索没有结果判断功能添加
2. 包含出资信息 包含详情页
3. 包含年报信息
4. 添加统计信息
5. 完成topo信息添加 已经check
6. 完成列表页名称提取
'''


class GsxtGuangXiWorker(GsxtBaseWorker):
    def __init__(self, **kwargs):
        GsxtBaseWorker.__init__(self, **kwargs)

    def get_search_list_html(self, keyword, session):
        param_list = []
        try:
            url = 'http://{host}/sydq/loginSydqAction!sydq.dhtml'.format(host=self.host)
            content = self.get_captcha_geetest(url, '#keyword_qycx', '#popup-submit', keyword, '.search-result')
            if content is None:
                return param_list, self.SEARCH_ERROR

            jq = PyQuery(content, parser='html')
            if jq.find('div.search-result').find('p.total').find('span.light').text() == '0':
                return param_list, self.SEARCH_NOTHING_FIND

            param_set = set()
            pattern = 'showDialog\("/gjjbj/gjjQueryCreditAction!queryEntIndexInfo\.dhtml\?entId=(.*?)&clear=true&urltag=1&urlflag=0&credit_ticket=(.*?)",".*?",".*?"\)'
            regex = re.compile(pattern)
            item_list = jq.find('div.search-result').find('ul').find('li').items()
            for item in item_list:
                h3 = item.find('.title')
                onclick = h3.attr('onclick')
                if onclick is None or onclick == '':
                    continue

                search_list = regex.findall(onclick)
                if len(search_list) <= 0:
                    continue

                hash_obj = search_list[0][0] + '_' + search_list[0][1]
                if hash_obj in param_set:
                    continue

                ent_state1 = h3.find('span[class=ent_state1]').remove()
                status = ent_state1.text()
                if status is None or status == '':
                    ent_state2 = h3.find('span[class=ent_state2]').remove()
                    status = ent_state2.text()
                    if status is None or status == '':
                        ent_state3 = h3.find('span[class=ent_state3]').remove()
                        status = ent_state3.text()
                        if status is None or status == '':
                            ent_state4 = h3.find('span[class=ent_state4]').remove()
                            status = ent_state4.text()

                h3.find('strong').remove()
                company = h3.text()
                if company is None or company == '':
                    continue

                search_name = company.replace(' ', '')
                if search_name == '':
                    continue

                seed_code = None
                code_text = item.find('.t-info').find('td').eq(0).text()
                if code_text is not None and code_text.strip() != '':
                    part = code_text.split('：')
                    if len(part) >= 2:
                        seed_code = part[1]

                param = {
                    'credit_ticket': search_list[0][1],
                    'entId': search_list[0][0],
                    'search_name': search_name
                }

                # 统一社会信用号
                if seed_code is not None and seed_code.strip() != '':
                    param['unified_social_credit_code'] = seed_code

                if status is not None and status != '':
                    param['status'] = status

                param_set.add(hash_obj)
                param_list.append(param)
        except Exception as e:
            self.log.exception(e)
            return param_list, self.SEARCH_ERROR

        return param_list, self.SEARCH_SUCCESS if len(param_list) > 0 else self.SEARCH_ERROR

    @staticmethod
    def get_company_name(text):
        pattern = '<td><strong>企业名称：</strong>(.*?)</td>'
        search_list = re.findall(pattern, text.encode('utf-8'))
        if len(search_list) > 0:
            return search_list[0].strip()

        pattern = '<td><strong>名称：</strong>(.*?)</td>'
        search_list = re.findall(pattern, text.encode('utf-8'))
        if len(search_list) > 0:
            return search_list[0].strip()

        return None

    def get_detail_info(self, session, href, info, data):
        if href is None or href == '':
            return None

        url = 'http://{host}{href}'.format(host=self.host, href=href)
        r = self.task_request(session, session.get, url=url)
        if r is None:
            self.append_model(data, info, url, '', status=self.STATUS_FAIL)
            return None

        self.append_model(data, info, url, r.text)
        return r.text

    # 进行翻页
    def get_nb_page_turn(self, session, data, page_url, page_text, year):
        url = page_url.split('?')[0]
        post_data = util.get_url_param(page_url)
        page_div = PyQuery(page_text, parser='html').find('.pages')
        # 判断是否由翻页信息
        if page_div.text().strip() == '':
            return
        page_nos = page_div.find('#pagescount').attr('value')
        page_size = page_div.find('#pageSize').attr('value')

        if page_nos is None or page_nos.strip() == '':
            return
        try:
            if int(page_nos) <= 1:
                return
        except Exception as e:
            self.log.exception(e)
            return

        if page_size is None or page_size.strip() == '':
            page_size = '5'

        post_data['pageNos'] = page_nos
        post_data['pageSize'] = page_size

        for page_num in xrange(2, int(page_nos) + 1):
            r = self.task_request(session, session.post, url=url, data=post_data)
            if r is None:
                self.append_model(data, Model.annual_info, url, '',
                                  status=self.STATUS_FAIL,
                                  year=year, classify=Model.type_detail,
                                  post_data=post_data)
            else:
                self.append_model(data, Model.annual_info, url, r.text,
                                  year=year, classify=Model.type_detail,
                                  post_data=post_data)

    def get_nb_detail_info(self, session, text, data, year):
        jq = PyQuery(text, parser='html')

        # 网站网店 没有翻页
        href = jq.find('#wzFrame').attr('src')
        if href is not None and href != '':
            url = 'http://{host}{href}'.format(host=self.host, href=href)
            r = self.task_request(session, session.get, url)
            if r is not None:
                self.append_model(data, Model.annual_info, url, r.text,
                                  year=year, classify=Model.type_detail)
            else:
                self.append_model(data, Model.annual_info, url, '',
                                  status=self.STATUS_FAIL,
                                  year=year, classify=Model.type_detail)

        # 股东信息  有翻页 需要重写
        href = jq.find('#gdczFrame').attr('src')
        if href is not None and href != '':
            url = 'http://{host}{href}'.format(host=self.host, href=href)
            r = self.task_request(session, session.get, url)
            if r is not None:
                self.append_model(data, Model.annual_info, url, r.text,
                                  year=year, classify=Model.type_detail)
                self.get_nb_page_turn(session, data, url, r.text, year)
            else:
                self.append_model(data, Model.annual_info, url, '',
                                  status=self.STATUS_FAIL,
                                  year=year, classify=Model.type_detail)

        # 对外投资 没有翻页
        href = jq.find('#dwtzFrame').attr('src')
        if href is not None and href != '':
            url = 'http://{host}{href}'.format(host=self.host, href=href)
            r = self.task_request(session, session.get, url)
            if r is not None:
                self.append_model(data, Model.annual_info, url, r.text,
                                  year=year, classify=Model.type_detail)
            else:
                self.append_model(data, Model.annual_info, url, '',
                                  status=self.STATUS_FAIL,
                                  year=year, classify=Model.type_detail)
        # 行政许可 有翻页
        href = jq.find('#xzxkFrame').attr('src')
        if href is not None and href != '':
            url = 'http://{host}{href}'.format(host=self.host, href=href)
            r = self.task_request(session, session.get, url)
            if r is not None:
                self.append_model(data, Model.annual_info, url, r.text,
                                  year=year, classify=Model.type_detail)
                self.get_nb_page_turn(session, data, url, r.text, year)
            else:
                self.append_model(data, Model.annual_info, url, '',
                                  status=self.STATUS_FAIL,
                                  year=year, classify=Model.type_detail)

        # 分支机构 有翻页
        href = jq.find('#fzjgFrame').attr('src')
        if href is not None and href != '':
            url = 'http://{host}{href}'.format(host=self.host, href=href)
            r = self.task_request(session, session.get, url)
            if r is not None:
                self.append_model(data, Model.annual_info, url, r.text,
                                  year=year, classify=Model.type_detail)
                self.get_nb_page_turn(session, data, url, r.text, year)
            else:
                self.append_model(data, Model.annual_info, url, '',
                                  status=self.STATUS_FAIL,
                                  year=year, classify=Model.type_detail)

        # 对外担保 有翻页
        href = jq.find('#dwdbFrame').attr('src')
        if href is not None and href != '':
            url = 'http://{host}{href}'.format(host=self.host, href=href)
            r = self.task_request(session, session.get, url)
            if r is not None:
                self.append_model(data, Model.annual_info, url, r.text,
                                  year=year, classify=Model.type_detail)
                self.get_nb_page_turn(session, data, url, r.text, year)
            else:
                self.append_model(data, Model.annual_info, url, '',
                                  status=self.STATUS_FAIL,
                                  year=year, classify=Model.type_detail)

        # 修改信息 有翻页
        href = jq.find('#xgFrame').attr('src')
        if href is not None and href != '':
            url = 'http://{host}{href}'.format(host=self.host, href=href)
            r = self.task_request(session, session.get, url)
            if r is not None:
                self.append_model(data, Model.annual_info, url, r.text,
                                  year=year, classify=Model.type_detail)
                self.get_nb_page_turn(session, data, url, r.text, year)
            else:
                self.append_model(data, Model.annual_info, url, '',
                                  status=self.STATUS_FAIL,
                                  year=year, classify=Model.type_detail)

    def get_year_info_list(self, text):
        jq = PyQuery(text, parser='html')
        tr_list = jq.find('tr').items()
        for index, item in enumerate(tr_list):
            if index == 0:
                continue
            try:
                year_info = item.find('td').eq(1).text()
                href = item.find('a').attr('href')

                year_list = re.findall('(\d+)', year_info)
                year = str(year_list[0]) if len(year_list) > 0 else None
                if href is None or href == '':
                    continue
                if year is None or year == '':
                    continue
                yield year, 'http://{host}{href}'.format(host=self.host, href=href)
            except Exception as e:
                self.log.exception(e)

    # 获得变更信息
    def get_change_info(self, session, data, credit_ticket, ent_id):
        url = 'http://{host}/gjjbj/gjjQueryCreditAction!xj_biangengFrame.dhtml?clear=true&credit_ticket={credit_ticket}'.format(
            host=self.host, credit_ticket=credit_ticket
        )
        page_num = 1
        total_page_num = 1
        request_list = []
        while page_num <= total_page_num:
            post_data = {
                'pageNos': page_num,
                'ent_id': ent_id,
                'urltag': '5',
                'pageSize': '5',
                'pageNo': page_num if page_num == 1 else page_num - 1,
            }

            r = self.task_request(session, session.post, url, data=post_data)
            if r is None:
                return None

            if r.text.find('暂无变更信息') != -1:
                return None

            # 获得总页码信息
            if total_page_num == 1:
                total_page_num = PyQuery(r.text, parser='html').find('#iframeFrame').find('#pagescount').attr('value')
                if total_page_num is None or total_page_num == '':
                    return None
                total_page_num = int(total_page_num)

            # # 获得变更信息详情页信息
            # self.get_change_info_detail(session, r.text, data)

            self.append_model(data, Model.change_info, url, r.text,
                              post_data=post_data)
            request_list.append(r.text)
            page_num += 1

        return request_list

    # 获得主要人员信息
    def get_key_person_info(self, session, data, credit_ticket, ent_id):
        url = 'http://{host}/gjjbj/gjjQueryCreditAction!zyryFrame.dhtml?flag=more&ent_id={entid}&credit_ticket={credit_ticket}'.format(
            host=self.host, entid=ent_id, credit_ticket=credit_ticket)
        r = self.task_request(session, session.get, url)
        if r is None:
            return None
        self.append_model(data, Model.key_person_info, url, r.text)
        return data

    # 获得分支结构信息
    def get_branch_info(self, session, data, credit_ticket, ent_id, href):
        pattern = '/gjjbj/gjjQueryCreditAction!fzjgFrame\.dhtml\?ent_id=.*?&regno=(.*?)&clear=true&urltag=.*?&credit_ticket=.*?'
        search_list = re.findall(pattern, href)
        if len(search_list) <= 0:
            return None

        url = 'http://{host}/gjjbj/gjjQueryCreditAction!fzjgFrame.dhtml?flag=more&ent_id={entid}&regno={regno}&credit_ticket={credit_ticket}'.format(
            host=self.host, entid=ent_id, credit_ticket=credit_ticket, regno=search_list[0])
        r = self.task_request(session, session.get, url)
        if r is None:
            return None

        self.append_model(data, Model.branch_info, url, r.text)
        return data

    # 获得出资详情
    def get_contributive_info_detail(self, session, data, text):
        url = 'http://{host}/gjjbjTab/gjjTabQueryCreditAction!gdczDetail.dhtml'.format(host=self.host)
        pattern = 'lookAjaxInfo\(\'(.*?)\',\'(.*?)\',\'(.*?)\'\)'
        regex = re.compile(pattern)
        a_list = PyQuery(text, parser='html').find('a').items()
        for a_item in a_list:
            tr_text = a_item.attr('onclick')
            if tr_text is None or tr_text == '':
                continue

            search_list = regex.findall(tr_text)
            if len(search_list) <= 0:
                continue

            post_data = {
                'ent_id': search_list[0][0],
                'chr_id': search_list[0][1],
                'ajax': True,
                'time': util.get_time_stamp(),
                'dateflag': search_list[0][2]
            }
            r = self.task_request(session, session.post, url, data=post_data)
            if r is None:
                self.append_model(data, Model.contributive_info, url, '',
                                  post_data=post_data, classify=Model.type_detail)
                continue
            self.append_model(data, Model.contributive_info, url, r.text,
                              post_data=post_data, classify=Model.type_detail)

    @staticmethod
    def get_contributive_info_page(text):
        total_page = PyQuery(text, parser='html').find('#pagescount').attr('value')
        if total_page is None or total_page.strip() == '':
            return 1

        return int(total_page)

    def get_contributive_info(self, session, data, credit_ticket, ent_id):
        url = 'http://{host}/gjjbj/gjjQueryCreditAction!touzirenInfo.dhtml?chr_id=&credit_ticket={credit_ticket}'.format(
            host=self.host, credit_ticket=credit_ticket)
        cur_page = 1
        total_page_num = 1
        while cur_page <= total_page_num:

            post_data = {
                'pageNos': cur_page,
                'ent_id': ent_id,
                'urltag': '',
                'pageSize': '5',
                'pageNo': cur_page if cur_page == 1 else cur_page - 1,
            }

            r = self.task_request(session, session.post, url, data=post_data)
            if r is None:
                if cur_page == 1:
                    return None
                self.append_model(data, Model.contributive_info, url, '',
                                  post_data=post_data)
                cur_page += 1
                continue

            # 获得出资详情
            self.get_contributive_info_detail(session, data, r.text)
            self.append_model(data, Model.contributive_info, url, r.text,
                              post_data=post_data)

            if total_page_num <= 1:
                total_page_num = self.get_contributive_info_page(r.text)

            cur_page += 1

        return data

    # 获得变更信息详情
    def get_change_info_detail(self, session, request_list, data):
        pattern = "selectTp\('(.*?)', '.*?', '.*?'\);"
        regex = re.compile(pattern)
        for text_item in request_list:
            a_list = PyQuery(text_item, parser='html').find('a').items()
            for a_item in a_list:
                onclick = a_item.attr('onclick')
                search_list = regex.findall(onclick)
                if len(search_list) <= 0:
                    continue

                url = 'http://{host}{sub_url}'.format(host=self.host, sub_url=search_list[0])
                r = self.task_request(session, session.get, url)
                if r is None:
                    continue

                detail = {
                    'url': url,
                    'text': r.text,
                    'status': self.STATUS_SUCCESS,
                    'match_feature': search_list[0],
                }
                self.append_model_item(data, Model.change_info, detail, classify=Model.type_detail)

    def get_other_info(self, session, data, text, credit_ticket, ent_id):
        if text == '':
            return

        jq = PyQuery(text, parser='html')
        frame_list = jq.find('iframe').items()

        for item in frame_list:
            href = item.attr('src')
            if href is None:
                continue

            # 出资信息
            if href.find('touzirenInfo') != -1:
                if self.get_contributive_info(session, data, credit_ticket, ent_id) is None:
                    text = self.get_detail_info(session, href, Model.contributive_info, data)
                    if text is None:
                        continue

                    # 获得出资详情
                    self.get_contributive_info_detail(session, data, text)
                continue

            # 主要人员信息
            if href.find('zyryFrame') != -1:
                if self.get_key_person_info(session, data, credit_ticket, ent_id) is None:
                    self.get_detail_info(session, href, Model.key_person_info, data)
                continue

            # 分支机构信息
            if href.find('fzjgFrame') != -1:
                if self.get_branch_info(session, data, credit_ticket, ent_id, href) is None:
                    self.get_detail_info(session, href, Model.branch_info, data)
                continue

            # 清算信息
            if href.find('qsxxFrame') != -1:
                self.get_detail_info(session, href, Model.liquidation_info, data)
                continue

            # 变更信息
            if href.find('biangengFrame') != -1:
                # 先查找是否有翻页的变更信息
                request_list = self.get_change_info(session, data, credit_ticket, ent_id)
                if request_list is None:
                    request_list = []
                    request_text = self.get_detail_info(session, href, Model.change_info, data)
                    if request_text is not None:
                        request_list.append(request_text)
                if request_list is not None and len(request_list) > 0:
                    self.get_change_info_detail(session, request_list, data)
                continue

            # 股东信息
            if href.find('getTabForNB_new') != -1 and href.find('flag_num=1') != -1:
                self.get_detail_info(session, href, Model.shareholder_info, data)
                continue

            # 企业年报
            if href.find('qynbxxList') != -1:
                url = 'http://{host}{href}'.format(host=self.host, href=href)
                r = self.task_request(session, session.get, url)
                if r is None:
                    continue

                for year, year_url in self.get_year_info_list(r.text):
                    r = self.task_request(session, session.get, year_url)
                    if r is None:
                        self.append_model(data, Model.annual_info, url, '',
                                          status=self.STATUS_FAIL,
                                          year=year, classify=Model.type_detail)
                        continue

                    self.append_model(data, Model.annual_info, url, r.text,
                                      year=year,
                                      classify=Model.type_detail)
                    self.get_nb_detail_info(session, r.text, data, year)

    def get_detail_html_list(self, seed, session, param_list):
        data_list = []
        for item in param_list:
            try:
                credit_ticket = item.get('credit_ticket', None)
                ent_id = item.get('entId', None)
                if credit_ticket is None or ent_id is None:
                    self.log.info('没有关键信息 item = {item}'.format(item=item))
                    continue

                search_name = item.get('search_name', None)
                if search_name is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                # 基本信息
                url = 'http://{host}/gjjbj/gjjQueryCreditAction!openEntInfo.dhtml?entId={' \
                      'entId}&clear=true&urltag=1&credit_ticket={credit_ticket}'.format(
                    host=self.host, entId=ent_id, credit_ticket=credit_ticket)
                r = self.task_request(session, session.get, url)
                if r is None:
                    continue

                if r.text.find('访问异常') != -1 and r.text.find('过于频繁或非正常访问') != -1:
                    self.log.error('IP 被封禁, 无法访问页面')
                    continue

                # 解析公司信息
                company = self.get_company_name(r.text)
                if company is None or company == '':
                    self.log.error('获取公司信息失败...{text}'.format(text=r.text))
                    continue

                # 建立数据模型
                data = self.get_model(company, seed, search_name, self.province)

                # 保存基础信息
                self.append_model(data, Model.base_info, url, r.text)

                # 获取其他信息
                self.get_other_info(session, data, r.text, credit_ticket, ent_id)

                data_list.append(data)
            except Exception as e:
                self.log.exception(e)

        return self.sent_to_target(data_list)
