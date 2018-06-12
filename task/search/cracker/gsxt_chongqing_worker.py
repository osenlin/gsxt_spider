#!/usr/bin/env python
# -*- coding:utf-8 -*-
import re

from pyquery import PyQuery

from base.gsxt_base_worker import GsxtBaseWorker
from common import util
from common.global_field import Model

'''
1. 搜索没有结果判断功能添加
2. 包含出资信息  有详情页, 且由三个页面组成
3. 包含年报信息
4. 添加统计信息
5. 添加完拓扑信息
6. 完成列表页名称提取
'''


class GsxtChongQingWorker(GsxtBaseWorker):
    def __init__(self, **kwargs):
        GsxtBaseWorker.__init__(self, **kwargs)

    def get_search_list_html(self, keyword, session):
        param_list = []
        try:
            # keyword = '重庆钢铁股份有限公司'
            url = 'http://{host}/'.format(host=self.host)
            content = self.get_captcha_geetest(url, '#s_txt_ipt', '#s-btn', keyword,
                                               '.r_s_tip_wr', add_link=True, click_first=False)
            if content is None:
                return param_list, self.SEARCH_ERROR

            # self.log.info(content)
            jq = PyQuery(content, parser='html')
            find_flag = jq.find('div.r_s_num_tip.ng-binding').find('.ng-binding').text()
            if find_flag == '0':
                self.log.info('没有搜索到任何信息: keyword = {key}'.format(key=keyword))
                return param_list, self.SEARCH_ERROR

            item_list = jq.find('.r_lst_nav').items()
            for item in item_list:
                pripid = item.attr('data-item-pripid')
                if pripid is None or pripid == '':
                    continue

                pritype = item.attr('data-item-pritype')
                if pritype is None or pritype == '':
                    continue

                search_name = item.attr('data-item-entnamehl')
                if search_name is None or search_name == '':
                    continue

                status = item.attr('data-item-regstate_cn')
                # 统一社会信用号
                seed_code = item.attr('data-item-uniscid_title')
                param = {
                    'pripid': pripid,
                    'search_name': search_name,
                    'pritype': pritype,
                }

                if status is not None and status != '':
                    param['status'] = status

                if seed_code is not None and seed_code.strip() != '':
                    param['unified_social_credit_code'] = seed_code

                param_list.append(param)

        except Exception as e:
            self.log.exception(e)
            return param_list, self.SEARCH_ERROR

        return param_list, self.SEARCH_SUCCESS if len(param_list) > 0 else self.SEARCH_ERROR

    # 获得公司名称
    @staticmethod
    def get_copmany_name(text):
        search_list = re.findall('"entname":"(.*?)"', text)
        if len(search_list) > 0:
            return search_list[0]

        return None

    def get_annual_info(self, session, pripid, pritype, data):
        # 年报信息
        annual_info_url = 'http://{host}/gsxt/api/anbaseindex/queryList/{pripid}/{pritype}?currentpage=1&pagesize=100&t={rand}'.format(
            host=self.host, pripid=pripid, pritype=pritype, rand=util.get_time_stamp())
        r = self.task_request(session, session.get, annual_info_url)
        if r is None:
            return None

        json_data = util.json_loads(r.text)
        if json_data is None:
            return None

        for item in json_data:
            nb_list = item.get('list', None)
            if nb_list is None:
                continue

            for nb_item in nb_list:
                anche_id = nb_item.get('ancheid', None)
                anche_year = nb_item.get('ancheyear', None)
                if anche_id is None:
                    continue
                if anche_year is None:
                    continue
                # 基本信息
                base_info_url = 'http://{host}/gsxt/api/anbaseinfo/queryForm/{ancheid}?currentpage=1&pagesize=100&t={rand}'.format(
                    host=self.host, ancheid=anche_id, rand=util.get_time_stamp())
                r = self.task_request(session, session.get, base_info_url)
                if r is not None:
                    self.append_model(data, Model.annual_info, base_info_url, r.text,
                                      year=anche_year,
                                      classify=Model.type_detail)
                else:
                    self.append_model(data, Model.annual_info, base_info_url, '',
                                      status=self.STATUS_FAIL,
                                      year=anche_year,
                                      classify=Model.type_detail)

                # 网站信息
                web_info_url = 'http://{host}/gsxt/api/anwebsiteinfo/queryList/{ancheid}/{pripid}/{pritype}?currentpage=1&pagesize=100&t={rand}'.format(
                    host=self.host, ancheid=anche_id, pritype=pritype, rand=util.get_time_stamp(), pripid=pripid)
                r = self.task_request(session, session.get, web_info_url)
                if r is not None:
                    self.append_model(data, Model.annual_info, web_info_url, r.text,
                                      year=anche_year,
                                      classify=Model.type_detail)
                else:
                    self.append_model(data, Model.annual_info, web_info_url, '',
                                      status=self.STATUS_FAIL,
                                      year=anche_year,
                                      classify=Model.type_detail)

                # 股东信息
                shareholder_info_url = 'http://{host}/gsxt/api/ansubcapital/queryList/{ancheid}?currentpage=1&pagesize=100&t={rand}'.format(
                    host=self.host, ancheid=anche_id, rand=util.get_time_stamp())
                r = self.task_request(session, session.get, shareholder_info_url)
                if r is not None:
                    self.append_model(data, Model.annual_info, shareholder_info_url, r.text,
                                      year=anche_year,
                                      classify=Model.type_detail)
                else:
                    self.append_model(data, Model.annual_info, shareholder_info_url, '',
                                      status=self.STATUS_FAIL,
                                      year=anche_year,
                                      classify=Model.type_detail)

                # 对外投资
                investment_info_url = 'http://{host}/gsxt/api/anforinvestment/queryList/{ancheid}?currentpage=1&pagesize=100&t={rand}'.format(
                    host=self.host, ancheid=anche_id, rand=util.get_time_stamp())
                r = self.task_request(session, session.get, investment_info_url)
                if r is not None:
                    self.append_model(data, Model.annual_info, investment_info_url, r.text,
                                      year=anche_year,
                                      classify=Model.type_detail)
                else:
                    self.append_model(data, Model.annual_info, investment_info_url, '',
                                      status=self.STATUS_FAIL,
                                      year=anche_year,
                                      classify=Model.type_detail)

                # 资产状况
                assets_info_url = 'http://{host}/gsxt/api/anbaseinfo/queryForm/{ancheid}?currentpage=1&pagesize=100&t={rand}'.format(
                    host=self.host, ancheid=anche_id, rand=util.get_time_stamp())
                r = self.task_request(session, session.get, assets_info_url)
                if r is not None:
                    self.append_model(data, Model.annual_info, assets_info_url, r.text,
                                      year=anche_year,
                                      classify=Model.type_detail)
                else:
                    self.append_model(data, Model.annual_info, assets_info_url, '',
                                      status=self.STATUS_FAIL,
                                      year=anche_year,
                                      classify=Model.type_detail)

                # 担保信息
                assurance_info_url = 'http://{host}/gsxt/api/anforguaranteeinfo/queryList/{ancheid}?currentpage=1&pagesize=100&t={rand}'.format(
                    host=self.host, ancheid=anche_id, rand=util.get_time_stamp())
                r = self.task_request(session, session.get, assurance_info_url)
                if r is not None:
                    self.append_model(data, Model.annual_info, assurance_info_url, r.text,
                                      year=anche_year,
                                      classify=Model.type_detail)
                else:
                    self.append_model(data, Model.annual_info, assurance_info_url, '',
                                      status=self.STATUS_FAIL,
                                      year=anche_year,
                                      classify=Model.type_detail)

                # 社保信息
                social_security_info_url = 'http://{host}/gsxt/api/ansocialinsuinfo/queryForm/{ancheid}?currentpage=1&pagesize=100&t={rand}'.format(
                    host=self.host, ancheid=anche_id, rand=util.get_time_stamp())
                r = self.task_request(session, session.get, social_security_info_url)
                if r is not None:
                    self.append_model(data, Model.annual_info, social_security_info_url, r.text,
                                      year=anche_year,
                                      classify=Model.type_detail)
                else:
                    self.append_model(data, Model.annual_info, social_security_info_url, '',
                                      status=self.STATUS_FAIL,
                                      year=anche_year,
                                      classify=Model.type_detail)

                # 股权变更
                change_info_url = 'http://{host}/gsxt/api/analterstockinfo/queryList/{ancheid}?currentpage=1&pagesize=100&t={rand}'.format(
                    host=self.host, ancheid=anche_id, rand=util.get_time_stamp())
                r = self.task_request(session, session.get, change_info_url)
                if r is not None:
                    self.append_model(data, Model.annual_info, change_info_url, r.text,
                                      year=anche_year,
                                      classify=Model.type_detail)
                else:
                    self.append_model(data, Model.annual_info, change_info_url, '',
                                      status=self.STATUS_FAIL,
                                      year=anche_year,
                                      classify=Model.type_detail)

                # 修改记录
                amendant_info_url = 'http://{host}/gsxt/api/anupdateinfo/queryList/{ancheid}?currentpage=1&pagesize=100&t={rand}'.format(
                    host=self.host, ancheid=anche_id, rand=util.get_time_stamp())
                r = self.task_request(session, session.get, amendant_info_url)
                if r is not None:
                    self.append_model(data, Model.annual_info, amendant_info_url, r.text,
                                      year=anche_year,
                                      classify=Model.type_detail)
                else:
                    self.append_model(data, Model.annual_info, amendant_info_url, '',
                                      status=self.STATUS_FAIL,
                                      year=anche_year,
                                      classify=Model.type_detail)

    def get_contributive_info_detail(self, session, text, data):
        try:
            json_data = util.json_loads(text)
            if json_data is None:
                return

            json_list = json_data[0].get('list', None)
            if json_list is None:
                return

            for index, item in enumerate(json_list):
                invid = item.get('invid', None)
                if invid is None:
                    continue

                url = 'http://{host}/gsxt/api/einv/gdxx/{invid}?currentpage=1&pagesize=5&t={rand}'.format(
                    host=self.host, invid=invid, rand=util.get_time_stamp())
                r = self.task_request(session, session.get, url)
                if r is not None:
                    self.append_model(data, Model.contributive_info, url, r.text,
                                      classify=Model.type_detail)

                url = 'http://{host}/gsxt/api/einvpaidin/queryList/{invid}?currentpage=1&pagesize=5&t={rand}'.format(
                    host=self.host, invid=invid, rand=util.get_time_stamp())
                r = self.task_request(session, session.get, url)
                if r is not None:
                    self.append_model(data, Model.contributive_info, url, r.text,
                                      classify=Model.type_detail)

                url = 'http://{host}/gsxt/api/efactcontribution/queryList/{invid}?currentpage=1&pagesize=5&t={rand}'.format(
                    host=self.host, invid=invid, rand=util.get_time_stamp())
                r = self.task_request(session, session.get, url)
                if r is not None:
                    self.append_model(data, Model.contributive_info, url, r.text,
                                      classify=Model.type_detail)
        except Exception as e:
            self.log.exception(e)

    def get_detail_html_list(self, seed, session, param_list):
        data_list = []
        session.headers = {
            'Host': self.host,
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/55.0.2883.95 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Cache-Control': 'no-cache',
            'X-Requested-With': 'XMLHttpRequest',
            'appkey': '8dc7959eeee2792ac2eebb490e60deed',
            'Accept-Encoding': 'gzip, deflate, sdch',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
        }

        for item in param_list:
            try:
                pri_pid = item.get('pripid', None)
                pri_type = item.get('pritype', None)
                if pri_pid is None or pri_type is None:
                    self.log.error('参数信息错误...item = {item}'.format(item=item))
                    continue

                search_name = item.get('search_name', None)
                if search_name is None:
                    self.log.error('参数错误: item = {item}'.format(item=item))
                    continue

                base_info_url = "http://{host}/gsxt/api/ebaseinfo/queryForm/{pripid}/{pritype}?currentpage=1&pagesize=5&t={rand}".format(
                    host=self.host, pripid=pri_pid, pritype=pri_type, rand=util.get_time_stamp())

                # 基本信息
                base_info = self.task_request(session, session.get, base_info_url)
                if base_info is None:
                    continue

                if len(base_info.text) <= 15:

                    base_info_url = 'http://{host}/gsxt/api/ebaseindex/queryForm/{pripid}/{pritype}?currentpage=1&pagesize=5&t={rand}'.format(
                        host=self.host, pripid=pri_pid, pritype=pri_type, rand=util.get_time_stamp())
                    base_info = self.task_request(session, session.get, base_info_url)
                    if base_info is None:
                        self.log.info('基本信息抓取失败: pripid = {pripid} text = {text}'.format(
                            pripid=pri_pid, text=base_info.text))
                        continue

                company = self.get_copmany_name(base_info.text)
                if company == '' or company is None:
                    self.log.error('公司名称信息解析错误..pripid = {pripid} {text}'.format(
                        pripid=pri_pid, text=base_info.text))
                    continue

                # 建立数据模型
                data = self.get_model(company, seed, search_name, self.province)

                # 变更信息
                change_info_url = 'http://{host}/gsxt/api/ealterrecoder/queryList/{pripid}/{pritype}?currentpage=1&pagesize=100&t={rand}'.format(
                    host=self.host, pripid=pri_pid, pritype=pri_type, rand=util.get_time_stamp())

                # 出资信息
                contributive_info_url = 'http://{host}/gsxt/api/einv/gdjczxxList/{pripid}/{pritype}?currentpage=1&pagesize=100&t={rand}'.format(
                    host=self.host, pripid=pri_pid, pritype=pri_type, rand=util.get_time_stamp())

                # 主要人员
                key_person_info_url = 'http://{host}/gsxt/api/epriperson/queryList/{pripid}/{pritype}?currentpage=1&pagesize=100&t={rand}'.format(
                    host=self.host, pripid=pri_pid, pritype=pri_type, rand=util.get_time_stamp())

                # 分支机构
                branch_info_url = 'http://{host}/gsxt/api/ebrchinfo/queryList/{pripid}/{pritype}?currentpage=1&pagesize=100&t={rand}'.format(
                    host=self.host, pripid=pri_pid, pritype=pri_type, rand=util.get_time_stamp())

                # 清算信息
                liquidation_info_url = 'http://{host}/gsxt/api/eliqmbrn/queryList/{pripid}/{pritype}?currentpage=1&pagesize=100&t={rand}'.format(
                    host=self.host, pripid=pri_pid, pritype=pri_type, rand=util.get_time_stamp())

                # 股东信息
                shareholder_info_url = 'http://{host}/gsxt/api/eiminvupdate/queryList/{pripid}/{pritype}?currentpage=1&pagesize=100&t={rand}'.format(
                    host=self.host, pripid=pri_pid, pritype=pri_type, rand=util.get_time_stamp())

                # 存储数据
                self.append_model(data, Model.base_info, base_info_url, base_info.text)

                # 清算信息
                liquidation_info = self.task_request(session, session.get, liquidation_info_url)
                if liquidation_info is not None:
                    self.append_model(data, Model.liquidation_info, liquidation_info_url, liquidation_info.text)
                else:
                    self.append_model(data, Model.liquidation_info, liquidation_info_url, '', status=self.STATUS_FAIL)

                # 变更信息
                change_info = self.task_request(session, session.get, change_info_url)
                if change_info is not None:
                    self.append_model(data, Model.change_info, change_info_url, change_info.text)
                else:
                    self.append_model(data, Model.change_info, change_info_url, '', status=self.STATUS_FAIL)

                # 股东信息
                shareholder_info = self.task_request(session, session.get, shareholder_info_url)
                if shareholder_info is not None:
                    self.append_model(data, Model.shareholder_info, shareholder_info_url, shareholder_info.text)
                else:
                    self.append_model(data, Model.shareholder_info, shareholder_info_url, '', status=self.STATUS_FAIL)

                # 出资信息
                contributive_info = self.task_request(session, session.get, contributive_info_url)
                if contributive_info is not None:
                    self.append_model(data, Model.contributive_info, contributive_info_url,
                                      contributive_info.text)
                    self.get_contributive_info_detail(session, contributive_info.text, data)
                else:
                    self.append_model(data, Model.contributive_info, contributive_info_url,
                                      '', status=self.STATUS_FAIL)

                # 主要人员
                key_person_info = self.task_request(session, session.get, key_person_info_url)
                if key_person_info is not None:
                    self.append_model(data, Model.key_person_info, key_person_info_url, key_person_info.text)
                else:
                    self.append_model(data, Model.key_person_info, key_person_info_url, '', status=self.STATUS_FAIL)

                # 分支机构
                branch_info = self.task_request(session, session.get, branch_info_url)
                if branch_info is not None:
                    self.append_model(data, Model.branch_info, branch_info_url, branch_info.text)
                else:
                    self.append_model(data, Model.branch_info, branch_info_url, '', status=self.STATUS_FAIL)

                # 获得年报信息
                self.get_annual_info(session, pri_pid, pri_type, data)

                data_list.append(data)
            except Exception as e:
                self.log.exception(e)

        return self.sent_to_target(data_list)
