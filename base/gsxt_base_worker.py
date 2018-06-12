# -*- coding: utf-8 -*-
# !/usr/bin/env python
import json
import time

import requests

from base.task_base_worker import TaskBaseWorker
from common import util
from common.generator_checker import create_check_object
from common.global_field import Model
from common.global_resource import is_debug
from common.mongo import MongDb
from config.conf import captcha_geetest_conf


class GsxtBaseWorker(TaskBaseWorker):
    # 关键字不合法
    SEARCH_KEYWORD_INVALID = -2
    # 搜索错误
    SEARCH_ERROR = -1
    # 搜索没有数据
    SEARCH_NOTHING_FIND = 0
    # 成功搜索到数据
    SEARCH_SUCCESS = 1

    # 抓取状态
    STATUS_SUCCESS = 'success'
    STATUS_FAIL = 'fail'
    STATUS_NOT_EXIST = 'not exist'

    # cookie table
    COOKIE_TABLE = 'gsxt_cookie_list'

    # 记录错误次数
    ERROR_TIMES = 'error_times'

    # 最大尝试次数
    MAX_ERROR_TIMES = 15

    def __init__(self, **kwargs):
        TaskBaseWorker.__init__(self, **kwargs)

        # 滑动验证码配置信息
        self.captcha_geetest_conf = captcha_geetest_conf

        try:
            self.check_obj = create_check_object(kwargs, self.province, self.log)
            self.check_obj = None
            self.log.warn('校验类初始化完成..')
        except Exception as e:
            self.check_obj = None
            self.log.warn('初始化校验类异常..')
            self.log.exception(e)

        # 脏数据样本
        self.dirty_table = 'dirty_crawl_data'

        # 列表页名称
        self.target_db.create_index(self.target_table, [("search_name", MongDb.ASCENDING)])
        # 入库时间建立索引
        self.target_db.create_index(self.target_table, [("in_time", MongDb.ASCENDING)])

        # 列表页名称
        self.target_db_new.create_index(self.target_table, [("search_name", MongDb.ASCENDING)])
        # 入库时间建立索引
        self.target_db_new.create_index(self.target_table, [("in_time", MongDb.ASCENDING)])

        # 站点索引
        self.source_db.create_index(self.source_table, [('province', MongDb.ASCENDING)])
        self.source_db.create_index(self.source_table, [('company_name', MongDb.ASCENDING)])
        self.source_db.create_index(self.source_table, [(self.crawl_flag, MongDb.ASCENDING)])

        # 搜索结果列表
        if self.search_table is not None:
            self.source_db.create_index(self.search_table, [('in_time', MongDb.ASCENDING)])
            self.source_db.create_index(self.search_table, [('province', MongDb.ASCENDING)])
            self.source_db.create_index(self.search_table, [('company_name', MongDb.ASCENDING)])
            self.source_db.create_index(self.search_table, [('unified_social_credit_code', MongDb.ASCENDING)])
            self.source_db.create_index(self.search_table, [('seed_code', MongDb.ASCENDING)])
            self.source_db.create_index(self.search_table, [('search_name', MongDb.ASCENDING)])
            self.source_db.create_index(self.search_table, [('priority', MongDb.ASCENDING)])

    def cracker_search_list_content(self, keyword, item=None):

        url = 'http://{host}/index.jspx'.format(host=self.host)
        json_data, content = self.get_captcha_geetest_full(url, '#searchText', '#click', keyword, '#searchtips')
        if content is None:
            return None

        # 这里存储cookie
        cookie_list = json_data.get('cookies', None)
        if cookie_list is None:
            self.log.error('没有cookie信息, 保存cookie失败..')
            return content

        cookie = ''
        length = len(cookie_list)
        for index, it in enumerate(cookie_list):
            cookie += it['name'] + '=' + it['value']
            if index != length - 1:
                cookie += '; '

        if item is None:
            self.source_db.save(self.COOKIE_TABLE, {
                '_id': self.province,
                'Cookie': cookie,
                'in_time': util.get_now_time()
            })
        else:
            item['Cookie'] = cookie
            item['in_time'] = util.get_now_time()
            self.source_db.save(self.COOKIE_TABLE, item)

        return content

    def get_search_list_content(self, keyword, session):
        url = 'http://{host}/searchList.jspx?top=top&checkNo=&searchType=1&entName={keyword}'.format(
            keyword=keyword, host=self.host)

        # 先读取cookie
        item = self.source_db.find_one(self.COOKIE_TABLE, {'_id': self.province})
        if item is None:
            self.log.info('没有搜索到cookie信息: province = {province} keyword = {keyword}'.format(
                province=self.province, keyword=keyword))
            return self.cracker_search_list_content(keyword)

        cookie = item.get('Cookie', None)
        if cookie is None:
            self.log.info('获得cookie信息为None: province = {province} keyword = {keyword}'.format(
                province=self.province, keyword=keyword))
            return self.cracker_search_list_content(keyword, item=item)

        session.headers['Cookie'] = cookie.replace(' ', '')
        r = self.task_request(session, session.get, url)
        if r is None:
            self.log.info('通过添加Cookie没有访问到页面信息: province = {province} keyword = {keyword}'.format(
                province=self.province, keyword=keyword))
            return self.cracker_search_list_content(keyword, item=item)

        if r.text.find('验证码不正确') != -1:
            self.log.error('cookie 已过时...')
            return self.cracker_search_list_content(keyword, item=item)

        self.log.info('通过cookie获得数据...company = {company}'.format(company=keyword))
        return r.text

    # 获得model
    def get_model(self, _id, seed, search_name, province, data_list=None):
        if data_list is None:
            data_list = {}
        data = dict(_id=_id,
                    seed=seed,
                    search_name=search_name,
                    province=province,
                    datalist=data_list,
                    in_time=util.get_now_time())
        data[self.crawl_flag] = self.CRAWL_UN_FINISH
        data[self.ERROR_TIMES] = 0
        return data

    # 按列表添加数据
    def append_model_list(self, model, data_type, item_list, classify=Model.type_list):
        if model is None:
            return

        data_list = model.get('datalist', None)
        if not isinstance(data_list, dict):
            return

        info = data_list.get(data_type, None)
        if info is None:
            data_list[data_type] = {classify: item_list}
            return

        info[classify] = item_list

    # 添加 item
    def append_model_item(self, model, data_type, detail_info, classify=Model.type_list):
        if model is None:
            return

        if 'url' not in detail_info:
            raise StandardError('detail_info 没有url信息')

        if 'text' not in detail_info:
            raise StandardError('detail_info 没有url信息')

        if 'status' not in detail_info:
            raise StandardError('detail_info 没有url信息')

        data_list = model.get('datalist', None)
        if not isinstance(data_list, dict):
            return

        info = data_list.get(data_type, None)
        if info is None:
            info = {classify: [detail_info]}
            data_list[data_type] = info
            return
        classify_list = info.get(classify, None)
        if classify_list is None:
            info[classify] = [detail_info]
            return

        classify_list.append(detail_info)

    def append_model(self, model, data_type, url, text,
                     status=STATUS_SUCCESS,
                     year=None,
                     classify=Model.type_list,
                     post_data=None):
        '''
        添加数据
        :param status: 抓取状态
        :param post_data:
        :param year:
        :param model: 数据文档
        :param data_type: base_info or other_info
        :param classify: list or detail
        :param url: 网页url
        :param text: 网页文本
        :return:
        '''
        if model is None:
            return

        data_list = model.get('datalist', None)
        if not isinstance(data_list, dict):
            return

        detail_info = {'url': url, 'text': text, 'status': status}
        if year is not None:
            detail_info['year'] = year

        if post_data is not None:
            if isinstance(post_data, dict):
                detail_info['post_data'] = json.dumps(post_data)

        info = data_list.get(data_type, None)
        if info is None:
            info = {classify: [detail_info]}
            data_list[data_type] = info
            return
        classify_list = info.get(classify, None)
        if classify_list is None:
            info[classify] = [detail_info]
            return

        classify_list.append(detail_info)

    def get_search_list_html(self, keyword, session):
        return [], self.SEARCH_ERROR

    def get_detail_html_list(self, seed, session, param_list):
        return 0, []

    # 存储搜索列表
    def save_search_list(self, company, code, param_list):

        match_param = None
        if self.search_table is None:
            return param_list, match_param

        rank = 1
        data_list = []
        for param in param_list:

            # 必须要有列表名 才进行存储
            search_name = param.get('search_name')
            if search_name is None:
                continue

            # 取得解析出的统一社会信用号代码信息
            unified_social_credit_code = param.get('unified_social_credit_code')

            # 不在参数中存储统一社会信用号
            if 'unified_social_credit_code' in param:
                unified_social_credit_code = unified_social_credit_code.strip().upper()
                param.pop('unified_social_credit_code')

            if company is not None:
                replace_name_1 = company.replace('(', '（').replace(')', '）')
                replace_name_2 = company.replace('（', '(').replace('）', ')')
            else:
                replace_name_1 = ''
                replace_name_2 = ''

            # 确定优先级, 如果种子名称跟列表名称一样 则优先级最高为 0
            if search_name == company \
                    or search_name == replace_name_1 \
                    or search_name == replace_name_2 \
                    or (code == unified_social_credit_code and code is not None):
                priority = 0
            else:
                priority = 1

            data = {
                # 以搜索列表名与省份信息作为唯一主键
                '_id': util.generator_id({'priority': priority}, search_name, self.province),
                'search_name': search_name,
                'province': self.province,
                'in_time': util.get_now_time(),
                'param': param,
                'rank': rank,
                'priority': priority,
                self.ERROR_TIMES: 0,
            }

            # 加入注册码
            if unified_social_credit_code is not None:
                data['unified_social_credit_code'] = unified_social_credit_code

            # 添加搜索种子信息
            if company is not None:
                data['company_name'] = company
            if code is not None:
                data['seed_code'] = code

            # 如果是完全匹配则重置抓取状态信息
            if priority == 0:
                data[self.crawl_flag] = 0
                match_param = param.copy()

            data_list.append(data)
            rank += 1

        # 调试模式下不实际插入数据
        #if not is_debug:
        self.source_db.insert_batch_data(self.search_table, data_list)

        return param_list, match_param

    # 获得加密后的pripid
    def get_encry_pripid(self, encry_url, script):
        session = requests.session()
        session.headers['Content-Type'] = 'application/json'

        post_data = {"script": script}

        try:
            r = session.post(encry_url, json=post_data)
            if r.status_code == 200:
                return r.text
        except Exception as e:
            self.log.exception(e)

        return None

    # 获得加密后的信息
    def get_encry_pripid_detail(self, encry_url, script):
        encry_href = self.get_encry_pripid(encry_url, script)
        if encry_href is None:
            return None

        json_data = util.json_loads(encry_href)
        if json_data is None:
            return None

        error = json_data.get('error', 'fail')
        if error == 'fail':
            return None
        if error is not None:
            return None

        result = json_data.get('result', None)
        if result is None:
            return None

        return result

    def get_captcha_geetest_by_proxy(self, url, input_selector, search, keyword, result,
                                     success=None,
                                     proxy=None,
                                     add_link=None,
                                     click_first=None):

        session = requests.session()
        session.headers['Content-Type'] = 'application/json'

        post_data = {
            "url": url,
            "searchInputSelector": input_selector,
            "searchBtnSelector": search,
            "searchText": keyword,
            "resultIndicatorSelector": result,
        }
        if success is not None:
            post_data["successIndicatorSelector"] = success

        if proxy is not None:
            post_data["proxy"] = proxy

        if add_link is not None:
            post_data['__SPECIAL_HACK_FOR_CHONGQING__ADD_LINK__'] = add_link

        if click_first is not None:
            post_data['__SPECIAL_HACK_FOR_CHONGQING__CLICK_FIRST__'] = click_first

        try:
            r = session.post(self.captcha_geetest_conf['url'], json=post_data)
            if r.status_code != 200:
                # self.log.warn('验证码服务请求错误: province = {province} status = {status} keyword = {key}'.format(
                #     province=self.province, status=r.status_code, key=keyword))
                return None, None

            json_data = util.json_loads(r.text)
            if json_data is None:
                self.log.error('json数据转换失败...text = {text}'.format(text=r.text))
                return None, None

            if not json_data.get('success', False):
                status = json_data.get('status', 1)
                if status == -1:
                    self.report_proxy(proxy)

                self.log.warn('验证码识别失败: province = {province} keyword = {key} 识别状态码: status = {status}'.format(
                    province=self.province, key=keyword, status=json_data.get('status', 0)))
                return json_data, None

            content = json_data.get('content', None)
            if content is None:
                self.log.error('找不到相对应的content字段: province = {province} key = {key} text = {text}'.format(
                    province=self.province, key=keyword, text=r.text))
                return None, None

            self.log.info('验证码破解成功: province = {province} key = {key}'.format(
                province=self.province, key=keyword))
            return json_data, content
        except Exception as e:
            self.log.exception(e)
        # self.log.error('验证码识别异常: province = {province} key = {key}'.format(
        #     province=self.province, key=keyword))
        return None, None

    def get_captcha_geetest_full(self, url, input_selector, search, keyword, result, success=None,
                                 add_link=None,
                                 click_first=None):
        proxy = self.get_http_proxy()['http'][7:]
        return self.get_captcha_geetest_by_proxy(url, input_selector, search, keyword, result,
                                                 success=success,
                                                 proxy=proxy,
                                                 add_link=add_link,
                                                 click_first=click_first)

    def get_captcha_geetest(self, url, input_selector, search, keyword, result,
                            success=None,
                            add_link=None,
                            click_first=None):

        json_data, content = self.get_captcha_geetest_full(url, input_selector, search, keyword, result,
                                                           success=success,
                                                           add_link=add_link,
                                                           click_first=click_first)
        return content

    # 选择需要插入的数据库
    def choose_database(self, target_table, data_list):
        length = len(data_list)
        data_list_old = []
        data_list_new = []

        for item in data_list:
            _id = item.get('_id')
            if _id is None:
                self.log.error('没有_id信息')
                continue

            result = self.target_db.find_one(target_table, {'_id': _id}, ['_id'])
            if result is None:
                data_list_new.append(item)
            else:
                data_list_old.append(item)

        self.target_db.insert_batch_data(target_table, data_list_old)
        self.target_db_new.insert_batch_data(target_table, data_list_new)

        return length

    def sent_to_target(self, data_list, name_error_count=0):

        # 如果存入抛异常, 则直接标示为没有任何存入数据
        return self.choose_database(self.target_table, data_list) + name_error_count, data_list

    def get_searchitem_by_code(self, code):
        return self.source_db.find_one(self.search_table,
                                       {'unified_social_credit_code': code, 'province': self.province})

    def get_searchitem_by_name(self, company):
        search_list = []
        if self.search_table is None:
            self.log.error('没有search_table信息')
            return None

        if company is None or company == '':
            self.log.error('关键字信息不正确..')
            return None

        if '(' in company and ')' in company:
            search_list.append(company)
            search_list.append(company.replace('(', '（').replace(')', '）'))
        elif '（' in company and '）' in company:
            search_list.append(company)
            search_list.append(company.replace('（', '(').replace('）', ')'))
        else:
            search_list.append(company)

        for search_name in search_list:
            search_item = self.source_db.find_one(self.search_table,
                                                  {'search_name': search_name, 'province': self.province})
            if search_item is not None:
                return search_item

        return None

    # 通过列表页链接信息获取详情页信息
    def get_detail_html_by_searchinfo(self, seed, session, search_item):
        param = search_item.get('param', {})
        result_length, detail_list = self.get_detail_html_list(seed, session, [param])
        if result_length > 0:
            return self.CRAWL_FINISH, detail_list

        self.log.warn('通过详情页链接访问数据失败, 删除链接: seed = {seed}'.format(seed=seed))
        self.source_db.delete(self.search_table, {'_id': search_item.get('_id')})
        return self.CRAWL_UN_FINISH, []

    # -2 公司名称太短, -1 公司名称不符合规格, 2 剔除搜索过没有信息的关键字 1 代表已经抓完了
    def set_crawl_flag(self, item, flag, cur_time=None):
        '''
        {
        company:'企业名称',
        crawl_online:
        -2 公司名称太短,
        -1 公司名称不符合规格,
        0 代表抓取失败
        1 代表已经抓完了
        2 剔除搜索过没有信息的关键字
        3 代表当前关键字搜索出列表结果 但是没有找到完整匹配关键字
        crawl_online_time: '2017-02-22 19:03:43',
        query:'企业名称',
        }
        '''

        # 获得当前时间
        time_key = self.crawl_flag + '_time'
        if cur_time is None:
            cur_time = util.get_now_time()

        # 更新字段信息
        item[time_key] = cur_time
        item[self.crawl_flag] = flag

        # 更新信息 调试模式下不存储数据
        if not is_debug:
            self.source_db.save(self.source_table, item)

    # 反馈抓取情况
    def report_crawl_status(self, query_name, crawl_flag,
                            start_schedule_time='',
                            detail_name='',
                            cur_time=None):
        if is_debug:
            return

        # 获得当前时间
        time_key = self.crawl_flag + '_time'
        if cur_time is None:
            cur_time = util.get_now_time()

        crawl_status = {
            'company': detail_name,
            self.crawl_flag: crawl_flag,
            time_key: cur_time,
            'query': query_name,
            'start_schedule_time': start_schedule_time,
        }

        parse_status = {
            'company': detail_name,
            'province': self.province,
        }

        # 反馈抓取情况
        if not is_debug:
            self.report_mq_thread.push_msg(json.dumps(crawl_status))

        # 如果抓取成功了, 反馈到解析模块
        if crawl_flag == self.CRAWL_FINISH:
            if not is_debug:
                self.parse_mq_thread.push_msg(json.dumps(parse_status))

    # 搜索抓取到的链接信息
    def get_searchlist_item(self, company, seed_code):
        if company is not None:
            return self.get_searchitem_by_name(company)
        if seed_code is not None:
            return self.get_searchitem_by_code(seed_code)
        return None

        # 设置原始表中关键字搜索状态
        #  -2 公司名称太短, -1 公司名称不符合规格, 2 剔除搜索过没有信息的关键字 1 代表已经抓完了

    def set_detail_crawl_flag(self, item, flag):

        # 获得当前时间
        cur_time = util.get_now_time()
        time_key = self.crawl_flag + '_time'

        # 更新字段信息
        item[time_key] = cur_time
        item[self.crawl_flag] = flag

        # 如果错误 则记录错误次数
        if flag == self.CRAWL_UN_FINISH:
            if self.ERROR_TIMES in item:
                item[self.ERROR_TIMES] += 1
            else:
                item[self.ERROR_TIMES] = 1
        else:
            item[self.ERROR_TIMES] = 0

        # 更新信息
        if not is_debug:
            self.source_db.save(self.source_table, item)

    # 判断是否需要抓取 True 需要抓取 False 不需要抓取
    def check_detail_crawl_flag(self, item):

        # 如果是调试模式则直接返回,不校验状态
        if is_debug:
            return True

        # 如果没有抓取记录, 则需要抓取
        if self.crawl_flag not in item:
            return True

        # 如果连时间字段都没有, 则说明还没有抓过
        time_key = self.crawl_flag + '_time'
        pre_time = item.get(time_key, None)
        if pre_time is None:
            return True

        # 获得抓取标记
        flag = item.get(self.crawl_flag)

        # 如果关键字不合法 也不需要再搜索
        if flag == self.CRAWL_INVALID_NAME:
            return False

        # 关键字信息异常 不需要再次搜索
        if flag == self.CRAWL_SHORT_NAME:
            return False

        # 如果时间超过过期时间 则需要重新抓取
        cur_time = util.get_now_time()
        if util.sub_time(cur_time, pre_time) >= self.retention_time:
            self.log.info('cur_time = {cur} pre_time = {pre} _id = {_id}'.format(
                cur=cur_time, pre=pre_time, _id=item.get('_id', '')))
            return True

        # 如果在过期时间内已经完成抓取, 则不再抓取
        if flag == self.CRAWL_FINISH:
            return False

        # 过期时间内搜索没有数据的 也不需要再搜索
        if flag == self.CRAWL_NOTHING_FIND:
            return False

        # 如果抓取失败 且失败次数达到最大次数 则不再进行抓取
        if flag == self.CRAWL_UN_FINISH:
            if self.ERROR_TIMES in item:
                if item[self.ERROR_TIMES] >= self.MAX_ERROR_TIMES:
                    return False
        return True

    # 抓取入口
    def query_online_company(self, item):
        company_name = item.get('company_name')
        seed_code = item.get('unified_social_credit_code')
        start_schedule_time = item.get('start_schedule_time', '')
        if company_name is not None:
            query_name = company_name
        else:
            query_name = seed_code

        crawl_status, match_item = self.crawl_online_company(company_name, seed_code, query_name)
        cur_time = None
        detail_name = ''

        # 如果抓取成功了, 则获得抓取到的准确时间
        if crawl_status is self.CRAWL_FINISH and match_item is not None:
            cur_time = match_item.get('in_time')
            detail_name = match_item.get('_id')

        # 反馈抓取情况
        self.report_crawl_status(query_name, crawl_status,
                                 start_schedule_time=start_schedule_time,
                                 detail_name=detail_name,
                                 cur_time=cur_time)

        # 标记抓取状态
        self.set_crawl_flag(item, crawl_status, cur_time=cur_time)

        return crawl_status

    # 在线工商抓取正式入口
    def crawl_online_company(self, company_name, seed_code, query_name):
        match_item = None
        crawl_status = self.CRAWL_UN_FINISH
        session = self.get_new_session()

        # 先查找是否在网页库中已经抓取了 链接信息
        search_item = self.get_searchlist_item(company_name, seed_code)
        if search_item is not None:
            if company_name is not None:
                self.log.info('列表页中获得访问url信息: company_name = {company_name}'.format(
                    company_name=company_name))
            elif seed_code is not None:
                self.log.info('列表页中获得访问url信息: seed_code = {seed_code}'.format(
                    seed_code=seed_code))
            crawl_status, detail_list = self.get_detail_html_by_searchinfo(query_name, session, search_item)
            if crawl_status == self.CRAWL_FINISH:
                match_item = detail_list[0]

        # 如果通过链接信息没有抓取到数据 则通过滑动验证码破解进行抓取
        if match_item is not None:
            return crawl_status, match_item

        param_list = []
        error_code = self.SEARCH_ERROR
        # 重试三次
        for _ in xrange(3):
            param_list, error_code = self.get_search_list_html(query_name, session)
            # 搜索数据错误
            if error_code == self.SEARCH_ERROR:
                time.sleep(1)
                # 切换动态代理
                session.proxies = self.get_random_proxy()
                continue

            if error_code == self.SEARCH_SUCCESS:
                break

            # 没有搜到数据
            if error_code == self.SEARCH_NOTHING_FIND:
                return self.CRAWL_NOTHING_FIND, match_item

            # 搜索的关键字不合法
            if error_code == self.SEARCH_KEYWORD_INVALID:
                return self.CRAWL_INVALID_NAME, match_item

        # 如果没有搜索成功, 则返回搜索列表页失败
        if error_code != self.SEARCH_SUCCESS:
            return crawl_status, match_item

        self.log.info('企业列表搜索成功: seed = {seed}'.format(seed=query_name))

        # 存储搜索列表基本信息
        data_list, match_param = self.save_search_list(company_name, seed_code, param_list)
        if match_param is None:
            return self.CRAWL_NO_MATCH_NAME, match_item

        result_length, detail_list = self.get_detail_html_list(query_name, session, [match_param])
        if result_length > 0:
            return self.CRAWL_FINISH, detail_list[0]

        return crawl_status, match_item

    def query_online_task(self, item):
        try:
            if not isinstance(item, dict):
                self.log.info('参数错误: item = {item}'.format(item=item))
                return self.province

            company_name = item.get('company_name')
            seed_code = item.get('unified_social_credit_code')

            # 先插入数据 调试模式下 不插入数据
            if not is_debug:
                self.source_db.save(self.source_table, item)

            if company_name is not None:
                self.log.info('开始抓取任务...province = {province} company = {company}'.format(
                    province=self.province, company=company_name))
            elif seed_code is not None:
                self.log.info('开始抓取任务...province = {province} seed_code = {code}'.format(
                    province=self.province, code=seed_code))

            status = self.query_online_company(item)
            if company_name is not None:
                self.log.info('完成抓取任务...province = {province} company = {company} status = {status}'.
                              format(province=self.province, company=company_name, status=status))
            elif seed_code is not None:
                self.log.info('完成抓取任务...province = {province} seed_code = {code} status = {status}'.
                              format(province=self.province, code=seed_code, status=status))

            return self.province
        except Exception as e:
            self.log.exception(e)
        return self.province

    # 判断是否需要抓取 True 需要抓取 False 不需要抓取
    def check_crawl_flag(self, item):

        # 如果是调试模式则直接返回,不校验状态
        if is_debug:
            return True

        # 如果没有抓取记录, 则需要抓取
        if self.crawl_flag not in item:
            return True

        # 如果连时间字段都没有, 则说明还没有抓过
        time_key = self.crawl_flag + '_time'
        pre_time = item.get(time_key)
        if pre_time is None:
            return True

        # 获得抓取标记
        flag = item.get(self.crawl_flag)
        # 如果关键字不合法 也不需要再搜索
        if flag == self.CRAWL_INVALID_NAME:
            return False

        # 关键字信息异常 不需要再次搜索
        if flag == self.CRAWL_SHORT_NAME:
            return False

        # 如果时间超过过期时间 则需要重新抓取
        cur_time = util.get_now_time()
        retention_time = util.sub_time(cur_time, pre_time)
        if retention_time >= self.retention_time:
            self.log.info('cur_time = {cur} pre_time = {pre} _id = {_id}'.format(
                cur=cur_time, pre=pre_time, _id=item.get('_id', '')))
            return True

        # 如果在过期时间内已经完成抓取, 则不再抓取
        if flag == self.CRAWL_FINISH:
            return False

        # 过期时间内搜索没有数据的 也不需要再搜索
        if flag == self.CRAWL_NOTHING_FIND:
            return False

        return True

    # 测试
    def query_offline_company(self, company_name, seed_code, query_name):
        try:
            session = self.get_new_session()
            param_list, error_code = self.get_search_list_html(query_name, session)
            # 搜索数据错误
            if error_code == self.SEARCH_ERROR:
                return self.CRAWL_UN_FINISH

            # 没有搜到数据
            if error_code == self.SEARCH_NOTHING_FIND:
                return self.CRAWL_NOTHING_FIND

            # 搜索的关键字不合法
            if error_code == self.SEARCH_KEYWORD_INVALID:
                return self.CRAWL_INVALID_NAME

            # 获取参数个数
            list_length = len(param_list)

            # 存储搜索列表基本信息
            data_list, match_param = self.save_search_list(company_name, seed_code, param_list)
            if list_length > len(data_list):
                return self.CRAWL_UN_FINISH

            result_length, detail_list = self.get_detail_html_list(query_name, session, param_list)
            if result_length >= list_length:
                # 判断是否由完整匹配的企业名称
                if match_param is not None:
                    return self.CRAWL_FINISH

                return self.CRAWL_NO_MATCH_NAME
        except Exception as e:
            self.log.exception(e)
        return self.CRAWL_UN_FINISH

    def query_task(self, item):

        if not isinstance(item, dict):
            self.log.info('参数错误: item = {item}'.format(item=item))
            return self.province

        # 判断是否需要进行抓取
        if not self.check_crawl_flag(item):
            # self.log.info('当前状态不进行抓取: province = {province} company = {company}'.format(
            #     province=self.province, company=company))
            # self.log.info('item = {item}'.format(item=item))
            return self.province

        company_name = item.get('company_name')
        seed_code = item.get('unified_social_credit_code')
        if company_name is not None:
            query_name = company_name
        else:
            query_name = seed_code

        self.log.info('开始抓取任务...province = {province} query_name = {query_name}'.format(
            province=self.province, query_name=query_name))
        status = self.query_offline_company(company_name, seed_code, query_name)

        self.set_crawl_flag(item, status)
        self.log.info('完成抓取任务...province = {province} query_name = {query_name} status = {status}'.format(
            province=self.province, query_name=query_name, status=status))
        return self.province
