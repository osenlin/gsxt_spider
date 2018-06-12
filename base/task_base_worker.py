#!/usr/bin/env python
# encoding: utf-8
"""
"""
import os
import random
import time

import requests
from fake_useragent import UserAgent

from common import util
from common.global_resource import global_log, target_db, source_db, is_debug, target_db_new
from common.proxy_local import ProxyLocal
from common.queue_mq_thread import MqQueueThread
from config.conf import remote_proxy_conf, parse_mq_conf, report_mq_conf


class TaskBaseWorker(object):
    #  -2 公司名称太短,
    # -1 公司名称不符合规格,
    # 0 代表抓取失败
    # 1 代表已经抓完了
    # 2 没有搜索到任何信息
    # 3 代表当前关键字搜索出列表结果 但是没有找到完整匹配关键字
    CRAWL_SHORT_NAME = -2
    CRAWL_INVALID_NAME = -1
    CRAWL_UN_FINISH = 0
    CRAWL_FINISH = 1
    CRAWL_NOTHING_FIND = 2
    CRAWL_NO_MATCH_NAME = 3

    # 过期时间
    RETENTION_TIME = 3600 * 24 * 7

    USER_AGENT_LIST = [
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
        "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 "
        "Safari/536.11",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 "
        "Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
    ]

    # 静态代理配置文件相对路径
    PROXY_PATH = '/config/proxies_200.txt'

    # 动态代理
    PROXY_TYPE_DYNAMIC = 1
    # 静态代理
    PROXY_TYPE_STATIC = 2

    def __init__(self, **kwargs):

        # 获得项目根路径
        self.base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

        # 初始化配置信息
        self.host = kwargs.get('host', '')
        self.logfile = kwargs.get('logfile', '')
        self.province = kwargs.get('province', '')
        self.target_table = kwargs.get('target_table', '')
        self.source_table = kwargs.get('source_table', '')
        self.crawl_flag = kwargs.get('crawl_flag', 'crawl_online')
        self.search_table = kwargs.get('search_table')
        self.retention_time = kwargs.get('retention_time')
        if self.retention_time is None:
            self.retention_time = self.RETENTION_TIME
        else:
            self.retention_time = eval(self.retention_time)

        self.parse_mq_conf = parse_mq_conf
        self.is_parse_mq_open = eval(kwargs.get('is_parse_mq_open', 'False'))
        self.report_mq_conf = report_mq_conf
        self.is_report_mq_open = eval(kwargs.get('is_report_mq_open', 'False'))

        # 默认动态代理
        self.proxy_type = self.PROXY_TYPE_STATIC
        self.ssl_type = 'http'

        # 打开日志
        self.log = global_log

        # 打开代理
        self.proxy_local = ProxyLocal(proxies_file=self.base_path + self.PROXY_PATH, log=self.log)

        # 目标库
        self.target_db = target_db

        # 新网页库
        self.target_db_new = target_db_new

        # 搜索列表存储表
        self.source_db = source_db

        # 开启消息队列线程
        self.parse_mq_thread = MqQueueThread(
            server_conf=self.parse_mq_conf, log=self.log, is_open=self.is_parse_mq_open)
        self.parse_mq_thread.start()

        self.report_mq_thread = MqQueueThread(
            server_conf=self.report_mq_conf, log=self.log, is_open=self.is_report_mq_open)
        self.report_mq_thread.start()

        try:
            self.ua = UserAgent()
        except:
            self.ua = None

        # 判断当前是什么模式
        if is_debug is True:
            self.log.info('当前处于调试模式: is_debug = True')
        else:
            self.log.info('当前处于上线模式: is_debug = False')

    def __del__(self):

        self.log.info('退出解析消息线程..')
        self.parse_mq_thread.close()
        self.parse_mq_thread.join()
        del self.parse_mq_thread

        self.log.info('退出反馈消息线程..')
        self.report_mq_thread.close()
        self.report_mq_thread.join()
        del self.report_mq_thread

    # 离线工商抓取入口
    def query_task(self, item):
        pass

    # 在线工商抓取入口
    def query_online_task(self, item):
        pass

    def task_request(self, session, requester, url, retry=3, **kwargs):
        kwargs['timeout'] = kwargs.get('timeout', 20)
        total_start_time = time.time()
        start_time = 0

        for _ in xrange(retry):
            try:
                start_time = time.time()
                result = requester(url=url, **kwargs)
                end_time = time.time()
                if result.status_code == 200:
                    if not util.check_html(result.text):
                        self.log.error('无效用户: proxy = {proxy} url = {url}'.format(
                            url=url, proxy=session.proxies))
                        return None
                    return result

                # 打印代理异常信息
                self.log.warn(
                    '代理状态码异常: proxy = {proxy} status_code = {code}  url = {url} used_time = {used}s'.format(
                        proxy=session.proxies, code=result.status_code, url=url, used=end_time - start_time))

                # 反馈代理
                self.report_session_proxy(session)

                # 出错时才更换代理
                session.proxies = self.get_random_proxy()
            except Exception as e:
                # 反馈代理
                self.report_session_proxy(session)

                end_time = time.time()
                self.log.warn('代理访问异常: proxy = {proxy} url = {url} used_time = {used}s msg = {msg}'.format(
                    url=url, used=end_time - start_time, msg=e.message, proxy=session.proxies))
                session.proxies = self.get_random_proxy()

        # try:
        #     session.proxies = self.get_socks5_proxy()
        #     start_time = time.time()
        #     result = requester(url=url, **kwargs)
        #     end_time = time.time()
        #     if result.status_code == 200:
        #         if not util.check_html(result.text):
        #             self.log.error('无效用户: proxy = {proxy} url = {url}'.format(
        #                 url=url, proxy=session.proxies))
        #             return None
        #         return result
        #     self.log.warn('静态代理状态码异常: proxy = {proxy} status_code = {code} url = {url} used_time = {used}s'.format(
        #         proxy=session.proxies, code=result.status_code, url=url, used=end_time - start_time))
        # except Exception as e:
        #     end_time = time.time()
        #     self.log.warn('静态代理访问异常: proxy = {proxy} url = {url} used_time = {used}s msg = {msg}'.format(
        #         proxy=session.proxies, url=url, used=end_time - start_time, msg=e.message))

        total_end_time = time.time()
        self.log.error('访问失败: url = {url} total time = {total}s'.format(
            url=url, total=total_end_time - total_start_time))
        return None

    # 获得浏览器信息
    def get_user_agent(self):

        if self.ua is None:
            return self.USER_AGENT_LIST[random.randint(0, len(self.USER_AGENT_LIST) - 1)]

        return str(self.ua.random)

    def get_new_session(self, host=None):
        session = requests.session()
        if host is None:
            host = self.host

        session.proxies = self.get_random_proxy(host=host)
        session.headers['User-Agent'] = self.get_user_agent()
        return session

    # 通过session获取代理信息
    def report_session_proxy(self, session):
        # 出现异常则反馈代理
        self.report_proxy(session.proxies[self.ssl_type])

    def report_proxy(self, proxy):
        if proxy is None:
            self.log.warn('代理为None')
            return

        # # 如果是静态代理则不反馈
        # if self.proxy_type == self.PROXY_TYPE_STATIC:
        #     return

        proxy_conf = remote_proxy_conf

        try:
            # if self.proxy_type == self.PROXY_TYPE_DYNAMIC:
            #     proxy_conf = remote_proxy_conf
            # # elif self.proxy_type == self.PROXY_TYPE_DYNAMIC_NEW:
            # #     proxy_conf = remote_proxy_conf_new
            # else:
            #     self.log.warn('没有匹配到任何动态代理配置..')
            #     return

            bad_ip = proxy.split('@')[1]
            if self.proxy_local.find_ip(bad_ip):
                return

            url = 'http://{host}:{port}/bad_proxy/{h}/{bad_ip}'.format(
                h=self.host,
                host=proxy_conf['host'],
                port=proxy_conf['port'],
                bad_ip=bad_ip
            )
            r = requests.get(url, timeout=2)
            if r is None or r.status_code != 200:
                self.log.warn('反馈动态代理失败: proxy = {proxy}'.format(proxy=proxy))
                return
        except Exception as e:
            self.log.error('反馈动态代理失败: proxy = {proxy}'.format(proxy=proxy))
            self.log.exception(e)
            return

        self.log.info('反馈失败代理成功: proxy = {proxy}'.format(proxy=proxy))

    # 随机选择代理
    def get_random_proxy(self, host=None):
        proxy_type = random.choice([self.PROXY_TYPE_STATIC, self.PROXY_TYPE_DYNAMIC])

        # 如果是动态代理
        if proxy_type == self.PROXY_TYPE_DYNAMIC:
            return self.get_dynamic_proxy(host)

        # 否则返回socks5的静态代理
        static_proxy = self.proxy_local.get_local_proxy()['socks5']
        proxies = {self.ssl_type: static_proxy}
        self.log.info('静态代理 ip = {ip}'.format(ip=static_proxy))
        return proxies

    # 获取socks5 代理接口
    # def get_socks5_proxy(self, host=None):
    #     if self.proxy_type != self.PROXY_TYPE_STATIC:
    #         return self.get_http_proxy(host)
    #
    #     # 如果是静态代理 则获取静态代理
    #     proxies = {self.ssl_type: self.proxy_local.get_local_proxy()['socks5']}
    #     return proxies

    def get_dynamic_proxy(self, host):

        for _ in xrange(3):
            try:
                r = requests.get('http://{host}:{port}/proxy/{h}'.format(
                    h=host, host=remote_proxy_conf['host'], port=remote_proxy_conf['port']),
                    timeout=10)
                if r is None or r.status_code != 200 or 'failed' in r.text or 'False' in r.text:
                    time.sleep(1)
                    self.log.warn("动态代理服务异常, 重试...")
                    continue

                proxies = {self.ssl_type: 'http://{host}'.format(host=r.text)}
                self.log.info('鲲鹏 ip = {ip}'.format(ip=r.text))
                return proxies
            except Exception as e:
                self.log.error("动态代理访问异常:")
                self.log.exception(e)
                time.sleep(1)

        proxy = self.proxy_local.get_local_proxy()['http']
        self.log.warn("获取动态代理失败, 暂时使用静态代理: {}".format(proxy))
        return {self.ssl_type: proxy}

    # 更改为默认先使用本地代理服务
    def get_http_proxy(self, host=None):
        proxies = None
        if host is None:
            host = self.host
        try:
            if self.proxy_type == self.PROXY_TYPE_DYNAMIC:
                return self.get_dynamic_proxy(host)
            elif self.proxy_type == self.PROXY_TYPE_STATIC:
                static_proxy = self.proxy_local.get_local_proxy()['http']
                proxies = {self.ssl_type: static_proxy}
                self.log.info('静态代理 ip = {ip}'.format(ip=static_proxy))
        except Exception as e:
            static_proxy = self.proxy_local.get_local_proxy()['http']
            proxies = {self.ssl_type: static_proxy}
            self.log.info('静态代理 ip = {ip}'.format(ip=static_proxy))
            self.log.exception(e)
        return proxies
