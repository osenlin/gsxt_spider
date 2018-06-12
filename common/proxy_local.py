# coding=utf-8
import random
import time

import requests
import threadpool

from config.conf import static_proxy_url


def test_run_task(item, index, log):
    session = requests.session()
    url_list = [
        'http://www.baidu.com',
        'http://www.163.com/',
        'http://www.sohu.com/',
        'http://www.qq.com/',
    ]
    try:
        # 防呆措施
        rand_inx = random.randint(0, len(url_list) - 1)
        if rand_inx > len(url_list) - 1:
            rand_inx = 0
        if rand_inx < 0:
            rand_inx = len(url_list) - 1

        proxy = {'http': item['http']}
        session.proxies = proxy
        resp = session.get(url_list[rand_inx], timeout=10)
        if resp.status_code == 200:
            log.info(str(session.proxies) + ':SUCCESS:' + str(index))
        else:
            log.info(str(session.proxies) + ':FAIL:' + str(index))
            item['status'] = 0
    except Exception:
        item['status'] = 0
        log.info(str(session.proxies) + ':exception:' + str(index))


class ProxyLocal(object):
    obj_count = 0
    statis_proxy = []

    def __init__(self, proxies_file='./config/proxies_200.txt', log=None, test=False):

        self.log = log

        self.proxy_list = []

        self.search_list = {}

        self.index = 0

        self.proxies_file = proxies_file

        self.load_and_test(test=test)

    def load_and_test(self, test=False):
        # 加载代理信息
        ProxyLocal.load_steady_proxy(self.proxies_file, log=self.log)

        if test:
            ProxyLocal.test_steady_proxy(ProxyLocal.statis_proxy, self.log)

        # 加载代理信息
        self.proxy_list = [item for item in ProxyLocal.statis_proxy if item['status'] == 1]
        self.log.info('加载代理个数为: {length}'.format(length=len(self.proxy_list)))

        # 加载用于搜索的代理信息
        for item in ProxyLocal.statis_proxy:
            ip = item['http'].split('@')[1]
            self.search_list[ip] = ip
            # self.log.info(ip)

        if len(self.proxy_list) <= 100:
            self.log.error('代理数目异常!!!!!!!!!!!')

        # 随机初始位置
        self.index = random.randint(0, len(self.proxy_list) - 1) if len(self.proxy_list) >= 1 else 0

    def find_ip(self, bad_ip):
        if bad_ip in self.search_list:
            return True
        return False

    @staticmethod
    def load_steady_proxy(proxies_file, log=None):
        if ProxyLocal.obj_count != 0:
            return

        ProxyLocal.obj_count = 1

        # 访问url获取最新的静态代理信息
        try:
            r = requests.get(static_proxy_url, timeout=10)
            if r is None or r.status_code != 200:
                raise Exception("代理更新异常...")

            with open(proxies_file, "w") as p_write:
                line_list = r.text.strip().split('\n')
                for line in line_list:
                    line = line.strip().strip("\r").strip("\n")
                    if len(line) <= 0:
                        continue
                    proxy = {'http': 'http://' + line, 'status': 1}
                    if '7777' in line:
                        new_line = line.replace('7777', '55555')
                        proxy['socks5'] = 'socks5://' + new_line
                    elif '8088' in line:
                        new_line = line.replace('8088', '1088')
                        proxy['socks5'] = 'socks5://' + new_line
                    else:
                        proxy['socks5'] = 'http://' + line
                        log.error('没有对应端口信息: {}'.format(line))

                    ProxyLocal.statis_proxy.append(proxy)
                    p_write.write(line + "\r\n")
                log.info('从远端更新代理成功....')
        except Exception as e:
            log.error("从远端更新失败, 使用本地文件...")
            log.exception(e)
            with open(proxies_file) as fp:
                for line in fp:
                    line = line.strip().strip("\r").strip("\n")
                    if len(line) <= 0:
                        continue
                    proxy = {'http': 'http://' + line, 'status': 1}
                    if '7777' in line:
                        new_line = line.replace('7777', '55555')
                        proxy['socks5'] = 'socks5://' + new_line
                    elif '8088' in line:
                        new_line = line.replace('8088', '1088')
                        proxy['socks5'] = 'socks5://' + new_line
                    else:
                        proxy['socks5'] = 'http://' + line
                        log.error('没有对应端口信息: {}'.format(line))

                    ProxyLocal.statis_proxy.append(proxy)

    def get_local_proxy(self):

        # 这里需要加锁

        length = len(self.proxy_list)
        if length <= 0:
            return None
        self.index += 1
        self.index %= length

        # 这里需要加锁

        return self.proxy_list[self.index]

    @staticmethod
    def test_steady_proxy(proxy_list, log):
        log.info('开始进行代理测试..')
        start_time = time.time()
        pool = threadpool.ThreadPool(50)
        for index, item in enumerate(proxy_list):
            req = threadpool.WorkRequest(test_run_task, [item, index, log])
            pool.putRequest(req)
        pool.wait()
        end_time = time.time()
        log.info('代理测试完成..')
        log.info('use time: {use_time}'.format(use_time=end_time - start_time))


if __name__ == '__main__':
    from common.global_resource import global_log

    proxies = ProxyLocal('../config/proxies_200.txt', test=True, log=global_log)
