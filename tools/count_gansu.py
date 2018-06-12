#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: count_gansu.py
@time: 2017/7/21 17:03
"""
import sys
from email.header import Header
from email.mime.text import MIMEText
from smtplib import SMTP

sys.path.append('../')

from common.mongo import MongDb

from logger import Gsxtlogger

mail_from_addr = 'datamonitor@haizhi.com'
mail_password = 'LcoS!WKXmWmFu2Or'
mail_to_addrs = ['youfeng@haizhi.com', 'zhangjun@haizhi.com']
# mail_to_addrs = ['youfeng@haizhi.com']

company_data_conf = {
    'host': '172.16.215.2',
    'port': 40042,
    'db': 'company_data',
    'username': 'work',
    'password': 'haizhi'
}

app_data_conf = {
    'host': '172.16.215.16',
    'port': 40042,
    'db': 'app_data',
    'username': 'work',
    'password': 'haizhi'
}

log = Gsxtlogger('count_gansu.log').get_logger()

company_data_db = MongDb(company_data_conf['host'], company_data_conf['port'], company_data_conf['db'],
                         company_data_conf['username'],
                         company_data_conf['password'], log=log)

app_data_db = MongDb(app_data_conf['host'], app_data_conf['port'], app_data_conf['db'],
                     app_data_conf['username'],
                     app_data_conf['password'], log=log)


def get_now_time():
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def main():
    log.info("开始统计甘肃抓取情况...")

    company_table = 'offline_all_list'
    link_table = 'online_all_search'
    app_data_table = 'enterprise_data_gov'

    # 已抓取到链接的企业个数
    link_crawl_num = 0

    # 已抓取到详情页的企业个数
    detail_crawl_num = 0

    # 已正常解析入库的个数
    parse_num = 0

    # 当前遍历到的位置
    current_point = 0

    for item in company_data_db.traverse_batch(company_table, {'province': 'gansu'}):
        company_name = item.get('company_name')
        current_point += 1

        if current_point % 1000 == 0:
            log.info("当前遍历进度: {}".format(current_point))

        if company_name is None:
            log.error("获取公司名称失败....")
            continue

        link_item = company_data_db.find_one(link_table, {'search_name': company_name})
        if link_item is None:
            continue

        link_crawl_num += 1

        crawl_online = link_item.get('crawl_online')
        if crawl_online is None:
            continue

        if crawl_online != 1:
            continue

        detail_crawl_num += 1

        if app_data_db.find_one(app_data_table, {'company': company_name}) is None:
            continue

        parse_num += 1

    # 初始化字符串
    mail_text = ""

    text = "需要抓取的总企业数目: {}".format(current_point)
    mail_text += text + '\r\n'
    log.info(text)

    text = "已经抓取到链接企业个数: {}".format(link_crawl_num)
    mail_text += text + '\r\n'
    log.info(text)

    text = "已经抓取到详情页企业个数: {}".format(detail_crawl_num)
    mail_text += text + '\r\n'
    log.info(text)

    text = "已经完整更新入库企业个数: {}".format(parse_num)
    mail_text += text + '\r\n'
    log.info(text)

    text = "更新比例: {}%".format(parse_num * 100 / current_point)
    mail_text += text + '\r\n'
    log.info(text)

    send_email(
        mail_from_addr,
        mail_password,
        mail_to_addrs,
        '甘肃工商企业信息更新统计 - %s' % get_now_time(),
        MIMEText(mail_text, 'plain', 'utf-8')
    )


def send_email(from_addr, password, to_addrs, subject, msg, smtp_host="smtp.weibangong.com", smtp_port=465):
    email_client = SMTP(smtp_host, smtp_port)
    email_client.login(from_addr, password)
    msg['Subject'] = Header(subject, 'utf-8')
    msg['From'] = from_addr
    msg['To'] = str(to_addrs)
    email_client.sendmail(from_addr, to_addrs, msg.as_string())
    email_client.quit()


if __name__ == '__main__':
    import time

    while True:
        main()

        time.sleep(12 * 3600)
