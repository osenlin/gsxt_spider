#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: find_field.py
@time: 2017/7/20 09:41
"""

import sys
import time
from email.header import Header
from email.mime.text import MIMEText
from smtplib import SMTP

from pyquery import PyQuery

sys.path.append('../')

from common.util import get_now_time
from common.mongo import MongDb
from logger import Gsxtlogger

mongo_db_webpage_new = {
    "host": "172.16.215.2",
    "port": 40042,
    "db": "crawl_data_new",
    "username": "work",
    "password": "haizhi",
}

mongo_db_webpage_old = {
    "host": "172.16.215.2",
    "port": 40042,
    "db": "crawl_data",
    "username": "offline",
    "password": "offline",
}

log = Gsxtlogger('find_equity_field.log').get_logger()

target_db_new = MongDb(mongo_db_webpage_new['host'], mongo_db_webpage_new['port'], mongo_db_webpage_new['db'],
                       mongo_db_webpage_new['username'], mongo_db_webpage_new['password'], log=log)

target_db_old = MongDb(mongo_db_webpage_old['host'], mongo_db_webpage_old['port'], mongo_db_webpage_old['db'],
                       mongo_db_webpage_old['username'], mongo_db_webpage_old['password'], log=log)

mail_from_addr = 'datamonitor@haizhi.com'
mail_password = 'LcoS!WKXmWmFu2Or'
mail_to_addrs = ['youfeng@haizhi.com']


def send_email(from_addr, password, to_addrs, subject, msg, smtp_host="smtp.weibangong.com", smtp_port=465):
    email_client = SMTP(smtp_host, smtp_port)
    email_client.login(from_addr, password)
    msg['Subject'] = Header(subject, 'utf-8')
    msg['From'] = from_addr
    msg['To'] = str(to_addrs)
    email_client.sendmail(from_addr, to_addrs, msg.as_string())
    email_client.quit()


def find_task(db, which):
    count = 0

    # 股权出质
    equity_pledged_info = u'equity_pledged_info'

    source_table = "online_crawl_gansu_new"
    for item in db.traverse_batch(source_table):
        data_list = item.get('datalist')
        company = item.get('_id')

        count += 1

        if not isinstance(data_list, dict):
            log.error("{which} table: 没有 datalist company = {company}".format(
                company=company, which=which))
            continue

        if equity_pledged_info not in data_list:
            continue

        value = data_list.get(equity_pledged_info)
        if value is None:
            continue

        if 'detail' in value:
            log.info("{which} table: {equity} company = {company} have detail".format(
                equity=equity_pledged_info, company=company, which=which))
            continue

        if 'list' not in value:
            continue

        list_array = value.get('list')
        if not isinstance(list_array, list) or len(list_array) <= 0:
            continue

        for item0 in list_array:
            text = item0.get('text')
            if text is None:
                continue

            tr_list = PyQuery(text, parser='html').find('#stockTab').find('tr')
            if tr_list.length > 2:
                log.info("{which} table: {equity} company = {company} have list".format(
                    equity=equity_pledged_info, company=company, which=which))
                break
            if tr_list.length == 2 and tr_list.eq(1).find('td').length > 5:
                log.info("{which} table: {equity} company = {company} have list".format(
                    equity=equity_pledged_info, company=company, which=which))
                break

    log.info("查找结束: {which} count = {count}".format(which=which, count=count))


def main():
    start_time = time.time()
    find_task(target_db_new, 'new')
    find_task(target_db_old, 'old')
    end_time = time.time()

    send_email(
        mail_from_addr,
        mail_password,
        mail_to_addrs,
        '甘肃股权质押属性查找完成 - %s' % get_now_time(),
        MIMEText('耗时: {} s'.format(end_time - start_time), 'plain', 'utf-8')
    )


if __name__ == '__main__':
    main()
