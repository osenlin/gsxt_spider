# coding=utf-8
import base64
import json

from tld import get_tld

__author__ = 'fengoupeng'

import hashlib
import os
import re
import time
import urlparse
import urllib
import itertools
import collections
import functools
import threading
from datetime import timedelta, datetime
from urllib import unquote
from HTMLParser import HTMLParser


def multi_thread_singleton(cls, *args, **kwargs):
    instance = {}
    instance_lock = threading.Lock()

    @functools.wraps(cls)
    def _singleton(*args, **kwargs):
        if cls in instance: return instance[cls]
        with instance_lock:
            return cls(*args, **kwargs)

    return _singleton


def process_url(base_url, deal_url):  # 处理url
    if deal_url is None or not isinstance(deal_url, basestring):
        return None
    if len(deal_url) == 0:
        return None
    deal_url = deal_url.strip()
    deal_url = change_ref(deal_url)
    # deal_url = deal_url.strip().split("#", 1)[0]
    # deal_url = decode_url(deal_url)
    if deal_url.startswith("http"):
        return deal_url
    elif deal_url.startswith("javascript"):
        return None
    else:
        return urlparse.urljoin(base_url, deal_url)


RAW_PATTERN = "(\\?\\\u[0-9a-zA-Z]{4})"


def change_raw(text):  # 处理unicode转义序列
    datas = re.findall(RAW_PATTERN, text)
    if len(datas) > 0:
        datas = set(datas)
        for data in datas:
            temp = data.replace("\\\\", "\\")
            if temp in [u"\\u0000", u"\\u0001", u"\\u0002", u"\\u0003", u"\\u0004"]:
                replace = ""
            else:
                replace = temp.decode("raw_unicode_escape")
            text = text.replace(data, replace)
    return text


REF_PATTERN = "(&#?[0-9a-zA-Z]{,9};)"


def change_ref(text):  # 处理网页中的转义序列
    datas = re.findall(REF_PATTERN, text)
    if len(datas) > 0:
        parser = HTMLParser()
        datas = set(datas)
        for data in datas:
            replace = parser.unescape(data)
            text = text.replace(data, replace)
    return text


UDE_PATTERN = "(%[0-9A-Za-z]{2})"


def decode_url(string):  # 处理URL编码
    if not isinstance(string, basestring) or string.find("%") == -1:
        return string
    temp = "%" + string.split("%", 1)[1]
    if re.match(UDE_PATTERN, temp) is None:
        return string
    if isinstance(string, unicode):
        temp = str(string)
    else:
        temp = string
    q = unquote(temp)
    if isinstance(q, unicode):
        return q
    p = re.compile("[0-9a-zA-Z\\+\\-\\s\p{P}]")
    l = len(p.sub("", q))
    try:
        if l % 3 == 0:
            q = q.decode("utf-8")
        elif l % 4 == 0:
            q = q.decode("gbk")
        else:
            q = q.decode("utf-8")
    except:
        try:
            q = q.decode("gbk")
        except:
            return string
    return q


def change_sql_str(string):
    return string.replace("\"", "'").replace("\\", "\\\\")


def change_sql_array(array):
    string = "["
    for item in array:
        if isinstance(item, basestring):
            string += "'" + change_sql_str(item).replace("'", "\\'") + "',"
        elif isinstance(item, int) or isinstance(item, long) or isinstance(item, float):
            string += unicode(item) + ","
        elif item is None:
            string += "NULL,"
    if string != "[":
        string = string[:-1]
    string += "]"
    return string


def get_md5(string):
    m2 = hashlib.md5()
    m2.update(string)
    return m2.hexdigest()


def get_project_path():
    project_name = "crawler"
    file_path = os.getcwdu()
    project_path = file_path[:file_path.find(project_name + "/") + len(project_name)]
    return project_path


def re_find_one(pattern, string):
    datas = re.findall(pattern, string)
    if len(datas) > 0:
        return datas[0]
    else:
        return None


def xpath_find_one(node, xpath_str):
    arr = node.xpath(xpath_str)
    if len(arr) > 0:
        result = arr[0].strip()
    else:
        result = None
    return result


def xpath_find_all(node, xpath_str):
    arr = node.xpath(xpath_str)
    if len(arr) > 0:
        result = "".join(arr).strip()
    else:
        result = None
    return result


def get_now_time():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def get_today():
    return time.strftime("%Y-%m-%d")


def get_date():
    return time.strftime("%Y%m%d")


def get_date_bias(count, day=None, formater='%Y%m%d'):
    if not day:
        day = time.strftime(formater)
    dateNow = datetime.strptime(day, formater)
    return (dateNow - timedelta(days=count)).strftime(formater)


def str_obj(obj, encoding='utf-8'):
    if isinstance(obj, unicode):
        return obj.encode(encoding)
    return obj


def unicode_obj(obj, encoding='utf-8'):
    if isinstance(obj, str):
        return obj.decode(encoding)
    return obj


def deal_json_content(content):
    json_start_index = content.find('{')
    list_start_index = content.find('[')
    if list_start_index > -1 and (json_start_index == -1 or list_start_index < json_start_index):
        list_finish_index = content.rfind(']')
        if list_finish_index > list_start_index:
            json_content = content[list_start_index: list_finish_index + 1]
            return json_content
    elif json_start_index > -1 and (list_start_index == -1 or json_start_index < list_start_index):
        json_finish_index = content.rfind('}')
        if json_finish_index > json_start_index:
            json_content = content[json_start_index: json_finish_index + 1]
            return json_content
    return None


def get_json_value(json_obj, path):
    array = path.split('.')
    obj = json_obj
    for key in array:
        if not isinstance(obj, dict):
            return None
        tmp = obj.get(key)
        if tmp is None:
            return None
        obj = tmp
    return obj


# 返回日期数组(由小到大)
def get_date_array(start_date, finish_date, format='%Y-%m-%d'):
    if start_date > finish_date:
        date = start_date
        start_date = finish_date
        finish_date = date
    dates = [start_date]
    if start_date != finish_date:
        date = start_date
        while date < finish_date:
            date = get_date_bias(-1, date, format)
            dates.append(date)
    return dates


# 返回月份组(由大到小)
def get_month_array(start_month, finish_month):
    if start_month < finish_month:
        month = start_month
        start_month = finish_month
        finish_month = month
    months = [start_month]
    month = start_month
    while month > finish_month:
        year = int(month[0:4])
        month = int(month[4:])
        month -= 1
        if month == 0:
            year -= 1
            month = 12
        month = str(month)
        if len(month) == 1:
            month = '0' + month
        month = str(year) + month
        months.append(month)
    return months


def deal_with_spend(spend):
    spend = int(spend)
    second = spend % 60
    spend = spend / 60
    if not spend:
        return '%d秒' % second
    minute = spend % 60
    spend = spend / 60
    if not spend:
        return '%d分%d秒' % (minute, second)
    hour = spend % 24
    spend = spend / 24
    if not spend:
        return '%d时%d分%d秒' % (hour, minute, second)
    else:
        return '%d天%d时%d分%d秒' % (spend, hour, minute, second)


def params_str_to_json(params_str):
    result = {}
    for params in params_str.split('&'):
        param, value = params.split('=')
        result[param] = urllib.unquote(value)
    return result


import ctypes


def get_md5_i64(obj):
    m = hashlib.md5()
    m.update(obj)
    return ctypes.c_int64(int(m.hexdigest()[8:-8], 16)).value


def unique(list_, key=lambda x: x):
    """efficient function to uniquify a list preserving item order"""
    seen = set()
    result = []
    for item in list_:
        seenkey = key(item)
        if seenkey in seen:
            continue
        seen.add(seenkey)
        result.append(item)
    return result


def get_url_info(base_url):
    url_split = urlparse.urlsplit(base_url)
    url_info = {}
    url_info['site'] = url_split.netloc
    url_info['site'] = url_info['site'].split(':')[0]
    url_info['site_id'] = get_md5_i64(url_info['site'])
    url_info['path'] = url_split.path
    url_info['query'] = url_split.query
    url_info['fragment'] = url_split.fragment
    try:
        url_info['domain'] = get_tld(base_url)
    except Exception as e:
        url_info['domain'] = url_info['site']
    if url_info.get('domain'):
        url_info['domain_id'] = get_md5_i64(url_info['domain'])
    else:
        url_info['domain_id'] = None

    url_info['url'] = base_url
    url_info['url_id'] = get_md5_i64(base_url)
    return url_info


def url_query_decode(string):
    query_info = {}
    for x in string.split('&'):
        if len(x) <= 0:
            continue
        kk = x.split('=')
        if len(kk) == 1:
            query_info[kk[0]] = ""
        elif len(kk) > 1:
            query_info[kk[0]] = urllib.unquote("=".join(kk[1:]))
    return query_info


def decode_content(string, charset=None):
    if not string or isinstance(string, unicode):
        return string, None
    if charset:
        if charset.lower() in ['gb2312', 'gbk', 'gb18030']:
            try:
                return string.decode('gb18030'), 'gb18030'
            except:
                pass
        return string.decode(charset, 'ignore'), charset
    for charset in ['gb18030', 'utf-8']:
        try:
            return string.decode(charset), charset
        except:
            pass


def base64_encode_json(obj):
    order_data = collections.OrderedDict()
    for k in sorted(obj.keys()):
        order_data[k] = obj[k]
    return base64.encodestring(json.dumps(order_data))


def base64_decode_json(string):
    return json.loads(base64.b64decode(string))


def url_encode(url):
    if isinstance(url, unicode):
        url = url.encode('utf-8')
    url = url.replace('\t', '').replace('\n', '').replace('\r', '')
    url = urllib.quote(url, '!:?=/&%')
    return url


def is_index_url(url):
    prefix_list = ['/index', '/default']
    affix_list = ['.htm', '.html', '.shtml', '.stm', '.shtm', '.asp', '.aspx', '.php', '.jsp']
    index_list = [x + y for x, y in itertools.product(prefix_list, affix_list)]
    index_list.append('/')
    index_list.append('/index')
    path = urlparse.urlparse(url).path
    result = 1 if path.lower() in index_list else 0
    return True if result and ('?' not in url) else False


def build_hzpost_url(base_url, postdata):
    url_info = get_url_info(base_url)
    query_info = url_query_decode(url_info.get('query'))
    query_info['HZPOST'] = base64_encode_json(postdata)
    hz_url = base_url.split("?")[0] + '?' + urllib.urlencode(query_info)
    return hz_url


def extract_hzpost_url(base_url):
    url_info = get_url_info(base_url)
    query_info = url_query_decode(url_info.get('query'))
    postdata = None
    if isinstance(query_info, dict) and query_info.get('HZPOST'):
        hz_post = query_info.get('HZPOST')
        postdata = base64_decode_json(hz_post)
        del query_info['HZPOST']
        if len(query_info) > 0:
            base_url = base_url.split("?")[0] + '?' + urllib.urlencode(query_info)
        else:
            base_url = base_url.split("?")[0]
    return {"url": base_url, 'postdata': postdata}


if __name__ == '__main__':
    print json.dumps(url_query_decode(
        "searchKey=&bidMenu=&bidMenuName=&chnlIds=&bidType=0&bidWay=0&region=339900&releaseStartDate=&releaseEndDate=&noticeEndDate=&noticeEndDate1=&pub_org=&solrType=0&frontMobanType=1&pageNum=4&pageCount=1000"))
    print url_encode(
        "http://xy.fspc.gov.cn/tentbaseinfoAction!getTentbaseinfoById.do?creditquery.id=2C47AD61C2CE405FADA921CA7E6EAB13&num=4")
    test_url = "http://101.201.196.20:8081?hello=world"
    print get_url_info(test_url).get('domain')
    postdata = {'post': "data", 'h1': 12, 'h2': "323", 'h0': 12}
    hz_url = build_hzpost_url(test_url, postdata)
    print hz_url
    print extract_hzpost_url(hz_url)
    postdata = {'h0': 12, 'h2': '323', 'h1': 12, 'post': 'data'}
    print postdata
    print build_hzpost_url(test_url, postdata)
    postdata = {'h2': '323', 'h0': 12, 'h1': 12, 'post': 'data'}
    print postdata
    hzurl = build_hzpost_url(test_url, postdata)
    print hzurl
    print extract_hzpost_url(hzurl)
    hzurl = 'http://wenshu.court.gov.cn/List/ListContent?HZPOST=eyJEaXJlY3Rpb24iOiAiZGVzYyIsICJJbmRleCI6IDcsICJPcmRlciI6ICJcdTg4YzFcdTUyMjRc%0AdTY1ZTVcdTY3MWYiLCAiUGFnZSI6ICIyMCIsICJQYXJhbSI6ICJcdTg4YzFcdTUyMjRcdTY1ZTVc%0AdTY3MWY6MjAxNi0xMC0yNCBUTyAyMDE2LTExLTAxLFx1NmNkNVx1OTY2Mlx1NTczMFx1NTdkZjpc%0AdTY1YjBcdTc1ODZcdTc3MDEsXHU0ZTJkXHU3ZWE3XHU2Y2Q1XHU5NjYyOlx1NjViMFx1NzU4Nlx1%0AN2VmNFx1NTQzZVx1NWMxNFx1ODFlYVx1NmNiYlx1NTMzYVx1NWRmNFx1OTdmM1x1OTBlZFx1Njk1%0AZVx1ODQ5OVx1NTNlNFx1ODFlYVx1NmNiYlx1NWRkZVx1NGUyZFx1N2VhN1x1NGViYVx1NmMxMVx1%0ANmNkNVx1OTY2MiJ9%0A'
    print hzurl
    print json.dumps(extract_hzpost_url(hzurl))
