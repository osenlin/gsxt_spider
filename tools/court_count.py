#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: court_count.py
@time: 2017/7/31 17:29
"""

# !/usr/bin/env python
# -*-coding:utf-8 -*-

import pymongo

app_data_conf = {
    'host': '172.16.215.16',
    'port': 40042,
    'db': 'app_data',
    'username': 'work',
    'password': 'haizhi'
}

conn = pymongo.MongoClient(app_data_conf['host'], app_data_conf['port'])
database = conn['app_data']
database.authenticate('work', 'haizhi')
collection = database['court_ktgg']
site_dict = {
    'whzy.hbfy.gov.cn': 'http://whzy.hbfy.gov.cn/DocManage/ViewDoc?docId=48ff441c-91f0-4a25-8fb1-ff1c2ef8a813',
    'nnzy.chinacourt.org': 'http://nnzy.chinacourt.org/article/index/id/M0g3MzAwNTAwNCACAAA%3D/page/1.shtml',
    'whyjfy.chinacourt.org': 'http://whyjfy.chinacourt.org/article/detail/2017/06/id/2903785.shtml',
    'www.njfy.gov.cn': 'http://www.njfy.gov.cn/www/njfy/lcgkKtgg.jsp?pageNo=2',
    'www.sscourt.gov.cn': 'http://www.sscourt.gov.cn/web/Content.aspx?chn=389&id=1615',
    'mdjzy.hljcourt.gov.cn': 'mdjzy.hljcourt.gov.cn',
    'yqzy.chinacourt.org': 'http://yqzy.chinacourt.org/article/detail/2017/05/id/2882313.shtml',
    'xzgy.susong51.net': 'http://xzgy.susong51.net/ktggPage.jspx?channelId=16818&listsize=14&pagego=14',
    'tv.hicourt.gov.cn': 'http://tv.hicourt.gov.cn/video/detail/court/0/id/1323',
    'www.nxfy.gov.cn': 'http://www.nxfy.gov.cn/sfgk/ktgg/201704/t20170412_4251831.html',
    'www.jlsfy.gov.cn': 'http://www.jlsfy.gov.cn/ktggInfo.jspx?fyid=750&bh=3A03C6E4E29C7559BF5E47789C708062&isapp=null',
    'gsgf.gssfgk.com': 'http://gsgf.gssfgk.com:80/ktggInfo.jspx?fyid=3750&bh=F3BDE300CF09AF590238ED6031D8E9EB',
    'hbgy.hbsfgk.org': 'http://hbgy.hbsfgk.org/ktggInfo.jspx?fyid=100&bh=F3FFB5D0D994D507C96972F741EC9002&isapp=null',
    'sxfy.chinacourt.org': 'http://sxfy.chinacourt.org/article/detail/2017/07/id/2934806.shtml',
    'www.hbfy.gov.cn': 'http://www.hbfy.gov.cn/DocManage/ViewDoc?docId=ab57e812-2887-4ae5-8883-8a3be151034a',
    'www.ynfy.gov.cn': 'http://www.ynfy.gov.cn/ktggInfo.jspx?fyid=3350&bh=A9CF8B1D2539CDFEBF9B544AD245773F&isapp=null',
    'nczy.chinacourt.org': 'http://nczy.chinacourt.org/article/detail/2017/04/id/2753617.shtml',
    'www.sxgaofa.cn': 'http://www.sxgaofa.cn/ts/viewtv?id=31ee1ff56c854987b358b5637a7ef0cb&pid=3c1b19f94528417e88ea8c6e2b563d05',
    'cszy.chinacourt.org': 'http://cszy.chinacourt.org/article/detail/2017/07/id/2932750.shtml',
    'ts.hncourt.gov.cn': 'http://ts.hncourt.gov.cn/front/video/stat/court/17/sid/4/page/2',
    'www.luyang.gov.cn': 'http://www.luyang.gov.cn/sitecn/About.aspx?columnid=1457',
    'www.hshfy.sh.cn': 'http://www.hshfy.sh.cn/shfy/gweb/ktgg_search_content.jsp?yzm=u87r&ft=&ktrqks=2009-12-09&ktrqjs=2020-01-09&spc=&yg=&bg=&ah=&pagesnum=1000&jdfwkey=n0vx9',
    'www.sdcourt.gov.cn': 'http://www.sdcourt.gov.cn/sdfy_search/tsxx/list.do?tsxx.court_no=0F19&curPage=1',
}
sites = site_dict.keys()

print "site \t\t\t_utime \t_in_time "
for site in sites:
    _utime_count = collection.find(
        {'_utime': {"$gte": "2017-07-30", "$lt": "2017-08-01"}, '_src.0.site': '%s' % site}).count()
    _in_time_count = collection.find(
        {'_in_time': {"$gte": "2017-07-30", "$lt": "2017-08-01"}, '_src.0.site': '%s' % site}).count()
    print "{} \t{} \t{}".format(site, _utime_count, _in_time_count)
