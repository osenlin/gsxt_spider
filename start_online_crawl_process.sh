#!/usr/bin/env bash

while true
do
#
# 北京详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w beijing | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'beijing' > /dev/null 2>&1 &
fi

# 上海详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w shanghai | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'shanghai' > /dev/null 2>&1 &
fi

# 广东详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w guangdong | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'guangdong' > /dev/null 2>&1 &
fi

# 辽宁详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w liaoning | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'liaoning' > /dev/null 2>&1 &
fi

# 山东详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w shandong | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'shandong' > /dev/null 2>&1 &
fi

# 山西详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w shanxicu | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'shanxicu' > /dev/null 2>&1 &
fi

# 福建详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w fujian | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'fujian' > /dev/null 2>&1 &
fi

# 湖南详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w hunan | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'hunan' > /dev/null 2>&1 &
fi

# 湖北详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w hubei | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'hubei' > /dev/null 2>&1 &
fi

# 河南详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w henan | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'henan' > /dev/null 2>&1 &
fi

# 西藏详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w xizang | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'xizang' > /dev/null 2>&1 &
fi

# 海南详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w hainan | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'hainan' > /dev/null 2>&1 &
fi

# 河北详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w hebei | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'hebei' > /dev/null 2>&1 &
fi

# 云南详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w yunnan | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'yunnan' > /dev/null 2>&1 &
fi

# 黑龙江详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w heilongjiang | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'heilongjiang' > /dev/null 2>&1 &
fi

# 新疆详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w xinjiang | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'xinjiang' > /dev/null 2>&1 &
fi

# 广西详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w guangxi | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'guangxi' > /dev/null 2>&1 &
fi

# 四川详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w sichuan | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'sichuan' > /dev/null 2>&1 &
fi

# 安徽详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w anhui | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'anhui' > /dev/null 2>&1 &
fi

# 甘肃详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w gansu | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'gansu' > /dev/null 2>&1 &
fi

# 江苏详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w jiangsu | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'jiangsu' > /dev/null 2>&1 &
fi

# 陕西详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w shanxi | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'shanxi' > /dev/null 2>&1 &
fi

# 吉林详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w jilin | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'jilin' > /dev/null 2>&1 &
fi

# 内蒙古详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w neimenggu | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'neimenggu' > /dev/null 2>&1 &
fi

# 天津详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w tianjin | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'tianjin' > /dev/null 2>&1 &
fi

# 青海详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w qinghai | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'qinghai' > /dev/null 2>&1 &
fi

# 浙江详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w zhejiang | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'zhejiang' > /dev/null 2>&1 &
fi

# 贵州详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w guizhou | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'guizhou' > /dev/null 2>&1 &
fi

# 宁夏详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w ningxia | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'ningxia' > /dev/null 2>&1 &
fi

# 江西详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w jiangxi | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'jiangxi' > /dev/null 2>&1 &
fi

# 重庆详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w chongqing | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'chongqing' > /dev/null 2>&1 &
fi

# 总局详情页
python_pid=`ps aux |grep start_online_crawl.py |grep python | grep -w online_gsxt_crawl.conf | grep -w gsxt | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_crawl.py 'config/online_gsxt_crawl.conf' 'gsxt' > /dev/null 2>&1 &
fi

sleep 5

done