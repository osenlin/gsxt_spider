#!/usr/bin/env bash

echo '更新最新代码...'
git pull

# 关掉脚本
ps -ef | grep -w start_online_crawl_process.sh | grep -v grep | awk '{print $2}' | xargs kill -9

# 关掉程序
ps -ef | grep -w start_online_crawl.py | grep -v grep | grep python | grep online_gsxt_crawl | awk '{print $2}' | xargs kill
ps -ef | grep -i phantomjs | grep -v grep | grep shandong | awk '{print $2}' | xargs kill

echo '停止工商代理监控脚本'
echo '休眠8s'
inter_time=8
sleep ${inter_time}

# 启动详情页进程
nohup sh start_online_crawl_process.sh > log/start_online_crawl_process.file 2>&1 &