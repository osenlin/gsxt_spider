#!/usr/bin/env bash

echo '更新最新代码...'
git pull

# 关掉脚本
ps -ef | grep -w start_online_parse_process.sh | grep -v grep | awk '{print $2}' | xargs kill -9

# 关掉程序
ps -ef | grep -w start_online_parse_task.py | grep -v grep | grep python | grep online_gsxt_parse | awk '{print $2}' | xargs kill


echo '停止工商代理监控脚本'
echo '休眠8s'
inter_time=8
sleep ${inter_time}

# 启动详情页进程
nohup sh start_online_parse_process.sh > /dev/null 2>&1 &