#!/usr/bin/env bash

while true
do
#
python_pid=`ps aux |grep start_offline_crawler.py |grep python | grep -w offline_gsxt_detail.conf | grep -w online_all_search | awk '{print $2}'`
if [ "$python_pid" == '' ]; then
    nohup python start_offline_crawler.py 'config/offline_gsxt_detail.conf' 'online_all_search' 'all' > /dev/null 2>&1 &
fi


sleep 5

done