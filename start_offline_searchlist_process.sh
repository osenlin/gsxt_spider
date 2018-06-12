#!/usr/bin/env bash

while true
do
#
python_pid=`ps aux |grep start_offline_crawler.py |grep python | grep -w offline_gsxt_searchlist.conf | grep -w offline_all_list | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_offline_crawler.py 'config/offline_gsxt_searchlist.conf' 'offline_all_list' 'all' > /dev/null 2>&1 &
fi

sleep 10

done