#!/usr/bin/env bash

while true
do

python_pid=`ps aux |grep start_offline_parse_task.py |grep python | grep -w offline_gsxt_parse.conf | awk '{print $2}'`
if [ "$python_pid" == '' ]; then
    nohup python start_offline_parse_task.py 'config/offline_gsxt_parse.conf' 'all' > /dev/null 2>&1 &
fi

sleep 10

done