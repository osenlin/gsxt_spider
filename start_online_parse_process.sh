#!/usr/bin/env bash

while true
do


python_pid=`ps aux |grep start_online_parse_task.py |grep python | grep -w online_gsxt_parse.conf | grep task_1 | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_parse_task.py 'config/online_gsxt_parse.conf' 1 'task_1' > /dev/null 2>&1 &
fi

python_pid=`ps aux |grep start_online_parse_task.py |grep python | grep -w online_gsxt_parse.conf | grep task_2 | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_parse_task.py 'config/online_gsxt_parse.conf' 2 'task_2' > /dev/null 2>&1 &
fi

python_pid=`ps aux |grep start_online_parse_task.py |grep python | grep -w online_gsxt_parse.conf | grep task_3 | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_parse_task.py 'config/online_gsxt_parse.conf' 3 'task_3' > /dev/null 2>&1 &
fi

python_pid=`ps aux |grep start_online_parse_task.py |grep python | grep -w online_gsxt_parse.conf | grep task_4 | awk '{print $2;}'`
if [ "$python_pid" == '' ]; then
    nohup python start_online_parse_task.py 'config/online_gsxt_parse.conf' 4 'task_4' > /dev/null 2>&1 &
fi

sleep 10

done