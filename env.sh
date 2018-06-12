#!/usr/bin/env bash

echo "进入env.sh 开始设置环境变量"

CURRENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "当前目录: current_dir = ${CURRENT_DIR}"
if [ ! -n "$CRAWLER_PATH" ]; then
    #export CRAWLER_PATH='/data/crawler'
    export CRAWLER_PATH="${CURRENT_DIR}"
    echo "导出路径: CRAWLER_PATH = ${CRAWLER_PATH}"
fi
Prog=/home/work/env/python-2.7/bin/python
PATH="/home/work/env/python-2.7/bin/:$HOME/.local/bin:$HOME/bin:$PATH"
export PATH

echo "退出env.sh"
