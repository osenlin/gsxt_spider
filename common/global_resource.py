# coding=utf-8
import re
import sys

from common.mongo import MongDb
from config.conf import mongo_db_target, mongo_db_source, mongo_db_target_new
from logger import Gsxtlogger

# 判断是否是在debug模式下
if len(sys.argv) > 1:
    is_debug = False
else:
    is_debug = True


# 'config/online_gsxt_crawl.conf' 'shanghai'

# 获得log的名称
def get_log_name():
    log_name = 'start_online_crawl.log'
    length = len(sys.argv)
    if length > 2:
        search_list = re.findall('config/(.*?)\.conf', sys.argv[1])
        if len(search_list) > 0:
            log_name = search_list[0] + '_' + sys.argv[2] + '.log'

    return log_name


global_logger = Gsxtlogger(get_log_name())
global_log = global_logger.get_logger()

# 旧网页库
target_db = MongDb(mongo_db_target['host'], mongo_db_target['port'], mongo_db_target['db'],
                   mongo_db_target['username'],
                   mongo_db_target['password'], log=global_log)

# 新网页库
target_db_new = MongDb(mongo_db_target_new['host'], mongo_db_target_new['port'], mongo_db_target_new['db'],
                       mongo_db_target_new['username'],
                       mongo_db_target_new['password'], log=global_log)

# 搜索列表存储表
source_db = MongDb(mongo_db_source['host'], mongo_db_source['port'], mongo_db_source['db'],
                   mongo_db_source['username'],
                   mongo_db_source['password'], log=global_log)
