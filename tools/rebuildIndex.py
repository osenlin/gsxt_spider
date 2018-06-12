#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: rebuildIndex.py
@time: 2017/6/10 15:07
"""
import pymongo

# 数据库地址
dbhost = '172.16.215.34'
# 数据库访问端口
dbport = 40043
# 需要访问的库
dbname = 'app_data'
# 访问授权信息
dbuser = 'work'
dbpass = 'haizhi'

conn = pymongo.MongoClient(dbhost, dbport,
                           connectTimeoutMS=30 * 60 * 1000,
                           serverSelectionTimeoutMS=30 * 60 * 1000,
                           maxPoolSize=600)
db = conn[dbname]
connected = db.authenticate(dbuser, dbpass)

table_name_list = set()

with open('mongo.conf') as pFile:
    for line in pFile:
        line = line.strip("\n")
        table_name_list.add(line.split(".")[0])

print '加载配置信息完成, 开始扫描重建索引...'
collection_name_list = db.collection_names()
for table_name in table_name_list:
    if table_name not in collection_name_list:
        print "%s not in..." % table_name
        continue

    for index in db[table_name].list_indexes():

        # 跳过_id索引
        if index['name'] == '_id_':
            continue

        # print index['name']

        if 'unique' in index:
            print '开始重建索引: ', table_name, index['name']
            db[table_name].drop_index(index['name'])
            print '删除索引完成: ', table_name, index['name']
            db[table_name].create_index(index['key'].items(), background=True, unique=False)
            print '重建索完成: ', table_name, index['name']
