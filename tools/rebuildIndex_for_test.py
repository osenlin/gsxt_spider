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

dbhost = '172.16.215.2'
dbport = 40042
dbname = 'data_sync'
dbuser = 'work'
dbpass = 'haizhi'

conn = pymongo.MongoClient(dbhost, dbport,
                           connectTimeoutMS=30 * 60 * 1000,
                           serverSelectionTimeoutMS=30 * 60 * 1000,
                           maxPoolSize=600)
db = conn[dbname]
connected = db.authenticate(dbuser, dbpass)

table_name_list = ['test']

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
        #print index['key']

        # print index
        if 'unique' in index:
            db[table_name].drop_index(index['name'])
            db[table_name].create_index(index['key'].items(), background=True, unique=False)
            print index['name']

        # if index['name'] != '_id_':
        #     db[table_name].create_index([(index['name'], pymongo.ASCENDING)], background=True, unique=False)
