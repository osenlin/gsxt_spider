# coding=utf-8
import sys

import pymongo


class MongDb(object):
    ASCENDING = pymongo.ASCENDING
    DESCENDING = pymongo.DESCENDING

    def __init__(self, dbhost, dbport, dbname, dbuser, dbpass, log=None):
        try:
            self.log = log

            self.conn = pymongo.MongoClient(dbhost, dbport,
                                            connectTimeoutMS=30 * 60 * 1000,
                                            serverSelectionTimeoutMS=30 * 60 * 1000,
                                            maxPoolSize=600)
            self.db = self.conn[dbname]
            if dbuser and dbpass:
                self.connected = self.db.authenticate(dbuser, dbpass)
            else:
                self.connected = True
        except Exception as e:
            self.log.error('{db} {port} {name} {user} {pwd}'.format(db=dbhost, port=dbport, name=dbname, user=dbuser,
                                                                    pwd=dbpass))
            self.log.exception(e)
            sys.exit(1)

    def __del__(self):
        self.db.logout()
        self.conn.close()
        self.log.info('释放mongodb资源')
        pass

    def check_connected(self):
        pass
        # if not self.connected:
        #     raise NameError('stat:connected Error')
        # while True:
        #     try:
        #         if self.conn.is_mongos:
        #             break
        #
        #         self.log.error('mongodb 连接异常, 等待连接正常...')
        #         time.sleep(10)
        #     except ServerSelectionTimeoutError as e:
        #         self.log.error('mongodb 连接异常, 等待连接正常...')
        #         self.log.exception(e)
        #         time.sleep(10)

    def save(self, table, value):
        try:
            self.check_connected()
            self.db[table].save(value)
        except Exception as e:
            self.log.exception(e)
            raise e

    def insert(self, table, value):
        try:
            self.check_connected()
            self.db[table].insert(value)
        except Exception as e:
            self.log.exception(e)
            raise e

    def update(self, table, conditions, value, s_upsert=False, s_multi=False):
        try:
            self.check_connected()
            self.db[table].update(conditions, value, upsert=s_upsert, multi=s_multi)
        except Exception as e:
            self.log.exception(e)
            raise e

    def upsert(self, table, data):
        try:
            self.check_connected()
            query = {'_id': data['_id']}
            if not self.db[table].find_one(query):
                self.db[table].insert(data)
            else:
                data.pop('_id')
                self.db[table].update(query, {'$set': data})
        except Exception as e:
            self.log.exception(e)
            raise e

    def find_and_modify(self, table, query=None, update=None,
                        upsert=False, sort=None, full_response=False,
                        manipulate=False, **kwargs):
        query = {} if query is None else query
        try:
            self.check_connected()
            self.db[table].find_and_modify(query=query, update=update, upsert=upsert,
                                           sort=sort, full_response=full_response,
                                           manipulate=manipulate, **kwargs)
        except Exception as e:
            self.log.exception(e)
            raise e

    def traverse(self, table, where=None):
        cursor = None
        try:
            self.check_connected()
            where = {} if where is None else where
            cursor = self.db[table].find(where, no_cursor_timeout=True)
            for item in cursor:
                yield item
        except Exception as e:
            self.log.exception(e)
            raise e
        finally:
            if cursor is not None:
                cursor.close()

    def traverse_batch(self, table, where=None):
        cursor = None
        try:
            self.check_connected()
            where = {} if where is None else where
            cursor = self.db[table].find(where, no_cursor_timeout=True).batch_size(500)
            for item in cursor:
                yield item
        except Exception as e:
            self.log.exception(e)
            raise e
        finally:
            if cursor is not None:
                cursor.close()
                self.log.info('关闭traverse_batch游标')

    def traverse_field(self, table, where, field):
        cursor = None
        try:
            self.check_connected()
            where = {} if where is None else where
            cursor = self.db[table].find(where, field, no_cursor_timeout=True)
            for item in cursor:
                yield item
        except Exception as e:
            self.log.exception(e)
            raise e
        finally:
            if cursor is not None:
                cursor.close()

    def select_field(self, table, where=None, field_list=None):
        try:
            self.check_connected()
            where = {} if where is None else where
            field_list = [] if field_list is None else field_list
            return self.db[table].find(where, field_list)
        except Exception as e:
            self.log.exception(e)
            raise e

    def select(self, table, value=None):
        try:
            self.check_connected()
            value = {} if value is None else value
            return self.db[table].find(value, no_cursor_timeout=True).batch_size(500)
        except Exception as e:
            self.log.exception(e)
            raise e

    def select_colum(self, table, value, colum):
        try:
            self.check_connected()
            return self.db[table].find(value, {colum: 1})
        except Exception as e:
            self.log.exception(e)
            raise e

    def select_count(self, table, value=None):
        try:
            self.check_connected()
            value = {} if value is None else value
            return self.db[table].find(value).count()
        except Exception as e:
            self.log.exception(e)
            raise e

    def select_one(self, table, value):
        try:
            self.check_connected()
            result = self.db[table].find(value).limit(1)  # fix-me to findOne function  find_one
            for item in result:
                return item
        except Exception as e:
            self.log.exception(e)
            raise e
        return None

    def select_limit(self, table, value, limit=500):
        try:
            self.check_connected()
            result = self.db[table].find(value).limit(limit)  # fix-me to findOne function  find_one
            for item in result:
                return item
        except Exception as e:
            self.log.exception(e)
            raise e
        return None

    def select_one_field(self, table, value, field):
        try:
            self.check_connected()
            result = self.db[table].find(value, field).limit(1)  # fix-me to findOne function  find_one
            for item in result:
                return item
            return None
        except Exception as e:
            self.log.exception(e)
            raise e

    def find_one(self, table, query, field=None):
        try:
            self.check_connected()
            if field is None:
                return self.db[table].find_one(query)
            return self.db[table].find_one(query, field)
        except Exception as e:
            self.log.exception(e)
            raise e

    def select_sort(self, table, value, sort):
        try:
            self.check_connected()
            return self.db[table].find(value).sort(sort)
        except Exception as e:
            self.log.exception(e)
            raise e

    def delete(self, table, value):
        try:
            self.check_connected()
            return self.db[table].remove(value)
        except Exception as e:
            self.log.exception(e)
            raise e

    # 删除数据库
    def drop(self, table):
        try:
            self.check_connected()
            self.db[table].drop()
        except Exception as e:
            self.log.exception(e)
            raise e

    # index => [(index_colunm, pymongo.DESCENDING/pymongo.ASCENDING)]
    def create_index(self, table, index):
        try:
            self.check_connected()
            # 后台建索引
            self.db[table].ensure_index(index, background=True)
        except Exception as e:
            self.log.exception(e)
            raise e

    # 删除索引
    def drop_indexes(self, table):
        try:
            self.check_connected()
            # 后台建索引
            self.db[table].drop_indexes()
        except Exception as e:
            self.log.exception(e)
            raise e

    def close_all_databases(self):
        try:
            self.check_connected()
            admin = self.conn['admin']
            auth = admin.authenticate('admin', 'liveadmin')
            if auth:
                return admin.command({'closeAllDatabases': 1})
        except Exception as e:
            self.log.exception(e)
            raise e
        return None

    def insert_many(self, table, documents, ordered=True,
                    bypass_document_validation=False):
        try:
            if documents is None or len(documents) <= 0:
                return

            self.check_connected()
            self.db[table].insert_many(
                documents=documents, ordered=ordered, bypass_document_validation=bypass_document_validation)
        except Exception as e:
            self.log.exception(e)
            raise e

    def insert_batch_data(self, table, data_list, is_order=False, insert=False):
        count = 0
        if data_list is None:
            return count

        length = len(data_list)
        if length <= 0:
            return count

        try:
            self.check_connected()
            bulk = self.db[table].initialize_ordered_bulk_op() if is_order else self.db[
                table].initialize_unordered_bulk_op()
            for item in data_list:
                if insert:
                    bulk.insert(item)
                else:
                    item_copy = item.copy()
                    _id = item_copy.pop('_id')
                    bulk.find({'_id': _id}).upsert().update({'$set': item_copy})
                count += 1
            bulk.execute({'w': 0})
            self.log.info('insert_logs: {length}'.format(length=len(data_list)))
        except Exception as e:
            self.log.exception(e)
            count = 0

        return count
