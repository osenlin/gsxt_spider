# coding:utf-8
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

reload(sys)
sys.setdefaultencoding('utf-8')


class Gsxtlogger(object):
    # 默认info级别
    level = logging.INFO

    # 存放目录名称
    folder = 'log'

    def __init__(self, filename, for_mat=None):
        pwd = os.path.abspath(os.path.dirname(__file__))

        directory = os.path.join(pwd, self.folder)
        if not os.path.exists(directory):
            os.mkdir(directory)

        self.file_path = directory + '/' + filename

        self.log = logging.getLogger(filename)
        self.log.setLevel(self.level)

        handler = TimedRotatingFileHandler(self.file_path, when='H', interval=12, backupCount=5, encoding='utf-8')

        # 设置输出格式
        format_log = "%(asctime)s %(threadName)s %(funcName)s %(filename)s:%(lineno)s %(levelname)s %(message)s"
        if for_mat is not None:
            format_log = for_mat
        fmt = logging.Formatter(format_log)
        handler.setFormatter(fmt)

        self.log.addHandler(handler)

        # 控制台输出
        stream = logging.StreamHandler()
        stream.setFormatter(fmt)

        self.log.addHandler(stream)

    def set_level(self, level):
        self.log.setLevel(level=level)
        return self.log

    def get_logger(self):
        return self.log

    def __del__(self):
        pass
        # # 删除文件
        # try:
        #     os.remove(self.file_path)
        # except:
        #     pass
        # llogging.shutdown()


if __name__ == '__main__':
    log = Gsxtlogger('log.log').get_logger()
    log.info('text')
    log.warn('text')
    log.error('text')
