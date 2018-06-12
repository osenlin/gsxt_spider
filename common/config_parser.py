#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: config_parser.py
@time: 2016/12/16 17:36
"""
import copy
from ConfigParser import RawConfigParser


class ConfigParser(object):
    def __init__(self, config_file='config/cmb_gsxt.conf'):
        self.config_list = {}
        self.load_config(config_file)

    def load_config(self, config_file):
        # 读取配置信息
        config_parser = RawConfigParser()
        config_parser.read(config_file)
        sections = config_parser.sections()
        for section in sections:
            config_dict = dict(config_parser.items(section))
            self.config_list[section] = config_dict

    def get_session(self, province):
        return copy.deepcopy(self.config_list.get(province, None))

    def get_all_session(self):
        return copy.deepcopy(self.config_list)

    def __del__(self):
        del self.config_list
