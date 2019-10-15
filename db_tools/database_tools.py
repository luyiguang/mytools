#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : database_tools.py
# @Author: Luyiguang
# @Date  : 2019/10/15 0015
# @Desc  : 数据库连接工具

import configparser
import os
import traceback


class DataBase(object):
    def __init__(self, db_modular=None):
        self.__db_name = None
        self.__db_user = None
        self.__db_password = None
        self.__db_host = None
        self.__db_port = None
        self.db_modular = db_modular

    def db_cursor(self):
        self.__db_config_read()
        conn = self.db_modular.connect(
            database=self.__db_name, user=self.__db_user, password=self.__db_password, host=self.__db_host,
            port=self.__db_port
        )
        return conn.cursor()

    def db_insert_many(self, temp, data):
        cur = self.db_cursor()
        try:
            cur.executemany(temp, data)
            cur.connection.commit()
        except:
            cur.connection.rollback()
            traceback.print_exc()
        finally:
            cur.connection.close()

    def db_insert(self, sql):
        cur = self.db_cursor()
        try:
            cur.execute(sql)
            cur.connection.commit()
        except:
            cur.connection.rollback()
            traceback.print_exc()
        finally:
            cur.connection.close()

    def db_query(self, table_name):
        cur = self.db_cursor()
        sql = "select * from {0};".format(table_name)
        cur.execute(sql)
        rows = cur.fetchall()
        return rows

    def __db_config_generate(self):
        config = configparser.ConfigParser()
        config.add_section("DB")
        config.set("DB", "Host", "localhost")
        config.set("DB", "Port", "5432")
        config.set("DB", "Database", "datainput")
        config.set("DB", "User", "postgres")
        config.set("DB", "Password", "root")
        with open("database.conf", "w+", encoding="utf-8") as f:
            config.write(f)

    def __db_config_read(self):
        if not os.path.exists("database.conf"):
            self.__db_config_generate()
        config = configparser.ConfigParser()
        config.read('database.conf', encoding="utf-8")
        if 'DB' not in config.sections():
            self.__db_config_generate()
            config.read('database.conf', encoding="utf-8")
        options = config.options('DB')
        if not ('Host' in options and 'User' in options and 'Password' in options and 'Port' in options and
                'Database' in options):
            self.__db_config_generate()
            config.read('database.conf', encoding="utf-8")
        self.__db_host = config.get('DB', 'Host')
        self.__db_user = config.get('DB', 'User')
        self.__db_password = config.get('DB', 'Password')
        self.__db_port = config.getint('DB', 'Port')
        self.__db_name = config.get('DB', 'Database')
