# -*- coding: UTF-8 -*-
'''
Author       : Ma Zhongjun
Date         : 2020-12-05 19:23:05
LastEditTime : 2020-12-10 11:11:20
Description  : 
'''

import os
import sys
import time
from getpass import getpass
import pymysql
from dbutils.pooled_db import PooledDB
import pandas as pd

os.chdir(sys.path[0])

username = input('MySQL username:')
password = getpass('MySQL password:')
database = input('MySQL database:')
isInsert = bool(input('Is insert?:'))

logPathRun = 'result/container/select_year_month_imo/log_run.csv'
print('Log file path (Running): {}'.format(logPathRun))


def init_pool():
    host = '127.0.0.1'
    port = 3306
    pool = PooledDB(
        creator=pymysql,
        maxconnections=0,  # 连接池允许的最大连接数，0和None表示不限制连接数
        mincached=12,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
        maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
        maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
        blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
        host=host,  # 此处必须是是127.0.0.1
        port=port,
        user=username,
        passwd=password,
        db=database,
        use_unicode=True,
        charset='utf8'
    )
    if isInsert:
        db = pool.connection()
        cursor = db.cursor()
        sqls = [
            'set unique_checks=0',
            'set foreign_key_checks=0',
            'set autocommit=0',
        ]
        for sql in sqls:
            cursor.execute(sql)
        db.commit()
        cursor.close()
        db.close()
    return pool


pool = init_pool()


def alter_index(year: int, month: int, index: str):
    '''
    description: 为table添加索引
    param year {int} 形如database_ais_yearmonth
    param month {int}
    param index {str} 需要添加索引的表头
    return logInfo {str} 记录执行sql语句的结果
    '''
    # 元素类型检查
    if not isinstance(year, int):
        raise TypeError('Argument {} must be {}'.format('year', 'int'))
    if not isinstance(month, int):
        raise TypeError('Argument {} must be {}'.format('month', 'int'))
    if not isinstance(index, str):
        raise TypeError('Argument {} must be {}'.format('index', 'str'))

    startTime = time.time()
    db = pool.connection()
    cursor = db.cursor()

    table = '{}_ais_{}{}'.format(
        database, year, month) if month > 9 else '{}_ais_{}0{}'.format(database, year, month)
    sql = 'alter table {} add index ({})'.format(table, index)

    try:
        cursor.execute(sql)
        db.commit()
        log = 'success'
    except Exception as e:
        db.rollback()
        log = e
    finally:
        cursor.close()
        db.close()
        totalTime = time.time()-startTime
        with open(logPathRun, 'a') as f:
            logInfo = '{}|{}|{}|{}\n'.format(year, month, totalTime, log)
            f.write(logInfo)
        return logInfo


def select_by_year_month_imo(year: int, month: int, imo: int):
    '''
    description: 根据年月和IMO查表
    param year {int}
    param month {int}
    param imo {int}
    return {*}
    '''
    # 元素类型检查
    if not isinstance(year, int):
        raise TypeError('Argument {} must be {}'.format('year', 'int'))
    if not isinstance(month, int):
        raise TypeError('Argument {} must be {}'.format('month', 'int'))
    if not isinstance(imo, int):
        raise TypeError('Argument {} must be {}'.format('imo', 'int'))

    startTime = time.time()
    db = pool.connection()
    cursor = db.cursor()

    table = '{}_ais_{}{}'.format(
        database, year, month) if month > 9 else '{}_ais_{}0{}'.format(database, year, month)
    sql = """
        select imo,latitude,longitude,`timestamp`,speed,draught,
        nearest_port_id,nearest_port_name,distance_from_nearest_port
        from {}
        where imo = {}
        order by `timestamp`;
    """.format(table, imo)

    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        db.commit()
        log = 'success'
        df = pd.DataFrame(results, columns=[
            'imo', 'latitude', 'longitude', 'timestamp', 'speed', 'draught',
            'nearest_port_id', 'nearest_port_name', 'distance_from_nearest_port'])
    except Exception as e:
        df = pd.DataFrame()
        db.rollback()
        log = e
    finally:
        cursor.close()
        db.close()
        totalTime = time.time()-startTime
        with open(logPathRun, 'a') as f:
            logInfo = '{}|{}|{}|{}|{}\n'.format(
                year, month, imo, totalTime, log)
            f.write(logInfo)
        resultReturn = {
            'df': df,
            'imo': imo,
            'logInfo': logInfo
        }
        return resultReturn

def select_by_year_month_imo(yearMonth, imo: int):
    '''
    description: 根据年月的列表和IMO查表
    param yearMonth {list}
    param month {int}
    param imo {int}
    return {*}
    '''
    # 元素类型检查
    if not isinstance(yearMonth[0], int):
        raise TypeError('Argument {} must be {}'.format('yearMonth[0]', 'int'))
    if not isinstance(imo, int):
        raise TypeError('Argument {} must be {}'.format('imo', 'int'))

    startTime = time.time()
    db = pool.connection()
    cursor = db.cursor()

    for yearMonth in yearMonth

    table = '{}_ais_{}{}'.format(
        database, year, month) if month > 9 else '{}_ais_{}0{}'.format(database, year, month)
    sql = """
        select imo,latitude,longitude,`timestamp`,speed,draught,
        nearest_port_id,nearest_port_name,distance_from_nearest_port
        from {}
        where imo = {}
        order by `timestamp`;
    """.format(table, imo)

    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        db.commit()
        log = 'success'
        df = pd.DataFrame(results, columns=[
            'imo', 'latitude', 'longitude', 'timestamp', 'speed', 'draught',
            'nearest_port_id', 'nearest_port_name', 'distance_from_nearest_port'])
    except Exception as e:
        df = pd.DataFrame()
        db.rollback()
        log = e
    finally:
        cursor.close()
        db.close()
        totalTime = time.time()-startTime
        with open(logPathRun, 'a') as f:
            logInfo = '{}|{}|{}|{}|{}\n'.format(
                year, month, imo, totalTime, log)
            f.write(logInfo)
        resultReturn = {
            'df': df,
            'imo': imo,
            'logInfo': logInfo
        }
        return resultReturn
