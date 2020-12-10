# -*- coding: UTF-8 -*-
'''
Author       : Ma Zhongjun
Date         : 2020-11-26 22:38:49
LastEditTime : 2020-12-06 20:13:48
Description  : 多线程、异步返回
'''

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from sql_controller import select_by_year_month_imo

os.chdir(sys.path[0])

logPathResult = 'result/container/select_year_month_imo/log_result.csv'
print('Log file path (Result): {}'.format(logPathResult))

def run_multiThreadTasks(func, args, executorNum):
    '''
    description: 多线程执行函数func
    param {function} func，函数名
    param {list} args，函数func需要的的参数，list中的元素为tuple
    param {int} executorNum，根据需要设置，可以超线程所以最大应该是二倍核心数
    return {list} results，函数返回的参数，list中的元素为tuple
    '''
    startTime = time.time()

    executor = ThreadPoolExecutor(executorNum)
    tasks = [executor.submit(lambda p: func(*p), arg)
             for arg in args]
    results = [future.result() for future in as_completed(tasks)]

    endTime = time.time()
    print('Total time is {}s.'.format(endTime-startTime))
    return results


def get_args():
    df = pd.read_csv(
        'data/containerInfo_dwtGreaterThan3000.csv', header=0, sep=',')
    imos = df['imo'].values.tolist()
    args = [
        (year, month, imo) for imo in imos[0:4000]
        for year in range(2016,2020+1)
        for month in range(1,12+1)
    ]
    return args


def save_results(results):
    df = pd.read_csv(
        'data/containerInfo_dwtGreaterThan3000.csv', header=0, sep=',')
    imos = df['imo'].values.tolist()
    resultDfDict = {
        imo: [] for imo in imos
    }
    with open(logPathResult,'a') as f:
        for result in results:
            resultDfDict[result['imo']].append(result['df'])
            f.write(result['logInfo'])
    
    for key, value in resultDfDict.items():
        pd.concat(value).to_csv('E:/AIS_Container_dwt3000_2016-2020/{}.csv'.format(key), index=False, sep='|')
        


if __name__ == '__main__':
    # args = [(year, month, 'imo') for year in range(2011, 2019+1)
    #         for month in range(1, 12+1)]

    args = get_args()
    print(args)
    print('The length of args is {}.'.format(len(args)))

    # 设置足够数量的线程
    executorNum = len(args) if len(args) <= 64*2 else 64*2

    results = run_multiThreadTasks(select_by_year_month_imo, args, executorNum)

    save_results(results)
    # with open(logPathResult,'a') as f:
    #     for result in results:
    #         f.write(result)
