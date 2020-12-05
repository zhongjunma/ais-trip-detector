# -*- coding: UTF-8 -*-
'''
Author       : Ma Zhongjun
Date         : 2020-11-17 16:06:14
LastEditTime : 2020-11-17 17:10:50
Description  : 多核心
'''

import os,sys
os.chdir(sys.path[0])
import pandas as pd
import multiprocessing as mp
import func

def get_portData(fileName):
    portDf = pd.read_csv(
        'data/{}'.format(fileName), header=0, sep='|',
        usecols=['id', 'area', 'type', 'lat', 'lng']
    )
    portDf = portDf.loc[portDf['type'] == 'Port', :]
    return portDf

if __name__ == '__main__':
    os.chdir(sys.path[0])
    # 设置用多少个核心
    coresNum = int(mp.cpu_count())  # 获取 CPU 的核的数量
    coresNumUsed = int(coresNum*1)  # 用全部的核运行爬虫
    print('Core 数量：' + str(coresNum))
    print('使用数量：' + str(coresNumUsed))

    portDf = get_portData('PortData.csv')

    imos = os.listdir('data/ais')[0:6]

    subListLen = int(len(imos) / coresNumUsed)
    params = {}
    for i in range(coresNumUsed):
        if i != (coresNumUsed-1):
            params['Task' + str(i)] = imos[(subListLen*i): (subListLen*(i+1))]
        else:
            params['Task' + str(i)] = imos[(subListLen*i):]

    pool = mp.Pool(coresNumUsed)  # 创建一个进程池
    for value in params.values():
        pool.apply_async(func.get_tripData, args=(value, portDf))
    pool.close()
    pool.join()

    print('')