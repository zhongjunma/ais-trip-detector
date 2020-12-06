# -*- coding: UTF-8 -*-
'''
Author       : Ma Zhongjun
Date         : 2020-12-05 20:10:30
LastEditTime : 2020-12-05 20:13:07
Description  : 识别每一段可能的Trip
'''

import os,sys
os.chdir(sys.path[0])

import pandas as pd

def get_aisDf(filePath: str):
    '''
    description: 读取AIS数据并整理
    param filePath {str}
    return aisDf {pd.DataFrame}
    '''
    aisDf = pd.read_csv(filePath, header=None, sep='|')
    return aisDf
