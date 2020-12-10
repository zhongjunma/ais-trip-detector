# -*- coding: UTF-8 -*-
'''
Author       : Ma Zhongjun
Date         : 2020-12-05 20:10:30
LastEditTime : 2020-12-06 20:01:30
Description  : 识别每一段可能的Trip
'''

import os
import sys

import numpy as np
import pandas as pd
from geopy import distance
from tqdm import tqdm

os.chdir(sys.path[0])

portDf = pd.read_csv('data/PortInfo.csv', header=0, sep='|')


def get_aisDf(filePath: str):
    '''
    description: 读取AIS数据并整理
    param filePath {str}
    return aisDf {pd.DataFrame}
    '''
    aisDf = pd.read_csv(filePath, header=None, sep='|')
    aisDf['timestamp'] = pd.to_datetime(
        aisDf['timestamp'])  # 将object转化为timestamp

    # 所有速度小于 2 节都认为是无动力的低速状态
    aisDf['is_low_speed'] = False
    aisDf.loc[aisDf['speed'] <= 2, 'is_low_speed'] = True

    aisDf['is_in_port'] = False  # 船是否在港口的标签

    #  vectorize，计算距离和时间间隔
    aisDfTemp = aisDf.loc[aisDf.index[1:], [
        'latitude', 'longitude', 'timestamp']]  # 去掉第一行
    aisDfTemp.rename(columns={
        'latitude': 'latitude2',
        'longitude': 'longitude2',
        'timestamp': 'timestamp2'
    }, inplace=True)
    aisDf.drop(index=aisDf.index[-1], inplace=True)  # 去掉最后一行
    aisDfTemp.reset_index(drop=True, inplace=True)
    aisDf = pd.concat([aisDf, aisDfTemp], axis=1)

    def cal_distance_and_timeinterval(row):
        row['point_distance'] = distance.distance(
            (row['latitude'], row['longitude']),
            (row['latitude2'], row['longitude2'])
        ).nm
        row['time_interval'] = (row['timestamp2'] -
                                row['timestamp']).total_seconds()
        return row
    tqdm.pandas(desc='Cal distance')
    aisDf = aisDf.progress_apply(cal_distance_and_timeinterval, axis=1)
    aisDf['is_long_time_interval'] = False
    aisDf.loc[aisDf['time_interval'] >= 259200,
              'is_long_time_interval'] = True  # 超过3天
    aisDf['is_long_time_interval'].fillna(
        method='ffill', limit=1, inplace=True)
    aisDf.drop(columns=['latitude2', 'longitude2', 'timestamp2'], inplace=True)

    return aisDf


def divide_trips(aisDf):
    '''
    description: 分出每一段小trip
    param aisDf {pd.DataFrame}
    param portDf {pd.DataFrame} 港口的数据
    return aisDf {pd.DataFrame}
    '''

    def find_nearby_ports(lat, lon):
        '''
        description: 寻找经纬度附近最近的港口
        param lat {float}
        param lon {float}
        return isNearbyPorts, PortID, Distance to nearst port
        '''
        # 按照经纬度找到附近的港口，缩小范围
        portDfSub = portDf.loc[
            (portDf['lat'] >= lat-1) &
            (portDf['lat'] <= lat+1) &
            (portDf['lng'] >= lon-1) &
            (portDf['lng'] <= lon+1), :
        ]

        def cal_distance_to_port(row, coordinate):
            row['distance'] = distance.distance(
                coordinate, (row['lat'], row['lng'])).nm
            return row
        portDfSub = portDfSub.apply(
            cal_distance_to_port, coordinate=(lat, lon), axis=1)
        try:
            minDistanceIdx = portDfSub.loc[:, 'distance'].idxmin()
            portID = portDfSub.loc[minDistanceIdx, 'id']
            portDistance = portDfSub.loc[minDistanceIdx, 'distance']
            portArea = portDfSub.loc[minDistanceIdx, 'area']
            if portDistance <= 5:  # 5 海里大于 10 千米
                return True, portID, portDistance, portArea
            else:
                return False, -1, -1, None
        except KeyError:
            return False, -2, -2, None

    tripNo = 0
    tripDraft = aisDf.loc[0, 'draught']
    for ind in aisDf.index:
        if aisDf.loc[ind, 'is_low_speed']:
            try:
                if aisDf.loc[ind-1, 'is_in_port']:
                    # 如果前一次已经检查出在港口附近，而这一次又是低速
                    # 说明这一次也应该在港口中，不需要再计算距离了
                    aisDf.loc[ind, 'is_in_port'] = True
                    aisDf.loc[ind, 'trip_no'] = tripNo
                else:
                    # 如果前一次没有检查出在港口附近，而这一次又是低速
                    # 说明这一次需要计算距离，检查是否位于港口附近
                    isInPort, portID, portDistance, portArea = find_nearby_ports(
                        aisDf.loc[ind, 'latitude'], aisDf.loc[ind, 'longitude'], portDf)
                    if isInPort:
                        aisDf.loc[ind, 'is_in_port'] = isInPort
                        aisDf.loc[ind, 'port_id'] = portID
                        aisDf.loc[ind, 'port_distance'] = portDistance
                        aisDf.loc[ind, 'port_area'] = portArea
                        aisDf.loc[ind, 'port_area_start'] = portArea
                        aisDf.loc[ind-1, 'port_area_end'] = portArea
                        aisDf.loc[ind, 'port_id_start'] = portID
                        aisDf.loc[ind-1, 'port_id_end'] = portID
                        aisDf.loc[ind, 'trip_no'] = tripNo
                        aisDf.loc[ind-1, 'trip_draft'] = tripDraft
                    pass
            except KeyError:
                # 如果第一行就是低速状态，就需要直接检查
                isInPort, portID, portDistance, portArea = find_nearby_ports(
                    aisDf.loc[ind, 'latitude'], aisDf.loc[ind, 'longitude'], portDf)
                if isInPort:
                    aisDf.loc[ind, 'is_in_port'] = isInPort
                    aisDf.loc[ind, 'port_iD'] = portID
                    aisDf.loc[ind, 'port_distance'] = portDistance
                    aisDf.loc[ind, 'port_area'] = portArea
                    aisDf.loc[ind, 'port_area_start'] = portArea
                    aisDf.loc[ind, 'port_id_start'] = portID
                    aisDf.loc[ind, 'trip_no'] = tripNo
        else:
            # 当前时间戳不是低速状态，但有可能夹杂在低速状态之间的，所以这里也应该更新
            try:
                if aisDf.loc[ind-1, 'is_in_port']:
                    if aisDf.loc[ind+1, 'is_low_speed'] and aisDf.loc[ind, 'speed'] <= 3:
                        aisDf.loc[ind, 'is_low_speed'] = True
                        aisDf.loc[ind, 'is_in_port'] = True
                        aisDf.loc[ind, 'trip_no'] = tripNo
                    else:
                        tripNo += 1
            except KeyError:
                pass

        # 始终保持trip draft为最后更新的那一次
        currentDraft = aisDf.loc[ind, 'draught']
        if pd.isnull(tripDraft):
            if pd.isnull(currentDraft):
                pass
            else:
                tripDraft = currentDraft
        else:
            if pd.isnull(currentDraft):
                pass
            elif currentDraft != tripDraft:
                tripDraft = currentDraft

    aisDf['trip_no'].fillna(method='ffill', inplace=True)
    aisDf['trip_draft'].fillna(method='bfill', inplace=True)
    aisDf['port_area_start'].fillna(method='ffill', inplace=True)
    aisDf['port_area_end'].fillna(method='bfill', inplace=True)
    aisDf['port_id_start'].fillna(method='ffill', inplace=True)
    aisDf['port_id_end'].fillna(method='bfill', inplace=True)

    return aisDf
