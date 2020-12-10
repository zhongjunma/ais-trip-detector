# -*- coding: UTF-8 -*-
'''
Author       : Ma Zhongjun
Date         : 2020-11-12 21:19:06
LastEditTime : 2020-11-21 20:54:01
Description  : 为 AIS data 划分 Trip
'''

import os
import sys

import numpy as np
import pandas as pd
from geopy import distance
from tqdm import tqdm

os.chdir(sys.path[0])


def get_aisData(fileName):
    aisDf = pd.read_csv(
        'data/ais/{}'.format(fileName), header=0, sep=',',
        usecols=[
            'Latitude', 'Longitude', 'Timestamp', 'NearestPortID',
            'DistanceFromNearestPort', 'Speed', 'Draught'
        ]
    )
    aisDf['Timestamp'] = pd.to_datetime(aisDf['Timestamp'])
    aisDf.sort_values(by=['Timestamp'], inplace=True)
    aisDf.reset_index(drop=True, inplace=True)
    # 所有速度小于 2 节都认为是无动力的低速状态
    aisDf['isLowSpeed'] = False
    aisDf.loc[aisDf['Speed'] <= 2, 'isLowSpeed'] = True
    # 初始认为假设所有船都不在港口，之后一点一点更新
    aisDf['isInPort'] = False
    aisDfTemp = aisDf.loc[aisDf.index[1:], [
        'Latitude', 'Longitude', 'Timestamp']]
    aisDfTemp.rename(columns={
        'Latitude': 'Latitude2',
        'Longitude': 'Longitude2',
        'Timestamp': 'Timestamp2'
    }, inplace=True)
    aisDf.drop(index=aisDf.index[-1], inplace=True)
    aisDfTemp.reset_index(drop=True, inplace=True)
    aisDf = pd.concat([aisDf, aisDfTemp], axis=1)

    def cal_distance_and_timeinterval(row):
        row['pointDistance'] = distance.distance(
            (row['Latitude'], row['Longitude']),
            (row['Latitude2'], row['Longitude2'])
        ).nm
        row['timeInterval'] = (row['Timestamp2'] -
                               row['Timestamp']).total_seconds()
        return row
    # tqdm.pandas(desc='Cal distance')
    # aisDf = aisDf.progress_apply(cal_distance_and_timeinterval, axis=1)
    aisDf = aisDf.apply(cal_distance_and_timeinterval, axis=1)
    aisDf['isLongTimeInterval'] = False
    aisDf.loc[aisDf['timeInterval'] >= 259200, 'isLongTimeInterval'] = True
    return aisDf


def divide_trips(aisDf, portDf):
    '''
    description: 分出每一段trip
    param aisDf {pd.Dataframe}
    return {*}
    '''

    def find_nearby_ports(lat, lon, portDf):
        '''
        description: 寻找经纬度附近最近的港口
        param lat {float}
        param lon {float}
        return isNearbyPorts, PortID, Distance to nearst port
        '''
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
            if portDistance <= 10:  # 10 海里大于 20 千米
                return True, portID, portDistance, portArea
            else:
                return False, -1, -1, None
        except KeyError:
            return False, -2, -2, None

    tripNo = 0
    tripDraft = aisDf.loc[0, 'Draught']
    for ind in aisDf.index:
        if aisDf.loc[ind, 'isLowSpeed']:  # 低速时检查是否位于港口附近
            try:
                if aisDf.loc[ind-1, 'isInPort']:
                    # 如果前一次已经检查出在港口了，而这一次又是低速
                    # 说明这一次也应该在港口中，不需要再计算距离了
                    aisDf.loc[ind, 'isInPort'] = True
                    aisDf.loc[ind, 'tripNo'] = tripNo
                else:
                    # 前一次没有检查出在港口附近，所以这一次需要计算距离，检查是否位于港口附近
                    isInPort, portID, portDistance, portArea = find_nearby_ports(
                        aisDf.loc[ind, 'Latitude'], aisDf.loc[ind, 'Longitude'], portDf)
                    if isInPort:
                        aisDf.loc[ind, 'isInPort'] = isInPort
                        aisDf.loc[ind, 'PortID'] = portID
                        aisDf.loc[ind, 'PortDistance'] = portDistance
                        aisDf.loc[ind, 'PortArea'] = portArea
                        aisDf.loc[ind, 'PortAreaStart'] = portArea
                        aisDf.loc[ind-1, 'PortAreaEnd'] = portArea
                        aisDf.loc[ind, 'tripNo'] = tripNo
                        aisDf.loc[ind-1, 'tripDraft'] = tripDraft
                    pass
            except KeyError:
                # 第一行需要检查
                isInPort, portID, portDistance, portArea = find_nearby_ports(
                    aisDf.loc[ind, 'Latitude'], aisDf.loc[ind, 'Longitude'], portDf)
                if isInPort:
                    aisDf.loc[ind, 'isInPort'] = isInPort
                    aisDf.loc[ind, 'PortID'] = portID
                    aisDf.loc[ind, 'PortDistance'] = portDistance
                    aisDf.loc[ind, 'PortArea'] = portArea
                    aisDf.loc[ind, 'PortAreaStart'] = portArea
                    aisDf.loc[ind-1, 'PortAreaEnd'] = portArea
                    aisDf.loc[ind, 'tripNo'] = tripNo
                    # aisDf.loc[ind, 'CurrentDraft'] = aisDf.loc[ind, 'Draught']
        else:
            try:
                if aisDf.loc[ind-1, 'isInPort']:
                    if aisDf.loc[ind+1, 'isLowSpeed'] and aisDf.loc[ind, 'Speed'] <= 3:
                        aisDf.loc[ind, 'isLowSpeed'] = True
                        aisDf.loc[ind, 'isInPort'] = True
                        aisDf.loc[ind, 'tripNo'] = tripNo
                    else:
                        tripNo += 1
            except KeyError:
                pass
        currentDraft = aisDf.loc[ind, 'Draught']
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

    aisDf['tripNo'].fillna(method='ffill', inplace=True)
    aisDf.dropna(subset=['tripNo'], inplace=True)
    aisDf['tripDraft'].fillna(method='bfill', inplace=True)
    aisDf['PortAreaStart'].fillna(method='ffill', inplace=True)
    aisDf['PortAreaEnd'].fillna(method='bfill', inplace=True)

    # 怎么处理时间间隔较长的情况？直接删除？
    # aisDf.drop(
    #     index=aisDf.loc[
    #         aisDf['tripNo'].isin(
    #             aisDf.loc[aisDf['isLongTimeInterval'] == True, 'tripNo'].unique()
    #         ),:].index, inplace=True
    # )
    return aisDf


def get_tripData(fileNames, portDf):
    for fileName in tqdm(fileNames):
        aisDf = get_aisData(fileName)
        aisDf = divide_trips(aisDf, portDf)
        aisDf.to_csv('result/{}'.format(fileName), index=False, sep=',')
        print('save file')

def get_portData(fileName):
    portDf = pd.read_csv(
        'data/{}'.format(fileName), header=0, sep='|',
        usecols=['id', 'area', 'type', 'lat', 'lng']
    )
    portDf = portDf.loc[portDf['type'] == 'Port', :]
    return portDf
    
def get_loadStatus(fileName, tankerDf, tripDf):
    aisDf = pd.read_csv('result/{}'.format(fileName), header=0, sep=',')
    imo = int(os.path.splitext(fileName)[0])
    designDraft = tankerDf.loc[tankerDf['imo']==imo,'draught'].values[0]
    shipSize = tripDf.loc[tripDf['Imo']==imo, 'ShipSize'].values[0]
    aisDf['draftRatio'] = aisDf['tripDraft'] / designDraft

    afraUnloadedThreshold = 0.6525830
    afraPartLadenThreshold = 0.8161636
    panaUnloadedThreshold = 0.6871930
    panaPartLadenThreshold = 0.7866115

    if shipSize == 'Aframax':
        aisDf['loadStatus'] = 'Loaded'
        aisDf.loc[aisDf['draftRatio'] <= afraPartLadenThreshold, 'loadStatus'] = 'PartLaden'
        aisDf.loc[aisDf['draftRatio'] <= afraUnloadedThreshold, 'loadStatus'] = 'Unloaded'
    elif shipSize == 'Panamax':
        aisDf['loadStatus'] = 'Loaded'
        aisDf.loc[aisDf['draftRatio'] <= panaPartLadenThreshold, 'loadStatus'] = 'PartLaden'
        aisDf.loc[aisDf['draftRatio'] <= panaUnloadedThreshold, 'loadStatus'] = 'Unloaded'
    else:
        raise Exception('Wrong Type!')

    aisDf.to_csv('result/{}'.format(fileName), index=False, sep=',')

def clean_ais(fileName):
    aisDf = pd.read_csv('result/{}'.format(fileName), header=0, sep=',')
    aisDf['Timestamp'] = pd.to_datetime(aisDf['Timestamp'], utc=True)
    cleanDf = pd.DataFrame(
        columns={
            'date':[]
            # 'lat':[],
            # 'lon':[],
            # 'speed':[],
            # 'draft':[],
            # 'is_low_speed':[],
            # 'is_in_port':[],
            # 'load_status':[],
            # 'trip_no':[],
            # 'trip_start_area':[],
            # 'trip_end_area':[]
        }
    )
    cleanDf['date'] = pd.date_range(start='2017-01-01',end='2020-12-31',freq='D').astype(str)

    def get_firstRow(df):
        try:
            return df.iloc[[0],]
        except IndexError:
            return df

    aisDfTemp = aisDf.groupby([pd.Grouper(key='Timestamp', freq='D')]).apply(get_firstRow)
    aisDfTemp.rename(
        columns={
            'Latitude': 'lat',
            'Longitude': 'lon',
            'Timestamp': 'date',
            'Speed':'speed',
            'Draught':'draft',
            'isLowSpeed':'is_low_speed',
            'isInPort':'is_in_port',
            'loadStatus':'load_status',
            'tripNo':'trip_no',
            'tripDraft':'trip_draft',
            'PortAreaStart':'trip_start_area',
            'PortAreaEnd':'trip_end_area'
        },
        inplace=True
    )
    aisDfTemp = aisDfTemp.loc[:,['date','lat','lon','speed','draft','is_low_speed','is_in_port','load_status','trip_no','trip_draft','trip_start_area','trip_end_area']]
    aisDfTemp['date'] = pd.to_datetime(aisDfTemp['date']).dt.date.astype(str)
    aisDfTemp.reset_index(drop=True, inplace=True)
    cleanDf = pd.merge(cleanDf, aisDfTemp, how='left', on=['date'], validate='1:1')
    # aisDfTemp.to_csv('result/test.csv',index=False,sep=',')
    # cleanDf.to_csv('result/9169512_cleaned_test.csv',index=False,sep=',')

    cleanDf.reset_index(drop=True, inplace=True)
    cleanDf['load_status_ffill'] = cleanDf['load_status']
    cleanDf['trip_no_ffill'] = cleanDf['trip_no']
    cleanDf['load_status_ffill'].fillna(method='ffill', limit=3, inplace=True)
    cleanDf['trip_no_ffill'].fillna(method='ffill', limit=3, inplace=True)
    tripNoPredict = -1
    for ind in cleanDf.index:
        # if tripNoPredict == -1:
        #     if pd.isnull(cleanDf.loc[ind, 'load_status_ffill']):
        #         continue
        #     else:
        #         tripNoPredict = 0
        try:
            if pd.isnull(cleanDf.loc[ind, 'load_status_ffill']):
                cleanDf.loc[ind, 'trip_no_predict'] = -1
                continue
            if cleanDf.loc[ind, 'trip_no_ffill'] == cleanDf.loc[ind-1, 'trip_no_ffill']:
                continue
            if pd.isnull(cleanDf.loc[ind+1, 'load_status_ffill']):
                cleanDf.loc[ind, 'trip_no_predict'] = tripNoPredict
                continue
            if (cleanDf.loc[ind, 'load_status_ffill'] != 'Unloaded' and
            (cleanDf.loc[ind-1, 'load_status_ffill'] == 'Unloaded' or pd.isnull(cleanDf.loc[ind-1, 'load_status_ffill']))):
                tripNoPredict += 1
                cleanDf.loc[ind, 'trip_no_predict'] = tripNoPredict
        except KeyError:
            pass
    cleanDf['trip_no_predict'].fillna(method='ffill',inplace=True)
    def get_area(df):
        if df['trip_no_predict'].iloc[0] != -1:
            try:
                df.loc[df['load_status_ffill']!='Unloaded', 'trip_end_area_predict'] = df.loc[df['load_status_ffill']!='Unloaded', 'trip_end_area'].dropna().iloc[-1]
                df.loc[df['load_status_ffill']!='Unloaded', 'trip_start_area_predict'] = df.loc[df['load_status_ffill']!='Unloaded', 'trip_start_area'].dropna().iloc[0]
                df.loc[df['load_status_ffill']=='Unloaded', 'trip_no_predict'] = -1
            except IndexError:
                df['trip_no_predict'] = -1
                df['trip_start_area_predict'] = None
                df['trip_end_area_predict'] = None
        else:
            df['trip_start_area_predict'] = None
            df['trip_end_area_predict'] = None
        return df
    cleanDf['trip_no_predict_copy'] = cleanDf['trip_no_predict']
    cleanDf = cleanDf.groupby(by=['trip_no_predict_copy']).apply(get_area)
    cleanDf['trip_no_predict'].replace({-1:np.nan},inplace=True)
    cleanDf.drop(columns=['trip_no_predict_copy'], inplace=True)
    cleanDf.to_csv('result/clean/{}'.format(fileName),index=False,sep=',')
    pass


def cal_averageDesignDraft(tankerDf, tripDf):

    def cal_by_shipSize(shipSize):
        imos = tripDf.loc[tripDf['ShipSize']==shipSize,'Imo'].unique().tolist()
        draughts = tankerDf.loc[tankerDf['imo'].isin(imos),'draught']
        print('{}: min is {}, max is {}, mean is {}.'.format(shipSize, draughts.min(), draughts.max(), draughts.mean()))
        draughts = np.sort(draughts)
        draughts = draughts[1:-1]
        # draughts.to_csv('result/dwt_draft_{}.csv'.format(shipSize), index=False, sep=',')
        print('{}: min is {}, max is {}, mean is {}.'.format(shipSize, draughts.min(), draughts.max(), draughts.mean()))
    
    cal_by_shipSize('Aframax')
    cal_by_shipSize('Panamax')

# fileName = '9169512.csv'
tankerDf = pd.read_csv('data/tankerInfo.csv', header=0, sep=',')
tripDf = pd.read_csv('data/TripData.csv', header=0, sep=',')
# clean_ais(fileName)
cal_averageDesignDraft(tankerDf, tripDf)