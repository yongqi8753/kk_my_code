# -*- coding: utf-8 -*-

from math import radians, cos, sin, asin, sqrt
import pandas as pd
from tqdm.notebook import tqdm

def geodistance(lng1,lat1,lng2,lat2):
    '''
    Calculate the distance based on longitude and latitude

    Parameters
    ----------
    lng1 : TYPE
        DESCRIPTION.
    lat1 : TYPE
        DESCRIPTION.
    lng2 : TYPE
        DESCRIPTION.
    lat2 : TYPE
        DESCRIPTION.

    Returns
    -------
    distance : float
        the distance between two points. The unit is KM.

    '''
    #lng1,lat1,lng2,lat2 = (120.12802999999997,30.28708,115.86572000000001,28.7427)
    lng1, lat1, lng2, lat2 = map(radians, [float(lng1), float(lat1), float(lng2), float(lat2)]) # 经纬度转换成弧度
    dlon=lng2-lng1
    dlat=lat2-lat1
    a=sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    distance=2*asin(sqrt(a))*6371*1000 # the radius of the Earth，6371km
    distance=round(distance/1000,3)
    return distance

def Groupdata_by_vehicleid(data):
    '''
    Given one type of micro-mobility service, group the data by vehicleid 

    Parameters
    ----------
    data : DataFrame
        The extracted records for one type of service.

    Returns
    -------
    the recoreds grouped by vehicle id.

    '''
    vehicle_ids = data['bikeid'].unique()
# #    vehicle_id_counts = data['bikeid'].value_counts()
    Records_List = [] # a list of dataframes, each dataframe stores the records of one scooter
    for i in tqdm(range(len(vehicle_ids))):
        vid = vehicle_ids[i]
        temp = data[data['bikeid']==vid]
        temp.sort_values('timestamp', inplace=True)
        Records_List.append(temp)
        # print(i)
    # groups = data.groupby('bikeid')
    return Records_List

def Compare_geolocation(record1,record2):
    '''
    To check whether two records have the same geolocations

    Parameters
    ----------
    record1 : Series
        DESCRIPTION.
    record2 : Series
        DESCRIPTION.

    Returns
    -------
    Boolean variable.

    '''
    flag = True
    lon1 = record1['geom_lon']
    lat1 = record1['geom_lat']
    lon2 = record2['geom_lon']
    lat2 = record2['geom_lat']
    if (lon1==lon2)&(lat1==lat2):
        flag = True
    else:
        flag = False
    return flag

def Centroid(signal_drift):
    '''
    Calculate the centroid of multiple points

    Parameters
    ----------
    signal_drift : TYPE
        DESCRIPTION.

    Returns: average longitude and latitude
    -------
    Lon : TYPE
        DESCRIPTION.
    Lat : TYPE
        DESCRIPTION.

    '''
    Lon = 0
    Lat = 0
    for i in range(len(signal_drift)):
        coordinates = signal_drift[i]
        Lon += coordinates[0]
        Lat += coordinates[1]
    Lon = Lon/len(signal_drift)
    Lat = Lat/len(signal_drift)
    return Lon, Lat

                
def RecognizeTrips_df(location_data,dist_threshold):
    '''
    Recognize trips from the data records after the filtering
    step1: compare the locations of two adjacent points to check whether there is drift
           if yes, calculate the distance between them, and then compare the distance with the threshold
    step2: if the distance is less than threshold, it indicates that signal drifting is existed.
           Store all the drifting points in a list, and calculate the centroid of them

    Parameters
    ----------
    data : df
        The list of groups. Each group stores the records of one scooter in the
        form of dataframe.
    dist_threshold : float
        定位误差，单位为米

    Returns
    -------
    stop_list : TYPE
        DESCRIPTION.

    '''
    bikes = location_data['bikeid'].unique()
    trip_list = []
    for bike_id in tqdm(bikes):
        # print(i)
        group = location_data[location_data['bikeid']==bike_id].reset_index(drop=True)
        record1 = group.iloc[0]
        stop_list = []
        trip = []
        signal_drift = []
        for j in range(1,len(group)):
            record2 = group.iloc[j]                             
            flag = Compare_geolocation(record1,record2)
            if flag: #indicating no drift
                record1 = record2
            else: #Then calculate the average center
                lon1 = record1['geom_lon']
                lat1 = record1['geom_lat']
                lon2 = record2['geom_lon']
                lat2 = record2['geom_lat']
                dist = geodistance(lon1,lat1,lon2,lat2) * 1000 # unit is meter
                if dist > dist_threshold:
                    if(len(signal_drift)==0):
                        stop_list.append(record1)
                        stop_list.append(record2)
                        trip.append(stop_list)
                    else:
                        Lon, Lat = Centroid(signal_drift)
                        record1['geom_lon'] = Lon
                        record1['geom_lat'] = Lat
                        stop_list.append(record1)
                        stop_list.append(record2)
                        trip.append(stop_list)
                    stop_list = []
                    record1 = record2
                    signal_drift = []
                else:
                    signal_drift.append([lon1,lat1])
                    signal_drift.append([lon2,lat2])
                    record1 = record2
        trip_list.append(trip)           
    return trip_list            

def Generate_trip(trip_list):
    '''
        Convert origin and destination records into trip. Each trip corresponds to one
    record.

    Parameters
    ----------
    trip_list : list
        the list of series. Each series contains the origin and destination of one trip.

    Returns
    -------
    trips : dataframe
        each row corresponds to one trip.

    '''
    trips = pd.DataFrame(columns=['vehicle_id', 'stime', 'etime','slng','slat','elng','elat','duration','length','speed'])
    for i in tqdm(range(len(trip_list))):
        # print(i)
        if len(trip_list[i])>0:
            for j in range(len(trip_list[i])):
                temp_list = []
                vid = trip_list[i][j][0]['bikeid']
                stime = trip_list[i][j][0]['timestamp']
                slng = trip_list[i][j][0]['geom_lon']
                slat = trip_list[i][j][0]['geom_lat']
                etime = trip_list[i][j][1]['timestamp']
                elng = trip_list[i][j][1]['geom_lon']
                elat = trip_list[i][j][1]['geom_lat']
                
                #####trip filtering
                duration = (etime - stime).seconds
                distance = geodistance(slng,slat,elng,elat) * 1000 
                if (duration>60)&(duration<7200)&(distance>100)&(distance<10000):
                    speed = distance/duration*3.6  ###unit is km/h
                    if speed <= 25:
                        temp_list.append(vid)
                        temp_list.append(stime)
                        temp_list.append(etime)
                        temp_list.append(slng)
                        temp_list.append(slat)
                        temp_list.append(elng)
                        temp_list.append(elat)
                        temp_list.append(duration)
                        temp_list.append(distance)
                        temp_list.append(speed)
                        # s = pd.Series(temp_list)
                        size = trips.index.size
                        trips.loc[size] = temp_list
    return trips
                

                