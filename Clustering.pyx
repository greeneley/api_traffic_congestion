import pandas as pd
import numpy as np
import hdbscan
import matplotlib.pyplot as plt
# %matplotlib inline
from sklearn.cluster import DBSCAN
import time
from flask import request, jsonify
import folium
from sklearn.cluster import KMeans
from haversine import haversine, Unit
from flask import Response
import warnings
warnings.filterwarnings('ignore')
from scipy.spatial import ConvexHull
import psycopg2
from psycopg2 import Error

def StringMultiLineString(points_choose):
      result = 'MULTILINESTRING(('
      for point in points_choose:
          result = result + str(point) + ',' 
      result = result.rstrip(",")
      result = result + "))"
      result = result.replace('[', '')
      result = result.replace(']', '')
      return result

def color(grade):
    if(grade == 0):
        return "#F4D03F"
    if(grade == 1):
        return "#FD0404"
    if(grade == 2):
        return "#641E16"

def StringStreetNonCongestion(input):
    query_collection =  "select  ST_AsText(the_geom) from public.tbl_streets where ST_DWithin(ST_GeomFromText('" + str(input) + "', 4326), the_geom, 0);"
    try:
        connection = psycopg2.connect(user = "postgres",
                                      password = "123456aA@",
                                      host = "10.60.156.15",
                                      port = "8432",
                                      database = "viettelmapdb")
        
        cursor = connection.cursor()
        cursor.execute(query_collection)
        street_intersection = cursor.fetchall()
        # ============================================
        sub_feature = []
        for sub_street in street_intersection:
        #     print(sub_street)
            sub_street = sub_street[0].replace("LINESTRING", "")
            list = []
            sub_street = sub_street[1:-2].split(",")
            coordinate = {
                                    'type': 'Feature',
                                    'properties': {
                                        'color':  '#07EA00' 
                                    },
                                    'geometry': {
                                        'type': 'LineString',
                                        'coordinates': []
                                    }
                                }
            geometry = []
            for item in sub_street:
                geometry.append([float(item.split(" ")[0]), float(item.split(" ")[1])])
            coordinate['geometry']['coordinates'] = geometry
            sub_feature.append(coordinate)
        return sub_feature, street_intersection
    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                # print("PostgreSQL connection is closed")

def StringMultiStreet(input, street_intersection, grade):
    try:
        connection = psycopg2.connect(user = "postgres",
                                      password = "123456aA@",
                                      host = "10.60.156.15",
                                      port = "8432",
                                      database = "viettelmapdb")
        
        cursor = connection.cursor()
        street_multi = 'MULTILINESTRING(('
        for sub_street_intersection in street_intersection:
            street_multi = street_multi + str(sub_street_intersection)[13:-4] + ','
        street_multi = street_multi.rstrip(",") + "))"
        query_intersection_collection = str("select  ST_AsText(ST_Intersection(ST_BuildArea(ST_GeomFromText('" + str(input) + "', 4326)), ST_GeomFromText('" + str(street_multi) +  "', 4326)));") 
        cursor.execute(query_intersection_collection)
        cursor.execute(query_intersection_collection)
        points_after = cursor.fetchall()
        if points_after[0][0][0] == 'M':
            points_after = points_after[0][0].replace("MULTILINESTRING", "")
            points_after = points_after[2:-2].split("),(")
        else:
            points_after = points_after[0][0].replace("LINESTRING", "")
            points_after = points_after[1:-1].split("),(")
        list = []
        for item in points_after:
            item = item.split(",")
            list.append(item)
        sub_feature = []
        for item in list:
            coordinate = {
                                    'type': 'Feature',
                                    'properties': {
                                        'color':  color(grade) 
                                    },
                                    'geometry': {
                                        'type': 'LineString',
                                        'coordinates': []
                                    }
                                }
            geometry = []
            for sub_item in item:
                list_sub_item = sub_item.replace(" ", ",")
                geometry.append([float(list_sub_item.split(",")[0]), float(list_sub_item.split(",")[1])])
            coordinate['geometry']['coordinates'] = geometry
            sub_feature.append(coordinate)
        return sub_feature
    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                #print("PostgreSQL connection is closed")

class Clustering():

  def __init__(self, dataset, float radius):
   
    self.dataset = dataset
    self.radius = radius

  def clustering_dbscan(self, dataset, float radius):
    # radius (m) = distance maximum for each element in a cluster
    rads = np.radians(dataset)
    earth_radius_km = 6371000
    epsilon = radius / earth_radius_km #50m
    result = DBSCAN(eps = epsilon , min_samples=2,  metric='haversine', n_jobs = -1).fit(rads) # ko co min_cluster_size=10
    return result 

  def _build(self):
    new_data_frame = pd.DataFrame({"x":[], 
                    "y":[], "street": [], "speed": [], "cluster": [], "grade_congestion": [], "time_congestion": []})
    for name_street in np.unique(self.dataset['street']):
      street_value = self.dataset[self.dataset['street'] == name_street]
      clusterer = self.clustering_dbscan(street_value.ix[:,'x':'y'].values, self.radius)
      uniqueValues, occurCount = np.unique(clusterer.labels_, return_counts=True)
      frequent = {}
      for x, y in zip(uniqueValues, occurCount):
          if y < 10: clusterer.labels_[ clusterer.labels_ == x] = -1
          frequent[x] = y
      street_value.loc[:, 'cluster'] = [ x if x >= 0 else -1 for x in  clusterer.labels_]
      # ADD GRADE CONGESTION
      for row in street_value.itertuples():
          if(row.cluster == -1):
              street_value.at[row.Index,'grade_congestion'] = -1 
          elif (frequent[row.cluster] < 20):
            street_value.at[row.Index, 'grade_congestion'] = 0 
          elif ((frequent[row.cluster] < 40) and (frequent[row.cluster] >= 20)):
            street_value.at[row.Index, 'grade_congestion'] = 1
          else:
            street_value.at[row.Index, 'grade_congestion'] = 2 
      street_value['time_congestion'] = 0   
      street_value['speed_congestion'] = 0
      # ADD TIME_CONGESTION
      clusters = np.unique(street_value['cluster'].copy())
      clusters = clusters[ clusters >= 0]
      for value in clusters: 
              street_special = street_value[street_value['cluster'] == value]
              kmeans = KMeans(n_clusters=1, random_state=0).fit(street_special.loc[:,'x':'y'])
              lap = kmeans.cluster_centers_[0][0]
              lon = kmeans.cluster_centers_[0][1]
              distance = []
              for row in street_special.itertuples():
                  #distance.append(round(haversine((row.x, row.y), (lap,lon), unit=Unit.METERS), 4))
                  distance.append(round(haversine((row.x, row.y), (lap,lon)), 4))
              street_value['time_congestion'][street_value['cluster'] == value] = np.round((np.max(distance)*2)/street_special['speed'].mean()*60 + 15, 4)
              street_value['speed_congestion'][street_value['cluster'] == value] = street_special['speed'].mean()
      new_data_frame = new_data_frame.append(street_value, ignore_index = True)
    return new_data_frame
    
  def visualisation(self, new_data_frame):  
    some_map_v1 = folium.Map(location=[new_data_frame['x'].mean(), new_data_frame['y'].mean()], zoom_start=10)
    for index, row in new_data_frame.iterrows():
        if(row['grade_congestion'] == -1):
            some_map_v1.add_child(folium.Marker(location=[row['x'], row['y']],
                                              popup='<br> Lap: '+str(row['x'])+'</br><br> Long:'+str(row['y'])+'</br>',
                                              icon=folium.Icon(color='green')))
        elif(row['grade_congestion'] == 0):
              some_map_v1.add_child(folium.Marker(location=[row['x'], row['y']],
                                              popup='<br> Lap: '+str(row['x'])+'</br><br> Long:'+str(row['y'])+'</br><br> Time Congestion: '
                                              + str(row['time_congestion'])+' minute</br><br> Speed:'+str(row['speed_congestion'])+'</br>',
                                              icon=folium.Icon(color='orange')))
        elif(row['grade_congestion'] == 1):
              some_map_v1.add_child(folium.Marker(location=[row['x'], row['y']],
                                              popup='<br> Lap: '+str(row['x'])+'</br><br> Long:'+str(row['y'])+'</br><br> Time Congestion: '
                                              + str(row['time_congestion'])+' minute</br><br> Speed:'+str(row['speed_congestion'])+'</br>',
                                              icon=folium.Icon(color='red')))
        elif(row['grade_congestion'] == 2):
              some_map_v1.add_child(folium.Marker(location=[row['x'], row['y']],
                                              popup='<br> Lap: '+str(row['x'])+'</br><br> Long:'+str(row['y'])+'</br><br> Time Congestion: '
                                              + str(row['time_congestion'])+' minute</br><br> Speed:'+str(row['speed_congestion'])+'</br>',
                                              icon=folium.Icon(color='gray')))
    return some_map_v1

  def export_data(self, data):
    feature = {
                    'type': 'FeatureCollection',
                    'features': []
    }
    index = data[data['grade_congestion'] == -1].index
    data.drop(index, inplace=True)
    data = data.drop(['speed'], axis=1)
    # ListStreetNonCongestion = []
    ListSubFeature = []
    for name_street in np.unique(data['street']):
        export = data[data['street'] == name_street]
        clusters = np.unique(export['cluster'])
        for cluster in clusters:
            grade = np.unique(export['grade_congestion'][export['cluster'] == cluster]).astype(int)
            points = export.loc[:, ['y','x']][export['cluster'] == cluster].values
            allPoints=np.column_stack((points[:,0],points[:, 1]))
            hullPoints = ConvexHull(allPoints)
            polylines = np.append(hullPoints.vertices, hullPoints.vertices[0])   
            points_choose =  points[polylines]
            multiString = StringMultiLineString(points_choose)
            StreetNonCongestion, Intersection  = StringStreetNonCongestion(multiString)
            # try:
            #     for SubStreetNonCongestion in StreetNonCongestion:
            #         ListStreetNonCongestion.append(SubStreetNonCongestion)
            # except:
            #     print("Empty")
            sub_feature = StringMultiStreet(multiString, Intersection, grade)
            try:
                for x in sub_feature:
                        ListSubFeature.append(x)
            except:
                print("Empty")            
    # for item in ListStreetNonCongestion:
    #     feature['features'].append(item)
    for item_feature in ListSubFeature:
        feature['features'].append(item_feature)
    return feature 