#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
# import ipdb
from flask import Flask
from flask_cors import CORS

import json
import math
app = Flask(__name__)
CORS(app)
from Clustering import Clustering
#import library
import pandas as pd
import numpy as np
# %matplotlib inline
from flask import request, jsonify
from flask import Response
import warnings
warnings.filterwarnings('ignore')
import atexit
from apscheduler.scheduler import Scheduler
import psycopg2
from psycopg2 import Error
import time
cron = Scheduler(daemon=True)
# Explicitly kick off the background thread
cron.start()

@cron.interval_schedule(seconds=50)
def job_function():
  global datasets_json
  global result
  global data_analyst
  global map_result
  try:
    connection = psycopg2.connect(user = "postgres",
                                  password = "123456aA@",
                                  host = "10.60.156.15",
                                  port = "8432",
                                  database = "test_vtracking")
    cursor = connection.cursor()
    cursor.execute("select * from public.tbl_device ORDER BY RANDOM();")
    datasets_json = pd.DataFrame(cursor.fetchmany(size=50000), columns=['x', 'y','street','speed'])
    datasets_json['x'] = datasets_json['x'].apply(lambda x: float(x))
    datasets_json['y'] = datasets_json['y'].apply(lambda x: float(x))
    clustering = Clustering(datasets_json, 20)
    data_analyst = clustering._build()
    map_result = clustering.visualisation(data_analyst)
    result = clustering.export_data(data_analyst)
  except (Exception, psycopg2.Error) as error :
      print ("Error while connecting to PostgreSQL", error)
  finally:
      #closing database connection.
          if(connection):
              cursor.close()
              connection.close()
              print("PostgreSQL connection is closed")
#  Shutdown your cron thread if the web process is stopped
atexit.register(lambda: cron.shutdown(wait=False))
# =======================================
# >>>>>>>>>>>>> API <<<<<<<<<<<<<<<<<<<<<
# =======================================    

@app.route('/', methods=['GET'])
def home():
    return "<h1></p>"


@app.route('/api/v1/viettelmap/visualisation_default', methods=['GET'])
def api_map():
    global map_result
    return map_result._repr_html_()

@app.route('/api/v1/viettelmap/data', methods=['GET'])
def api_grade_congestion():
  global result
  # Check if an ID was provided as part of the URL.
  # If ID is provided, assign it to a variable.
  # If no ID is provided, display an error in the browser.
  return result

if __name__ == "__main__":
  app.run(host='10.30.176.50', port='5000', debug=True)    
