#!/usr/bin/env python
import MySQLdb
import MySQLdb.cursors
import config
import requests
import json
from pprint import pprint
from datetime import datetime, timedelta
import pytz

tz = pytz.timezone('US/Pacific')

import httplib2
import urllib2
from apiclient.discovery import build
from oauth2client import client

ONE_DAY_MS = 86400000
STEPS_DATASOURCE = "derived:com.google.step_count.delta:com.google.android.gms:estimated_steps"
ACTIVITY_DATASOURCE = "derived:com.google.activity.segment:com.google.android.gms:merge_activity_segments"

def get_aggregate(fit_service, startTimeMillis, endTimeMillis, dataSourceId):
  return fit_service.users().dataset().aggregate(userId="me", body={
    "aggregateBy": [{
      "dataTypeName": "com.google.step_count.delta",
      "dataSourceId": dataSourceId
    }],
    "bucketByTime": { "durationMillis": ONE_DAY_MS },
    "startTimeMillis": startTimeMillis,
    "endTimeMillis": endTimeMillis
  }).execute()

def get_and_store_fit_data(http_auth, cur, username):
  fit_service = build('fitness', 'v1', http=http_auth)
  now = tz.localize(datetime.now())
  lastMonth = now - timedelta(days=30)
  lastMonth = tz.localize(datetime(lastMonth.year, lastMonth.month, lastMonth.day))
  now = int(now.strftime("%s")) * 1000
  lastMonth = int(lastMonth.strftime("%s")) * 1000
#  print(help(fit_service.users().dataset().aggregate))
  steps = []
  activity = []
  print(username)
  try:
    stepsData = get_aggregate(fit_service, lastMonth, now, STEPS_DATASOURCE)
    for day in stepsData['bucket']:
      if day['dataset'][0]['point']:
        d = datetime.fromtimestamp(int(day['startTimeMillis'])/1000).strftime('%Y-%m-%d')
        s = day['dataset'][0]['point'][0]['value'][0]['intVal']
        steps.append([d, s])
    print("Steps:", steps)
  except Exception as e:
    print(e)
    print("No steps found")
  try:
    activityData = get_aggregate(fit_service, lastMonth, now, ACTIVITY_DATASOURCE)
    for day in activityData['bucket']:
      if day['dataset'][0]['point']:
        d = datetime.fromtimestamp(int(day['startTimeMillis'])/1000).strftime('%Y-%m-%d')
        for a in day['dataset'][0]['point']:
          activity_type = a['value'][0]['intVal']
          length_ms = a['value'][1]['intVal']
          n_segments = a['value'][2]['intVal']
          activity.append([d, activity_type, length_ms, n_segments])
    print("Activity:", activity)
  except Exception as e:
    print(e)
    print("No activity found")
  try:
    rows = cur.executemany("REPLACE INTO steps SET username=%s, day=%s, steps=%s".format(username), [[username] + s for s in steps])
    print("steps: {} rows affected".format(rows))
    rows = cur.executemany("REPLACE INTO activity SET username=%s, day=%s, activity_type=%s, length_ms=%s, n_segments=%s", [[username] + a for a in activity])
    print("activity: {} rows affected".format(rows))
  except Exception as e:
    print(e)
  return steps, activity

if __name__ == "__main__":
  with open('client_secret.json') as f:
    client_secret_json = json.load(f)
    client_id = client_secret_json['web']['client_id']
    client_secret = client_secret_json['web']['client_secret']
  
  db = MySQLdb.connect(host=config.dbhost, user=config.dbuser, passwd=config.dbpass, db=config.dbname, cursorclass=MySQLdb.cursors.DictCursor)
  cur = db.cursor()
  n_rows = cur.execute("SELECT * FROM google_fit")
  rows = cur.fetchall()
  for r in rows:
    username = r['username']
    refresh_token = r['refresh_token']
    creds = client.GoogleCredentials("", client_id, client_secret, refresh_token, 0, "https://accounts.google.com/o/oauth2/token", "Python")
    http_auth = creds.authorize(httplib2.Http())
    steps, activity = get_and_store_fit_data(http_auth, cur, username)
    db.commit()
  cur.close()
  # disconnect from server
  db.close()

