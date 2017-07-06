#!/usr/bin/env python
import MySQLdb
import MySQLdb.cursors
import config
import requests
import json
from pprint import pprint
from datetime import datetime, timedelta

import httplib2
import urllib2
from apiclient.discovery import build
from oauth2client import client

ONE_DAY_MS = 86400000

def get_fit_data(http_auth):
  fit_service = build('fitness', 'v1', http=http_auth)
  now = datetime.now()
  lastMonth = now - timedelta(days=30)
  lastMonth = datetime(lastMonth.year, lastMonth.month, lastMonth.day)
  now = int(now.strftime("%s")) * 1000
  lastMonth = int(lastMonth.strftime("%s")) * 1000
#  print(help(fit_service.users().dataset().aggregate))
  try:
    response = fit_service.users().dataset().aggregate(userId="me", body={
      "aggregateBy": [{
        "dataTypeName": "com.google.step_count.delta",
        "dataSourceId": "derived:com.google.step_count.delta:com.google.android.gms:estimated_steps"
      }],
      "bucketByTime": { "durationMillis": ONE_DAY_MS },
      "startTimeMillis": lastMonth,
      "endTimeMillis": now
    }).execute()
  except:
    print("No steps found")
    return {}
  days = []
  for day in response['bucket']:
    if day['dataset'][0]['point']:
      d = datetime.fromtimestamp(int(day['startTimeMillis'])/1000).strftime('%Y-%m-%d')
      steps = day['dataset'][0]['point'][0]['value'][0]['intVal']
      days.append((d, steps))
  return days

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
    fit_data = get_fit_data(http_auth)
    print(fit_data)
    rows = cur.executemany("REPLACE INTO steps SET username='{}', day=%s, steps=%s".format(username), fit_data)
    db.commit()
    print("{} rows affected".format(rows))
  cur.close()
  # disconnect from server
  db.close()

