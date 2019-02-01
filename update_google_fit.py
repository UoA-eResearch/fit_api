#!/usr/bin/env python
import json
from datetime import datetime

import MySQLdb.cursors
import httplib2
import pytz
from googleapiclient.discovery import build
from oauth2client import client

import backend


def get_aggregate(fit_service, startTimeMillis, endTimeMillis, dataSourceId):
    return fit_service.users().dataset().aggregate(userId="me", body={
        "aggregateBy": [{
            "dataTypeName": "com.google.step_count.delta",
            "dataSourceId": dataSourceId
        }],
        "bucketByTime": {"durationMillis": backend.ONE_DAY_MS},
        "startTimeMillis": startTimeMillis,
        "endTimeMillis": endTimeMillis
    }).execute()


def get_and_store_fit_data(http_auth, cur, username, past_n_days=30):
    fit_service = build('fitness', 'v1', http=http_auth)
    n_days_ago_millis = backend.calc_n_days_ago(past_n_days)
    #  print(help(fit_service.users().dataset().aggregate))
    steps = []
    activity = []
    now_millis = backend.current_milli_time()
    print('get and store fitness data for user {}'.format(username))
    try:
        stepsData = get_aggregate(fit_service, n_days_ago_millis, now_millis, backend.STEPS_DATASOURCE)
        for day in stepsData['bucket']:
            # store local date in the database
            d = datetime.fromtimestamp(int(day['startTimeMillis']) / 1000,
                                       tz=pytz.timezone(backend.LOCAL_TIMEZONE)).strftime(backend.DATE_FORMAT)
            if day['dataset'][0]['point']:
                s = day['dataset'][0]['point'][0]['value'][0]['intVal']
                steps.append([d, s])
            else:
                steps.append([d, 0])
        print("Steps:", steps)
    except Exception as e:
        print(e)
        print("No steps found")
    try:
        activityData = get_aggregate(fit_service, n_days_ago_millis, now_millis, backend.ACTIVITY_DATASOURCE)
        for day in activityData['bucket']:
            # store local date in the database
            d = datetime.fromtimestamp(int(day['startTimeMillis']) / 1000,
                                       tz=pytz.timezone(backend.LOCAL_TIMEZONE)).strftime(backend.DATE_FORMAT)
            if day['dataset'][0]['point']:
                for a in day['dataset'][0]['point']:
                    activity_type = a['value'][0]['intVal']
                    length_ms = a['value'][1]['intVal']
                    n_segments = a['value'][2]['intVal']
                    activity.append([d, activity_type, length_ms, n_segments])
            else:
                activity.append([d, 4, 0, 0])
        print("Activity:", activity)
    except Exception as e:
        print(e)
        print("No activity found")
    try:
        rows = cur.executemany("REPLACE INTO steps SET username=%s, day=%s, steps=%s".format(username),
                               [[username] + s for s in steps])
        print("steps: {} rows affected".format(rows))
        rows = cur.executemany(
            "REPLACE INTO activity SET username=%s, day=%s, activity_type=%s, length_ms=%s, n_segments=%s",
            [[username] + a for a in activity])
        print("activity: {} rows affected".format(rows))
    except Exception as e:
        print(e)
    return steps, activity


if __name__ == "__main__":
    with open(backend.client_secret_file) as f:
        client_secret_json = json.load(f)
        client_id = client_secret_json['web']['client_id']
        client_secret = client_secret_json['web']['client_secret']

    db = MySQLdb.connect(host=backend.config.get('database_config', 'dbhost'),
                         port=int(backend.config.get('database_config', 'dbport')),
                         user=backend.config.get('database_config', 'dbuser'),
                         passwd=backend.config.get('database_config', 'dbpass'),
                         db=backend.config.get('database_config', 'dbname'),
                         cursorclass=MySQLdb.cursors.DictCursor)
    cur = db.cursor()
    n_rows = cur.execute("SELECT * FROM google_fit")
    rows = cur.fetchall()
    for r in rows:
        username = r['username']
        refresh_token = r['refresh_token']
        creds = client.GoogleCredentials("", client_id, client_secret, refresh_token, 0,
                                         "https://accounts.google.com/o/oauth2/token", "Python")
        http_auth = creds.authorize(httplib2.Http())
        try:
            steps, activity = get_and_store_fit_data(http_auth, cur, username)
        except Exception as e:
            print("Unable to get fit data for {}! {}".format(username, e))
        db.commit()
    cur.close()
    # disconnect from server
    db.close()
