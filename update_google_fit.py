#!/usr/bin/env python
import json
import os
from datetime import datetime, timedelta

import MySQLdb.cursors
import httplib2
import pytz
from google.cloud import bigquery
from googleapiclient.discovery import build
from oauth2client import client

import config

ONE_DAY_MS = 86400000
STEPS_DATASOURCE = "derived:com.google.step_count.delta:com.google.android.gms:estimated_steps"
ACTIVITY_DATASOURCE = "derived:com.google.activity.segment:com.google.android.gms:merge_activity_segments"
HEART_RATE_DATASOURCE = 'derived:com.google.heart_rate.bpm:com.google.android.gms:merge_heart_rate_bpm'
LOCAL_TIMEZONE = 'US/Pacific'

# init environment variables
if 'CLIENT_SECRET' in os.environ:
    client_secret_file = os.environ['CLIENT_SECRET']
else:
    client_secret_file = 'client_secret.json'


def get_aggregate(fit_service, startTimeMillis, endTimeMillis, dataSourceId):
    return fit_service.users().dataset().aggregate(userId="me", body={
        "aggregateBy": [{
            "dataTypeName": "com.google.step_count.delta",
            "dataSourceId": dataSourceId
        }],
        "bucketByTime": {"durationMillis": ONE_DAY_MS},
        "startTimeMillis": startTimeMillis,
        "endTimeMillis": endTimeMillis
    }).execute()


def get_and_store_fit_data(http_auth, cur, username, past_n_days=30):
    fit_service = build('fitness', 'v1', http=http_auth)
    n_days_ago_millis, now_millis = calc_n_days_ago(past_n_days)
    #  print(help(fit_service.users().dataset().aggregate))
    steps = []
    activity = []
    print(username)
    try:
        stepsData = get_aggregate(fit_service, n_days_ago_millis, now_millis, STEPS_DATASOURCE)
        for day in stepsData['bucket']:
            # store local date in the database
            d = datetime.fromtimestamp(int(day['startTimeMillis']) / 1000, tz=pytz.timezone(LOCAL_TIMEZONE)).strftime(
                '%Y-%m-%d')
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
        activityData = get_aggregate(fit_service, n_days_ago_millis, now_millis, ACTIVITY_DATASOURCE)
        for day in activityData['bucket']:
            # store local date in the database
            d = datetime.fromtimestamp(int(day['startTimeMillis']) / 1000, tz=pytz.timezone(LOCAL_TIMEZONE)).strftime(
                '%Y-%m-%d')
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


# calculate the 0 hour datetime n days ago
def calc_n_days_ago(past_n_days, local_timezone=pytz.timezone(LOCAL_TIMEZONE)):
    now_utc = datetime.now(pytz.timezone('UTC'))
    now_utc_millis = (now_utc - datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds() * 1000
    now_local = now_utc.astimezone(local_timezone)
    n_days_ago_local = now_local - timedelta(days=past_n_days)
    n_days_ago_local_0_hour = local_timezone.localize(
        datetime(n_days_ago_local.year, n_days_ago_local.month, n_days_ago_local.day))
    n_days_ago_local_0_hour_millis = (n_days_ago_local_0_hour - datetime(1970, 1, 1,
                                                                         tzinfo=pytz.utc)).total_seconds() * 1000
    return int(n_days_ago_local_0_hour_millis), int(now_utc_millis)


def get_and_load_heart_rate(http_auth, username, past_n_days=30):
    fit_service = build('fitness', 'v1', http=http_auth)

    # calculate number of days in the past in local time to query Google fitness API
    n_days_ago_millis, now_millis = calc_n_days_ago(past_n_days)

    # method return values
    no_heart_rate_log = 'no heart rate data in the following days: ['
    heart_rate_log = '['
    heart_dataset_list = []

    print('calling Google fitness API to get heart rate data for user: ' + username)
    stepsData = get_aggregate(fit_service, n_days_ago_millis, now_millis, HEART_RATE_DATASOURCE)
    for daily_item in stepsData['bucket']:
        # store local date like '2019-01-22' in the database
        day = datetime.fromtimestamp(int(daily_item['startTimeMillis']) / 1000,
                                     tz=pytz.timezone(LOCAL_TIMEZONE)).strftime('%Y-%m-%d')
        if daily_item['dataset'][0]['point']:
            startTimeNanos = daily_item['dataset'][0]['point'][0]['startTimeNanos']
            endTimeNanos = daily_item['dataset'][0]['point'][0]['endTimeNanos']
            heart_datasetId = '{}-{}'.format(startTimeNanos, endTimeNanos)
            heart_rate_log += '"on day {}, heart rate datasetId: {}", '.format(day, heart_datasetId)
            heart_dataset = fit_service.users().dataSources().datasets().get(userId="me",
                                                                             dataSourceId=HEART_RATE_DATASOURCE,
                                                                             datasetId=heart_datasetId).execute()
            heart_dataset_list.append(heart_dataset)
        else:
            no_heart_rate_log += '"{}", '.format(day)
            continue

        # insert heart rate daily entries to BigQuery tables
        # if any of the daily entries exists on a specific date, assume all entries are in the table and skip
        # if the date is the local datetime's current date, only insert the nonexistent rows
        bigquery_client = bigquery.Client()
        inserted_count = 0
        query = "SELECT recordedTimeNanos from `{}.{}.{}` WHERE recordedLocalDate = '{}'".format(
            config.project, config.dataset, config.table, day)
        query_job = bigquery_client.query(query)
        existing_rows = list(query_job.result())

        # today's local date
        local_date_today = datetime.fromtimestamp(now_millis / 1000, tz=pytz.timezone(LOCAL_TIMEZONE)).strftime(
            '%Y-%m-%d')
        if day != local_date_today:
            if len(existing_rows) >= 1:
                continue
            else:
                # the daily records don't exist, insert them
                inserted_count += insert_to_bq(bigquery_client, config.dataset, config.table, day, heart_dataset,
                                               username)

        else:
            # date is today
            existing_rows_endTimeNanos = [row['recordedTimeNanos'] for row in existing_rows]
            inserted_count += insert_to_bq(bigquery_client, config.dataset, config.table, day, heart_dataset, username,
                                           existing_rows_endTimeNanos)

    return {
        'heart_rate_log': heart_rate_log + ']',
        'no_heart_rate_log': no_heart_rate_log + ']',
        'heart_datasets': heart_dataset_list,
        'inserted_count': inserted_count,
    }


# insert heart rate bmp rows except existing_rows of recordedTimeNanos
def insert_to_bq(bigquery_client, dataset, table, day, heart_dataset, username, existing_rows=[]):
    dataset_ref = bigquery_client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    table = bigquery_client.get_table(table_ref)

    to_insert_count = 0
    rows_to_insert = []

    if heart_dataset['point']:
        data_point_list = heart_dataset['point']
        for bpm_data_point in data_point_list:
            if int(bpm_data_point['endTimeNanos']) not in existing_rows:
                # SELECT  username, recordedTimeNanos, recordedLocalDate, bpm FROM `next19fit.fitness.heartrate` LIMIT 1000
                rows_to_insert.append(
                    (username, int(bpm_data_point['endTimeNanos']), day, int(bpm_data_point['value'][0]['fpVal'])))
                to_insert_count += 1
        if rows_to_insert:
            # BigQuery API request
            errors = bigquery_client.insert_rows(table, rows_to_insert)
            assert errors == []
        else:
            print("Warning! Nothing to insert to BigQuery; All data points already exist in BigQuery: {}".format(
                str(existing_rows)))
    else:
        print("Warning! Nothing to insert to BigQuery; no data points found in fitness dataset: {}".format(
            json.dumps(heart_dataset)))

    return to_insert_count


if __name__ == "__main__":
    with open(client_secret_file) as f:
        client_secret_json = json.load(f)
        client_id = client_secret_json['web']['client_id']
        client_secret = client_secret_json['web']['client_secret']

    db = MySQLdb.connect(host=config.dbhost, port=config.dbport, user=config.dbuser, passwd=config.dbpass,
                         db=config.dbname, cursorclass=MySQLdb.cursors.DictCursor)
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
