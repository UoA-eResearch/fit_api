import os
import time
from datetime import datetime, timedelta

import pytz
from configparser import ConfigParser
from google.cloud import bigquery
from googleapiclient.discovery import build

from update_google_fit import get_aggregate

DATE_FORMAT = '%Y-%m-%d'
ONE_DAY_MS = 86400000
STEPS_DATASOURCE = "derived:com.google.step_count.delta:com.google.android.gms:estimated_steps"
ACTIVITY_DATASOURCE = "derived:com.google.activity.segment:com.google.android.gms:merge_activity_segments"
HEART_RATE_DATASOURCE = 'derived:com.google.heart_rate.bpm:com.google.android.gms:merge_heart_rate_bpm'
epoch0 = datetime(1970, 1, 1, tzinfo=pytz.utc)
LOCAL_TIMEZONE = 'US/Pacific'

# init environment variables and configurations
if 'CLIENT_SECRET' in os.environ:
    client_secret_file = os.environ['CLIENT_SECRET']
else:
    client_secret_file = 'client_secret.json'
if 'APP_CONFIG' in os.environ:
    APP_CONFIG_FILENAME = os.environ['APP_CONFIG']
else:
    APP_CONFIG_FILENAME = 'app.config'

config = ConfigParser()
config.read(APP_CONFIG_FILENAME)
API_key = config.get('app_config', 'API_KEY')

GCP_project = config.get('app_config', 'project')
GCP_dataset = config.get('bigquery_config', 'dataset')
GCP_table_heartrate = config.get('bigquery_config', 'table_heartrate')
GCP_table_activities = config.get('bigquery_config', 'table_activities')
GCP_table_segments = config.get('bigquery_config', 'table_segments')
GCP_table_steps = config.get('bigquery_config', 'table_steps')


def current_milli_time():
    return int(round(time.time() * 1000))


def get_daily_steps(http_auth, start_year, start_month, start_day, end_time_millis=current_milli_time()):
    # calculate the timestamp in local time to query Google fitness API
    local_0_hour = pytz.timezone(LOCAL_TIMEZONE).localize(datetime(start_year, start_month, start_day))
    start_time_millis = int((local_0_hour - epoch0).total_seconds() * 1000)
    fit_service = build('fitness', 'v1', http=http_auth)
    steps = {}

    steps_data = get_aggregate(fit_service, start_time_millis, end_time_millis, STEPS_DATASOURCE)
    for daily_step_data in steps_data['bucket']:
        # use local date as the key
        local_date = datetime.fromtimestamp(int(daily_step_data['startTimeMillis']) / 1000,
                                            tz=pytz.timezone(LOCAL_TIMEZONE))
        local_date_str = local_date.strftime(DATE_FORMAT)

        data_point = daily_step_data['dataset'][0]['point']
        if data_point:
            count = data_point[0]['value'][0]['intVal']
            data_source_id = data_point[0]['originDataSourceId']
            steps[local_date_str] = {'steps': count, 'originDataSourceId': data_source_id}

    return steps


def get_activities(http_auth, start_year, start_month, start_day, end_time_millis=current_milli_time()):
    # calculate the timestamp in local time to query Google fitness API
    local_0_hour = pytz.timezone(LOCAL_TIMEZONE).localize(datetime(start_year, start_month, start_day))
    start_time_millis = int((local_0_hour - epoch0).total_seconds() * 1000)
    fit_service = build('fitness', 'v1', http=http_auth)
    activities = {}

    activityData = get_aggregate(fit_service, start_time_millis, end_time_millis, ACTIVITY_DATASOURCE)
    for daily_activity in activityData['bucket']:
        # use local date as the key
        local_date = datetime.fromtimestamp(int(daily_activity['startTimeMillis']) / 1000,
                                            tz=pytz.timezone(LOCAL_TIMEZONE))
        local_date_str = local_date.strftime(DATE_FORMAT)
        if local_date_str not in activities:
            activities[local_date_str] = {
                'daily_activities': [],
                'activity_dataset': None
            }

        activity_data_point = daily_activity['dataset'][0]['point']
        if activity_data_point:
            for activity in activity_data_point:
                activity_type = activity['value'][0]['intVal']
                length_ms = activity['value'][1]['intVal']
                n_segments = activity['value'][2]['intVal']

                # add daily activities
                activities[local_date_str]['daily_activities'].append({
                    'activity_type': activity_type,
                    'seconds': round(length_ms / 1000),
                    'segments': n_segments,
                })

        # get activity datasets
        start_time_nanos = int((local_date - epoch0).total_seconds() * 1000 * 1000 * 1000)
        end_time_nanos = start_time_nanos + 86400000000000
        activity_datasetId = '{}-{}'.format(start_time_nanos, end_time_nanos)
        print('calling Google Fitness API to get activity segment from dataSetId {}'.format(
            activity_datasetId))
        activity_dataset = fit_service.users().dataSources().datasets().get(userId="me",
                                                                            dataSourceId=ACTIVITY_DATASOURCE,
                                                                            datasetId=activity_datasetId).execute()
        activities[local_date_str]['activity_dataset'] = activity_dataset

    return activities


# calculate the 0 hour datetime n days ago
def calc_n_days_ago(past_n_days, local_timezone=pytz.timezone(LOCAL_TIMEZONE)):
    now_utc = datetime.now(pytz.timezone('UTC'))
    now_local = now_utc.astimezone(local_timezone)
    n_days_ago_local = now_local - timedelta(days=past_n_days)
    n_days_ago_local_0_hour = local_timezone.localize(
        datetime(n_days_ago_local.year, n_days_ago_local.month, n_days_ago_local.day))
    n_days_ago_local_0_hour_millis = (n_days_ago_local_0_hour - datetime(1970, 1, 1,
                                                                         tzinfo=pytz.utc)).total_seconds() * 1000
    return int(n_days_ago_local_0_hour_millis)


# insert heart rate bmp rows except existing_rows of recordedTimeNanos
def get_and_insert_heart_rate(http_auth, username, start_year, start_month, start_day,
                              end_time_millis=current_milli_time()):
    # calculate the timestamp in local time to query Google fitness API
    local_0_hour = pytz.timezone(LOCAL_TIMEZONE).localize(datetime(start_year, start_month, start_day))
    start_time_millis = int((local_0_hour - epoch0).total_seconds() * 1000)
    now_millis = current_milli_time()
    # today's local date
    local_date_today = datetime.fromtimestamp(now_millis / 1000, tz=pytz.timezone(LOCAL_TIMEZONE)).strftime(DATE_FORMAT)
    fit_service = build('fitness', 'v1', http=http_auth)

    # method return values
    no_heart_rate_log = 'no heart rate data in the following days: ['
    heart_rate_log = '['
    heart_dataset_list = []

    heartrate_data = get_aggregate(fit_service, start_time_millis, end_time_millis, HEART_RATE_DATASOURCE)
    bigquery_client = bigquery.Client()
    inserted_count = 0
    rows_to_insert = []
    query = "SELECT recordedLocalDate, COUNT(bpm) AS bpm_count FROM `{}.{}.{}` WHERE username = '{}' GROUP BY recordedLocalDate".format(
        GCP_project, GCP_dataset, GCP_table_heartrate, username)
    query_job = bigquery_client.query(query)
    local_date_rows = list(query_job.result())

    for daily_item in heartrate_data['bucket']:
        incoming_day_localized = datetime.fromtimestamp(int(daily_item['startTimeMillis']) / 1000,
                                                        tz=pytz.timezone(LOCAL_TIMEZONE))
        incoming_day_localized_str = incoming_day_localized.strftime(DATE_FORMAT)
        data_point = daily_item['dataset'][0]['point']
        if data_point:
            startTimeNanos = data_point[0]['startTimeNanos']
            endTimeNanos = data_point[0]['endTimeNanos']
            heart_datasetId = '{}-{}'.format(startTimeNanos, endTimeNanos)
            heart_rate_log += '"on day {}, heart rate datasetId: {}", '.format(
                incoming_day_localized_str, heart_datasetId)
            print('calling Google Fitness API to get heart rate from dataSetId {} for user {}'.format(
                heart_datasetId, username))
            heart_dataset = fit_service.users().dataSources().datasets().get(userId="me",
                                                                             dataSourceId=HEART_RATE_DATASOURCE,
                                                                             datasetId=heart_datasetId).execute()
            heart_dataset_list.append(heart_dataset)
        else:
            no_heart_rate_log += '"{}", '.format(incoming_day_localized_str)
            continue

        # insert heart rate daily entries to BigQuery tables
        if incoming_day_localized_str != local_date_today:
            # if any of the daily entries exists on a specific date, assume all entries are in the table and skip
            if [row for row in local_date_rows if row['recordedLocalDate'] == incoming_day_localized.date()]:
                continue
            else:
                # the daily records don't exist in the table, insert them from Google Fitness API
                rows_to_insert.extend(prep_insert_rows(incoming_day_localized_str, heart_dataset, username))

        else:
            # if the date is the local datetime's current date, only insert the nonexistent rows
            query = "SELECT recordedTimeNanos from `{}.{}.{}` WHERE recordedLocalDate = '{}' AND username = '{}' ".format(
                GCP_project, GCP_dataset, GCP_table_heartrate, incoming_day_localized_str, username)
            query_job = bigquery_client.query(query)
            existing_rows = list(query_job.result())
            existing_rows_endTimeNanos = [row['recordedTimeNanos'] for row in existing_rows]
            rows_to_insert.extend(
                prep_insert_rows(incoming_day_localized_str, heart_dataset, username, existing_rows_endTimeNanos))

    if rows_to_insert:
        dataset_ref = bigquery_client.dataset(GCP_dataset)
        table_ref = dataset_ref.table(GCP_table_heartrate)
        table = bigquery_client.get_table(table_ref)
        # BigQuery API request
        errors = bigquery_client.insert_rows(table, rows_to_insert)
        if errors:
            raise Exception(str(errors))
        inserted_count = len(rows_to_insert)
    return {
        'heart_rate_log': heart_rate_log + ']',
        'no_heart_rate_log': no_heart_rate_log + ']',
        'heart_datasets': heart_dataset_list,
        'inserted_count': inserted_count,
    }


def insert_steps(username, steps):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(GCP_dataset)
    table_steps_ref = dataset_ref.table(GCP_table_steps)
    table_steps = bigquery_client.get_table(table_steps_ref)

    # check existing rows by local date
    query = "SELECT DISTINCT recordedLocalDate FROM `{}.{}.{}` WHERE username = '{}' ORDER BY recordedLocalDate DESC ".format(
        GCP_project, GCP_dataset, GCP_table_steps, username)
    query_job = bigquery_client.query(query)
    existing_step_dates = [row['recordedLocalDate'] for row in query_job.result()]
    rows_to_insert = []
    now_utc = datetime.now(pytz.timezone('UTC'))
    now_local = now_utc.astimezone(pytz.timezone(LOCAL_TIMEZONE))

    for localDate, value in steps.iteritems():
        incoming_activity_date = datetime.strptime(localDate, DATE_FORMAT).date()

        # Do not insert today's activities because error occurs updating or deleting them
        if incoming_activity_date == now_local.date():
            continue

        # if incoming step's date not found in the existing steps table, insert incoming step count
        if incoming_activity_date not in existing_step_dates:
            rows_to_insert.append(
                (username, localDate, value['steps'], value['originDataSourceId'])
            )

    if rows_to_insert:
        # BigQuery API request
        errors = bigquery_client.insert_rows(table_steps, rows_to_insert)
        if errors:
            raise Exception(str(errors))

    return len(rows_to_insert)


def insert_activities(username, activities):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(GCP_dataset)
    table_activities_ref = dataset_ref.table(GCP_table_activities)
    table_activities = bigquery_client.get_table(table_activities_ref)
    table_segments_ref = dataset_ref.table(GCP_table_segments)
    table_segments = bigquery_client.get_table(table_segments_ref)

    # check existing rows by local date in the table
    query = "SELECT recordedLocalDate, COUNT(activity_type) AS activity_type_count FROM `{}.{}.{}` WHERE username = '{}' GROUP BY recordedLocalDate ORDER BY recordedLocalDate DESC ".format(
        GCP_project, GCP_dataset, GCP_table_activities, username)
    query_job = bigquery_client.query(query)
    existing_activity_dates = [row['recordedLocalDate'] for row in query_job.result()]
    activity_rows_to_insert = []
    segment_rows_to_insert = []
    now_utc = datetime.now(pytz.timezone('UTC'))
    now_local = now_utc.astimezone(pytz.timezone(LOCAL_TIMEZONE))

    for localDate, value in activities.iteritems():
        incoming_activity_date = datetime.strptime(localDate, DATE_FORMAT).date()

        # Do not insert today's activities because error occurs updating or deleting them
        if incoming_activity_date == now_local.date():
            continue

        # if incoming activity's date not found in the existing activities table, insert incoming activities
        if incoming_activity_date not in existing_activity_dates:
            for daily_activity in value['daily_activities']:
                activity_rows_to_insert.append(
                    (username, localDate, daily_activity['activity_type'], daily_activity['seconds'],
                     daily_activity['segments'])
                )
            # insert activity segments
            for point in value['activity_dataset']['point']:
                activity_type = point['value'][0]['intVal']
                segment_rows_to_insert.append(
                    (username, localDate, activity_type, point['startTimeNanos'], point['endTimeNanos'],
                     point['originDataSourceId'])
                )
    if activity_rows_to_insert:
        # BigQuery API request
        errors = bigquery_client.insert_rows(table_activities, activity_rows_to_insert)
        if errors:
            raise Exception(str(errors))
    if segment_rows_to_insert:
        # BigQuery API request
        errors = bigquery_client.insert_rows(table_segments, segment_rows_to_insert)
        if errors:
            raise Exception(str(errors))

    return {'inserted_activity_count': len(activity_rows_to_insert),
            'inserted_segment_count': len(segment_rows_to_insert)}


def prep_insert_rows(day, heart_dataset, username, existing_rows=[]):
    rows_to_insert = []

    if heart_dataset['point']:
        data_point_list = heart_dataset['point']
        for bpm_data_point in data_point_list:
            if int(bpm_data_point['endTimeNanos']) not in existing_rows:
                # SELECT  username, recordedTimeNanos, recordedLocalDate, bpm FROM `next19fit.fitness.heartrate` LIMIT 1000
                rows_to_insert.append(
                    (username, int(bpm_data_point['endTimeNanos']), day, int(bpm_data_point['value'][0]['fpVal'])))

    return rows_to_insert
