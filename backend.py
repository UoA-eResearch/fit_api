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
CALORIES_DATASOURCE = 'derived:com.google.calories.expended:com.google.android.gms:merge_calories_expended'
ACTIVITY_DATASOURCE = "derived:com.google.activity.segment:com.google.android.gms:merge_activity_segments"
HEART_RATE_DATASOURCE = 'derived:com.google.heart_rate.bpm:com.google.android.gms:merge_heart_rate_bpm'
epoch0 = datetime(1970, 1, 1, tzinfo=pytz.utc)

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
DEFAULT_TIMEZONE = config.get('app_config', 'default_timezone')
DEFAULT_BUCKET = config.get('app_config', 'bucket_name')
DATASTORE_KIND = config.get('app_config', 'datastore_kind')
GCP_project = config.get('app_config', 'project')
GCP_dataset = config.get('bigquery_config', 'dataset')
GCP_table_heartrate = config.get('bigquery_config', 'table_heartrate')
GCP_table_activities = config.get('bigquery_config', 'table_activities')
GCP_table_segments = config.get('bigquery_config', 'table_segments')
GCP_table_steps = config.get('bigquery_config', 'table_steps')
GCP_table_calories = config.get('bigquery_config', 'table_calories')


def current_milli_time():
    return int(round(time.time() * 1000))


def list_datasources(http_auth):
    fit_service = build('fitness', 'v1', http=http_auth)
    return fit_service.users().dataSources().list(userId="me").execute()


def get_daily_calories(http_auth, start_year, start_month, start_day, end_time_millis, local_timezone=DEFAULT_TIMEZONE):
    """
    Get user's daily calory related data
    :param http_auth: username authenticated HTTP client to call Google API
    :param start_year: start getting calory data from local date's year
    :param start_month: start getting calory data from local date's month
    :param start_day: start getting calory data from local date's day
    :param end_time_millis: getting calory data up to the end datetime in milliseconds Unix Epoch time
    :param local_timezone: timezone such as US/Pacific, one of the pytz.all_timezones
    :return: dict of daily calory and data source ID
    """
    # calculate the timestamp in local time to query Google fitness API
    local_0_hour = pytz.timezone(local_timezone).localize(datetime(start_year, start_month, start_day))
    start_time_millis = int((local_0_hour - epoch0).total_seconds() * 1000)
    fit_service = build('fitness', 'v1', http=http_auth)
    daily_calories = {}

    calory_data = get_aggregate(fit_service, start_time_millis, end_time_millis, CALORIES_DATASOURCE)
    for daily_calory_data in calory_data['bucket']:
        # use local date as the key
        local_date = datetime.fromtimestamp(int(daily_calory_data['startTimeMillis']) / 1000,
                                            tz=pytz.timezone(local_timezone))
        local_date_str = local_date.strftime(DATE_FORMAT)

        data_point = daily_calory_data['dataset'][0]['point']
        if data_point:
            calories = data_point[0]['value'][0]['fpVal']
            data_source_id = data_point[0]['originDataSourceId']
            daily_calories[local_date_str] = {'calories': calories, 'originDataSourceId': data_source_id}

    return daily_calories


def get_daily_steps(http_auth, start_year, start_month, start_day, end_time_millis, local_timezone=DEFAULT_TIMEZONE):
    """
    Get user's daily step related data
    :param http_auth: username authenticated HTTP client to call Google API
    :param start_year: start getting step data from local date's year
    :param start_month: start getting step data from local date's month
    :param start_day: start getting step data from local date's day
    :param end_time_millis: getting step data up to the end datetime in milliseconds Unix Epoch time
    :param local_timezone: timezone such as US/Pacific, one of the pytz.all_timezones
    :return: dict of daily steps and data source ID
    """
    # calculate the timestamp in local time to query Google fitness API
    local_0_hour = pytz.timezone(local_timezone).localize(datetime(start_year, start_month, start_day))
    start_time_millis = int((local_0_hour - epoch0).total_seconds() * 1000)
    fit_service = build('fitness', 'v1', http=http_auth)
    steps = {}

    steps_data = get_aggregate(fit_service, start_time_millis, end_time_millis, STEPS_DATASOURCE)
    for daily_step_data in steps_data['bucket']:
        # use local date as the key
        local_date = datetime.fromtimestamp(int(daily_step_data['startTimeMillis']) / 1000,
                                            tz=pytz.timezone(local_timezone))
        local_date_str = local_date.strftime(DATE_FORMAT)

        data_point = daily_step_data['dataset'][0]['point']
        if data_point:
            count = data_point[0]['value'][0]['intVal']
            data_source_id = data_point[0]['originDataSourceId']
            steps[local_date_str] = {'steps': count, 'originDataSourceId': data_source_id}

    return steps


def get_daily_activities(http_auth, start_year, start_month, start_day, end_time_millis,
                         local_timezone=DEFAULT_TIMEZONE):
    """
    get user's activities from Google fitness API
    :param http_auth: username authenticated HTTP client to call Google API
    :param start_year: start getting activity data from local date's year
    :param start_month: start getting activity data from local date's month
    :param start_day: start getting activity data from local date's day
    :param end_time_millis: getting activity data up to the end datetime in milliseconds Unix Epoch time
    :param local_timezone: timezone such as US/Pacific, one of the pytz.all_timezones
    :return: dict of daily activities and its data sets
    """
    # calculate the timestamp in local time to query Google fitness API
    local_0_hour = pytz.timezone(local_timezone).localize(datetime(start_year, start_month, start_day))
    start_time_millis = int((local_0_hour - epoch0).total_seconds() * 1000)
    fit_service = build('fitness', 'v1', http=http_auth)
    activities = {}

    activityData = get_aggregate(fit_service, start_time_millis, end_time_millis, ACTIVITY_DATASOURCE)
    for daily_activity in activityData['bucket']:
        # use local date as the key
        local_date = datetime.fromtimestamp(int(daily_activity['startTimeMillis']) / 1000,
                                            tz=pytz.timezone(local_timezone))
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


def calc_n_days_ago(past_n_days, local_timezone=pytz.timezone(DEFAULT_TIMEZONE)):
    """
    calculate the 0 hour datetime n days ago in milliseconds Unix Epoch time
    :param past_n_days: calculated with timedelta
    :param local_timezone: timezone such as US/Pacific, one of the pytz.all_timezones
    :return: milliseconds Unix Epoch time n days ago
    """
    now_utc = datetime.now(pytz.timezone('UTC'))
    now_local = now_utc.astimezone(local_timezone)
    n_days_ago_local = now_local - timedelta(days=past_n_days)
    n_days_ago_local_0_hour = local_timezone.localize(
        datetime(n_days_ago_local.year, n_days_ago_local.month, n_days_ago_local.day))
    n_days_ago_local_0_hour_millis = (n_days_ago_local_0_hour - datetime(1970, 1, 1,
                                                                         tzinfo=pytz.utc)).total_seconds() * 1000
    return int(n_days_ago_local_0_hour_millis)


def get_and_insert_heart_rate(http_auth, username, start_year, start_month, start_day, end_time_millis,
                              local_timezone=DEFAULT_TIMEZONE):
    """
    call Google Fitness API for user's heart rate bmp numbers and
    insert them to a BigQuery table except existing_rows of recordedTimeNanos
    :param http_auth: username authenticated HTTP client to call Google API
    :param username: user's Gmail
    :param start_year: start getting heart rate data from local date's year
    :param start_month: start getting heart rate data from local date's month
    :param start_day: start getting heart rate data from local date's day
    :param end_time_millis: getting heart rate data up to the end datetime in milliseconds Unix Epoch time
    :param local_timezone: timezone such as US/Pacific, one of the pytz.all_timezones
    :return: heart rate insert log, data set, no heart rate dates, count of inserted rows
    """
    # calculate the timestamp in local time to query Google fitness API
    local_0_hour = pytz.timezone(local_timezone).localize(datetime(start_year, start_month, start_day))
    start_time_millis = int((local_0_hour - epoch0).total_seconds() * 1000)
    fit_service = build('fitness', 'v1', http=http_auth)

    # method return values
    no_heart_rate_log = 'no heart rate data in the following days: ['
    heart_rate_log = '['
    heart_dataset_list = []

    heartrate_data = get_aggregate(fit_service, start_time_millis, end_time_millis, HEART_RATE_DATASOURCE)
    bigquery_client = bigquery.Client()
    inserted_count = 0
    rows_to_insert = []
    query = "SELECT recordedTimeNanos FROM `{}.{}.{}` WHERE username = '{}' AND recordedLocalDate >= '{}'".format(
        GCP_project, GCP_dataset, GCP_table_heartrate, username, "{}-{}-{}".format(start_year, start_month, start_day))
    query_job = bigquery_client.query(query)
    existing_rows = list(query_job.result())
    existing_rows = [row['recordedTimeNanos'] for row in existing_rows]

    for daily_item in heartrate_data['bucket']:
        incoming_day_localized = datetime.fromtimestamp(int(daily_item['startTimeMillis']) / 1000,
                                                        tz=pytz.timezone(local_timezone))
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

        # insert heart rate daily entries to BigQuery tables except existing rows
        if heart_dataset['point']:
            data_point_list = heart_dataset['point']
            for bpm_data_point in data_point_list:
                if int(bpm_data_point['endTimeNanos']) not in existing_rows:
                    # username, recordedTimeNanos, recordedLocalDate, bpm
                    rows_to_insert.append(
                        (username, int(bpm_data_point['endTimeNanos']), incoming_day_localized_str,
                         int(bpm_data_point['value'][0]['fpVal'])))

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


def insert_steps(username, steps, local_timezone=DEFAULT_TIMEZONE):
    """
    insert step counts to BigQuery except local date of today's steps per local_timezone
    :param username: user's Gmail
    :param steps: dictionary of local date as key, value is another dict of steps, originDataSourceId
    :param local_timezone: timezone such as US/Pacific, one of the pytz.all_timezones
    :return: inserted row count
    """
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
    now_local = now_utc.astimezone(pytz.timezone(local_timezone))

    for localDate, value in steps.iteritems():
        incoming_steps_date = datetime.strptime(localDate, DATE_FORMAT).date()

        # Do not insert today's steps because error occurs updating or deleting them
        if incoming_steps_date == now_local.date():
            continue

        # if incoming step's date not found in the existing table, insert incoming step count
        if incoming_steps_date not in existing_step_dates:
            rows_to_insert.append(
                (username, localDate, value['steps'], value['originDataSourceId'])
            )

    if rows_to_insert:
        # BigQuery API request
        errors = bigquery_client.insert_rows(table_steps, rows_to_insert)
        if errors:
            raise Exception(str(errors))

    return len(rows_to_insert)


def insert_calories(username, calories, local_timezone=DEFAULT_TIMEZONE):
    """
    insert calories to BigQuery except local date of today's calories per local_timezone
    :param username: user's Gmail
    :param calories: dictionary of local date as key, value is another dict of calories, originDataSourceId
    :param local_timezone: timezone such as US/Pacific, one of the pytz.all_timezones
    :return: inserted row count
    """
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(GCP_dataset)
    table_calories_ref = dataset_ref.table(GCP_table_calories)
    table_calories = bigquery_client.get_table(table_calories_ref)

    # check existing rows by local date
    query = "SELECT DISTINCT recordedLocalDate FROM `{}.{}.{}` WHERE username = '{}' ORDER BY recordedLocalDate DESC ".format(
        GCP_project, GCP_dataset, GCP_table_calories, username)
    query_job = bigquery_client.query(query)
    existing_calories_dates = [row['recordedLocalDate'] for row in query_job.result()]
    rows_to_insert = []
    now_utc = datetime.now(pytz.timezone('UTC'))
    now_local = now_utc.astimezone(pytz.timezone(local_timezone))

    for localDate, value in calories.iteritems():
        incoming_calories_date = datetime.strptime(localDate, DATE_FORMAT).date()

        # Do not insert today's calories because error occurs updating or deleting them
        if incoming_calories_date == now_local.date():
            continue

        # if incoming calory's date not found in the existing table, insert incoming calories
        if incoming_calories_date not in existing_calories_dates:
            rows_to_insert.append(
                (username, localDate, value['calories'], value['originDataSourceId'])
            )

    if rows_to_insert:
        # BigQuery API request
        errors = bigquery_client.insert_rows(table_calories, rows_to_insert)
        if errors:
            raise Exception(str(errors))

    return len(rows_to_insert)


def insert_activities(username, activities, local_timezone=DEFAULT_TIMEZONE):
    """
    insert activities to BigQuery except local date of today's activities per local_timezone
    :param username: user's Gmail
    :param activities: return from get_activities
    :param local_timezone: timezone such as US/Pacific, one of the pytz.all_timezones
    :return: inserted counts for 2 tables
    """
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
    now_local = now_utc.astimezone(pytz.timezone(local_timezone))

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


class UserDataFlow:
    def __init__(self, username, http_auth, start_year, start_month, start_day, end_time_millis, local_timezone):
        self.username = username
        self.http_auth = http_auth
        self.start_year = start_year
        self.start_month = start_month
        self.start_day = start_day
        self.end_time_millis = end_time_millis
        self.local_timezone = local_timezone

    def get_steps(self):
        self.steps = get_daily_steps(self.http_auth, self.start_year, self.start_month, self.start_day,
                                     self.end_time_millis, self.local_timezone)
        return self.steps

    def post_steps(self):
        if self.steps is not None:
            self.insert_steps_result = insert_steps(self.username, self.steps, self.local_timezone)
            return self.insert_steps_result
        else:
            raise RuntimeError('no self.steps to insert to BigQuery')

    def get_calories(self):
        self.calories = get_daily_calories(self.http_auth, self.start_year, self.start_month, self.start_day,
                                     self.end_time_millis, self.local_timezone)
        return self.calories

    def post_calories(self):
        if self.calories is not None:
            self.insert_calories_result = insert_calories(self.username, self.calories, self.local_timezone)
            return self.insert_calories_result
        else:
            raise RuntimeError('no self.calories to insert to BigQuery')

    def get_and_post_heart_rate(self):
        self.insert_heart_rate_result = get_and_insert_heart_rate(self.http_auth, self.username, self.start_year,
                                                                  self.start_month, self.start_day,
                                                                  self.end_time_millis, self.local_timezone)
        return self.insert_heart_rate_result

    def get_activities(self):
        self.activities = get_daily_activities(self.http_auth, self.start_year, self.start_month, self.start_day,
                                               self.end_time_millis, self.local_timezone)
        return self.activities

    def post_activities(self):
        if self.activities is not None:
            self.insert_activities_result = insert_activities(self.username, self.activities, self.local_timezone)
            return self.insert_activities_result
        else:
            raise RuntimeError('no self.activities to insert to BigQuery')
