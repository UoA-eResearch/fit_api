#!/usr/bin/env python
import json
from collections import OrderedDict

import bottle_mysql
import googleapiclient.errors
import httplib2
import pytz
from threading import Thread
from bottle import *
from google.cloud import datastore
from google.cloud import error_reporting
from google.cloud import storage
from googleapiclient.discovery import build
from oauth2client import client

import backend
from update_google_fit import get_and_store_fit_data

bad_activities = "(0,3,5,109,110,111,112,117,118)"

# bottle web framework init
app = Bottle()
application = app
# dbhost is optional, default is localhost
plugin = bottle_mysql.Plugin(dbhost=backend.config.get('database_config', 'dbhost'),
                             dbport=int(backend.config.get('database_config', 'dbport')),
                             dbuser=backend.config.get('database_config', 'dbuser'),
                             dbpass=backend.config.get('database_config', 'dbpass'),
                             dbname=backend.config.get('database_config', 'dbname'))
app.install(plugin)

# Google Cloud Stackdriver Debugger https://cloud.google.com/debugger/docs/setup/python
try:
    import googleclouddebugger

    googleclouddebugger.enable()
    print("Google Cloud Debugger enabled")
except ImportError as e:
    print >> sys.stderr, "Failed to load Google Cloud Debugger for Python 2: ".format(e)


def require_key():
    key = request.query.get('key', '')
    if key != backend.API_key:
        abort(httplib.UNAUTHORIZED, "invalid API key")


@app.get('/health')
def health_check(db):
    if db:
        return "database connection alive: " + str(db.connection)
    else:
        return HTTPError(httplib.INTERNAL_SERVER_ERROR, "database connection is in a bad state")


@app.get('/')
def default_get(db):
    name = request.query.get('state', '')
    p = request.urlparts
    redirect_uri = "{}://{}{}".format(p.scheme, p.netloc, p.path)

    flow = client.flow_from_clientsecrets(
        backend.client_secret_file,
        scope=["profile", "email", 'https://www.googleapis.com/auth/fitness.activity.read',
               'https://www.googleapis.com/auth/fitness.body.read'],
        redirect_uri=redirect_uri)
    flow.params['access_type'] = 'offline'
    flow.params['prompt'] = 'consent'
    if 'code' not in request.query:
        require_key()
        if not name:
            return static_file("index.html", ".")
        auth_uri = flow.step1_get_authorize_url(state=name)
        redirect(auth_uri)
    else:
        creds = flow.step2_exchange(code=request.query.code)
        http_auth = creds.authorize(httplib2.Http())
        user_info_service = build('oauth2', 'v2', http=http_auth)
        u = user_info_service.userinfo().get().execute()
        db.execute(
            "REPLACE INTO google_fit SET username=%s, google_id=%s, full_name=%s, gender=%s, image_url=%s, email=%s, refresh_token=%s",
            (name, u['id'], u['name'], u.get('gender'), u['picture'], u['email'], creds.refresh_token))
        print("Inserted", u)
        steps, activity = get_and_store_fit_data(http_auth, db, name)
        response.content_type = 'application/json'
        return json.dumps(dict(steps), sort_keys=True, indent=4)


@app.get('/oauth2callback')
def oauth2callback(db):
    urlparts = request.urlparts
    redirect_uri = "{}://{}{}".format(urlparts.scheme, urlparts.netloc, urlparts.path)
    timezone = request.query.get('state', None)

    flow = client.flow_from_clientsecrets(
        backend.client_secret_file,
        scope=["profile", "email", 'https://www.googleapis.com/auth/fitness.activity.read',
               'https://www.googleapis.com/auth/fitness.body.read'],
        redirect_uri=redirect_uri)
    flow.params['access_type'] = 'offline'
    flow.params['prompt'] = 'consent'
    creds = flow.step2_exchange(code=request.query.code)
    http_auth = creds.authorize(httplib2.Http())
    user_info_service = build('oauth2', 'v2', http=http_auth)
    get_user_task = user_info_service.userinfo().get()
    ds = datastore.Client()
    u = get_user_task.execute()

    # insert to cloud SQL
    db.execute(
        "REPLACE INTO google_fit SET username=%s, google_id=%s, full_name=%s, gender=%s, image_url=%s, email=%s, refresh_token=%s",
        (u['email'], u['id'], u['name'], u.get('gender'), u['picture'], u['email'], creds.refresh_token))

    # insert to Cloud Datastore
    entity = datastore.Entity(key=ds.key(backend.DATASTORE_KIND, u['email']))
    now = datetime.utcnow()
    entity.update({
        'refresh_token': creds.refresh_token,
        'google_id': u['id'],
        'gender': u.get('gender'),
        'picture': u['picture'],
        'timezone': unicode(timezone),
        'last_updated': now
    })
    ds.put(entity)
    response.content_type = 'application/json'

    # required to serialize entity
    entity['last_updated'] = now.strftime('%Y-%m-%d %H:%M:%S %Z')
    return json.dumps(entity.items())


@app.post('/v1/users/<username>/steps')
def insert_steps(username):
    error = check_headers_apikey()
    if error:
        return error
    steps = get_steps(username)
    if isinstance(steps, HTTPError) or isinstance(steps, HTTPResponse):
        return steps

    insert_result = {
        'inserted_count': backend.insert_steps(username, steps),
        'steps': steps
    }

    response.content_type = 'application/json'
    return insert_result


@app.get('/v1/users/<username>/steps')
def get_steps(username):
    error = check_headers_apikey()
    if error:
        return error
    http_auth, timezone = get_google_http_auth_n_user_timezone(username)
    end_time_millis, start_date, error = extract_header_dates()

    if error:
        if isinstance(error, HTTPError):
            return error
        else:
            return HTTPResponse({
                'code': httplib.BAD_REQUEST,
                'error': str(error)}, httplib.BAD_REQUEST)
    else:
        try:
            # end_time_millis in headers data is optional
            if end_time_millis is None:
                end_time_millis = backend.current_milli_time()

            steps = backend.get_daily_steps(http_auth, start_date['year'], start_date['month'], start_date['day'],
                                            end_time_millis, local_timezone=timezone)
            response.content_type = 'application/json'
            return steps
        except client.HttpAccessTokenRefreshError as err:
            return HTTPError(httplib.UNAUTHORIZED, "Refresh token invalid: " + str(err))
        except googleapiclient.errors.HttpError as err:
            return HTTPError(err.resp.status, "Google API HttpError: " + str(err))


@app.get('/v1')
def main():
    return static_file("post.html", ".")


@app.post('/v1/auth')
def google_auth():
    parts = request.urlparts
    redirect_uri = "{}://{}/oauth2callback".format(parts.scheme, parts.netloc)

    flow = client.flow_from_clientsecrets(
        backend.client_secret_file,
        scope=["profile", "email", 'https://www.googleapis.com/auth/fitness.activity.read',
               'https://www.googleapis.com/auth/fitness.body.read'],
        redirect_uri=redirect_uri)
    flow.params['access_type'] = 'offline'
    flow.params['prompt'] = 'consent'
    error = check_forms_apikey()
    if error:
        return error
    timezone = request.forms['timezone']
    auth_uri = flow.step1_get_authorize_url(state=timezone)
    redirect(auth_uri)


def check_headers_apikey():
    if 'apikey' not in request.headers or request.headers['apikey'] != backend.API_key:
        return HTTPError(httplib.UNAUTHORIZED, "invalid API key in {}".format("request.headers['apikey']"))


def check_forms_apikey():
    if 'apikey' not in request.forms or request.forms['apikey'] != backend.API_key:
        return HTTPError(httplib.UNAUTHORIZED, "invalid API key in {}".format("request.forms['apikey']"))


@app.get('/steps_for_user/<name>')
def steps_for_user(name, db):
    require_key()
    print(name)
    db.execute("SELECT day, steps FROM steps WHERE username=%s", (name,))
    result = dict([(r['day'], r['steps']) for r in db.fetchall()])
    print(result)
    response.content_type = 'application/json'
    return json.dumps(result, sort_keys=True, indent=4)


@app.get('/activity_for_user/<name>')
def activity_for_user(name, db):
    require_key()
    print(name)
    db.execute(
        "SELECT a.day, ROUND(SUM(a.length_ms) / 1000 / 60) AS minutes FROM activity a INNER JOIN activity_types t ON a.activity_type=t.id WHERE a.username=%s AND a.activity_type NOT IN {} GROUP BY a.day".format(
            bad_activities), (name,))
    result = dict([(r['day'], int(r['minutes'])) for r in db.fetchall()])
    print(result)
    response.content_type = 'application/json'
    return json.dumps(result, sort_keys=True, indent=4)


@app.get('/users/<name>/activities')
def user_activities(name, db):
    require_key()
    activities = query_activities(db, name)
    response.content_type = 'application/json'
    return json.dumps(activities, sort_keys=True, indent=4)


def query_activities(db, name):
    db.execute(
        "SELECT a.day, ROUND(a.length_ms / 1000 / 60) AS minutes, t.name as activity_type FROM activity a INNER JOIN activity_types t ON a.activity_type=t.id WHERE a.username=%s",
        (name,))
    result = db.fetchall()
    activities = {}
    for r in result:
        if r['day'] in activities:
            if 'daily_activities' not in activities[r['day']]:
                activities[r['day']]['daily_activities'] = []

            activities[r['day']]['daily_activities'].append(
                {"minutes": int(r['minutes']), "activity_type": r['activity_type']})

        else:
            activities[r['day']] = {}
            activities[r['day']]['daily_activities'] = []
            activities[r['day']]['daily_activities'].append(
                {"minutes": int(r['minutes']), "activity_type": r['activity_type']})
    return activities


@app.post('/v1/users/<username>/activities')
def insert_user_activities(username):
    error = check_headers_apikey()
    if error:
        return error
    activities = get_user_activities(username)
    if isinstance(activities, HTTPError) or isinstance(activities, HTTPResponse):
        return activities

    insert_result = {
        'inserted_count': backend.insert_activities(username, activities),
        'activities': activities
    }

    response.content_type = 'application/json'
    return insert_result


@app.get('/v1/users/<username>/activities')
def get_user_activities(username):
    error = check_headers_apikey()
    if error:
        return error
    http_auth, timezone = get_google_http_auth_n_user_timezone(username)
    end_time_millis, start_date, error = extract_header_dates()

    if error:
        if isinstance(error, HTTPError):
            return error
        else:
            return HTTPResponse({
                'code': httplib.BAD_REQUEST,
                'error': str(error)}, httplib.BAD_REQUEST)
    else:
        try:
            # end_time_millis in headers data is optional
            if end_time_millis is None:
                end_time_millis = backend.current_milli_time()

            activities = backend.get_daily_activities(http_auth, start_date['year'], start_date['month'],
                                                      start_date['day'], end_time_millis, local_timezone=timezone)
            response.content_type = 'application/json'
            return activities
        except client.HttpAccessTokenRefreshError as err:
            return HTTPError(httplib.UNAUTHORIZED, "Refresh token invalid: " + str(err))
        except googleapiclient.errors.HttpError as err:
            return HTTPError(err.resp.status, "Google API HttpError: " + str(err))


def extract_header_dates():
    """
    Extract headers of start_year, start_month, start_day, and end_time_millis
    where the start_* are local date and end_time_millis is the Unix Epoch time in milliseconds
    :return: end time in Unix Epoch time in milliseconds, start date dictionary, error if any
    """
    # parse headers data in request body
    start_date = {'year': request.headers.get('start_year', None), 'month': request.headers.get('start_month', None),
                  'day': request.headers.get('start_day', None)}
    end_time_millis = request.headers.get('end_time_millis', None)
    if end_time_millis is not None:
        try:
            end_time_millis = int(end_time_millis)
        except ValueError as e:
            return None, None, HTTPError(httplib.BAD_REQUEST,
                                         'Failed to convert end_time_millis in request.headers to int: ' + str(e))
    if start_date['year'] is None or start_date['month'] is None or start_date['day'] is None:
        return None, None, HTTPError(httplib.BAD_REQUEST, "headers did not contain start_year, start_month, start_day")
    else:
        start_date['year'] = int(start_date['year'])
        start_date['month'] = int(start_date['month'])
        start_date['day'] = int(start_date['day'])

    return end_time_millis, start_date, None


# bug: only 1 activity per day returned. correct result has multiple activities per day.
# Fix: refer to @app.get('/users/<name>/activities')
@app.get('/activity_for_user_details/<name>')
def activity_for_user_details(name, db):
    require_key()
    print(name)
    db.execute(
        "SELECT a.day, ROUND(a.length_ms / 1000 / 60) AS minutes, t.name as activity_type FROM activity a INNER JOIN activity_types t ON a.activity_type=t.id WHERE a.username=%s AND a.activity_type NOT IN {}".format(
            bad_activities), (name,))
    result = dict(
        [(r['day'], {"minutes": int(r['minutes']), "activity_type": r['activity_type']}) for r in db.fetchall()])
    print(result)
    response.content_type = 'application/json'
    return json.dumps(result, sort_keys=True, indent=4)


@app.get('/steps_for_user/last_week/<name>')
def steps_for_user_last_week(name, db):
    require_key()
    db.execute("SELECT SUM(steps) as sum FROM steps WHERE username=%s AND day >= date_sub(CURDATE(), INTERVAL 1 WEEK)",
               (name,))
    result = str(db.fetchone()['sum'])
    print(result)
    return result


@app.get('/steps_for_user/last_day/<name>')
def steps_for_user_last_day(name, db):
    require_key()
    db.execute("SELECT SUM(steps) as sum FROM steps WHERE username=%s AND day >= date_sub(CURDATE(), INTERVAL 1 DAY)",
               (name,))
    result = str(db.fetchone()['sum'])
    print(result)
    return result


@app.get('/users')
def get_users(db):
    require_key()
    db.execute("SELECT username FROM google_fit")
    result = [u['username'] for u in db.fetchall()]
    print(result)
    response.content_type = 'application/json'
    return json.dumps(result, sort_keys=True, indent=4)


@app.get('/step_leaderboard')
def steps_leaderboard(db):
    require_key()
    db.execute(
        "SELECT username, SUM(steps) as steps FROM steps WHERE day > date_sub(CURDATE(), INTERVAL 1 WEEK) GROUP BY username ORDER BY steps DESC LIMIT 20")
    result = OrderedDict([(r['username'], int(r['steps'])) for r in db.fetchall()])
    print(result)
    response.content_type = 'application/json'
    return json.dumps(result, indent=4)


@app.get('/activity_leaderboard')
def activity_leaderboard(db):
    require_key()
    db.execute(
        "SELECT username, ROUND(SUM(a.length_ms) / 1000 / 60) AS minutes FROM activity a INNER JOIN activity_types t ON a.activity_type=t.id WHERE day > date_sub(CURDATE(), INTERVAL 1 WEEK) AND a.activity_type NOT IN {} GROUP BY username ORDER BY minutes DESC LIMIT 20".format(
            bad_activities))
    result = OrderedDict([(r['username'], int(r['minutes'])) for r in db.fetchall()])
    print(result)
    response.content_type = 'application/json'
    return json.dumps(result, indent=4)


@app.get('/combined_leaderboard')
def combined_leaderboard(db):
    require_key()
    db.execute("""SELECT s.username, s.steps, m.minutes
FROM (
  SELECT username, SUM( steps ) AS steps
  FROM steps
  WHERE DAY > DATE_SUB( CURDATE( ) , INTERVAL 1 WEEK )
  GROUP BY username
) AS s
INNER JOIN (
  SELECT username, ROUND( SUM( a.length_ms ) /1000 /60 ) AS minutes
  FROM activity a
  INNER JOIN activity_types t ON a.activity_type = t.id
  WHERE a.day > DATE_SUB( CURDATE( ) , INTERVAL 1 WEEK )
  AND a.activity_type NOT
  IN {}
  GROUP BY a.username
) AS m ON s.username = m.username
ORDER BY s.steps DESC
LIMIT 20""".format(bad_activities))
    result = []
    for r in db.fetchall():
        result += [r['username'], int(r['steps']), int(r['minutes'])]
    print(result)
    response.content_type = 'application/json'
    return json.dumps(result, indent=4)


@app.get('/set_goal/<name>/<goal>')
def set_goal(name, goal, db):
    require_key()
    db.execute("REPLACE INTO activity_goals SET username=%s, minutes=%s", (name, goal))
    return "Goal set"


@app.post('/v1/users/<username>/heart')
def insert_heart_rate(username):
    error = check_headers_apikey()
    if error:
        return error
    http_auth, timezone = get_google_http_auth_n_user_timezone(username)
    end_time_millis, start_date, error = extract_header_dates()

    if error:
        if isinstance(error, HTTPError):
            return error
        else:
            return HTTPResponse({
                'code': httplib.BAD_REQUEST,
                'error': str(error)}, httplib.BAD_REQUEST)
    else:
        try:
            # end_time_millis in form data is optional
            if end_time_millis is None:
                result = backend.get_and_insert_heart_rate(http_auth, username,
                                                           start_date['year'], start_date['month'], start_date['day'],
                                                           local_timezone=timezone)
            else:
                result = backend.get_and_insert_heart_rate(http_auth, username,
                                                           start_date['year'], start_date['month'], start_date['day'],
                                                           end_time_millis, local_timezone=timezone)
            response.content_type = 'application/json'
            return result
        except client.HttpAccessTokenRefreshError as err:
            return HTTPError(httplib.UNAUTHORIZED, "Refresh token invalid: " + str(err))
        except googleapiclient.errors.HttpError as err:
            return HTTPError(err.resp.status, "Google API HttpError: " + str(err))


def get_google_http_auth_n_user_timezone(username):
    with open(backend.client_secret_file) as f:
        client_secret_json = json.load(f)
        client_id = client_secret_json['web']['client_id']
        client_secret = client_secret_json['web']['client_secret']
    ds = datastore.Client()
    key = ds.key('credentials', username)
    user = ds.get(key)
    assert user.key.id_or_name == username
    refresh_token = user['refresh_token']
    timezone = user['timezone']
    creds = client.GoogleCredentials(None, client_id, client_secret, refresh_token, None,
                                     "https://accounts.google.com/o/oauth2/token", "Python")
    http_auth = creds.authorize(httplib2.Http())
    return http_auth, timezone


@app.post('/v1/insert_daily_fitness')
def insert_daily_fitness_data_ondemand():
    """
    The query string needs to contain a list of users in the form of ?users=hil@gmail.com,estes@gmail.com,paes@gmail.com
    :return:
    """
    users_param = 'users'
    if users_param not in request.query:
        return HTTPError(httplib.BAD_REQUEST,
                         '{} does not exist in query string parameters; specify ?{}=user1@gmail.com,user2@company.com'.format(
                             users_param))
    usernames = request.query[users_param].split(',')

    return insert_daily_fitness_data_impl(usernames)


# callable only from App Engine cron jobs
@app.get('/v1/insert_daily_fitness')
def insert_daily_fitness_data():
    # validating request is from App Engine cron jobs
    app_engine_cron_header = 'X-Appengine-Cron'
    if app_engine_cron_header not in request.headers:
        return HTTPError(httplib.UNAUTHORIZED,
                         'Endpoint can only be invoked from Google App Engine cron jobs per https://cloud.google.com/appengine/docs/flexible/python/scheduling-jobs-with-cron-yaml')

    ds = datastore.Client()
    query = ds.query(kind=backend.DATASTORE_KIND)
    query.keys_only()
    usernames = list(query.fetch())
    usernames = [u.key.id_or_name for u in usernames]

    return insert_daily_fitness_data_impl(usernames)


def insert_daily_fitness_data_impl(usernames, bucket_name=backend.DEFAULT_BUCKET):
    """
    Call Google Fitness API for users in the Cloud Datastore credentials kind, save the responses in Cloud Storage,
    insert the fitness data to Cloud BigQuery.
    key is retry[username][category]['countdown']
    if value >= 0, retry down to value -1 or set value to -2 for non-recoverable errors
    if value is None, op has succeeded
    :param usernames: a list of usernames to call Google Fitness API with
    :param bucket_name: save responses from Google Fitness API to a Google Cloud Storage bucket
    :return: The results of getting from Google Fitness API and inserting to Cloud BigQuery
    """
    retry = {}
    threads = []

    for username in usernames:
        t = Thread(target=insert_daily_fitness_data_thread, args=(bucket_name, retry, username))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    is_error = False
    response.content_type = 'application/json'
    for username, category in retry.iteritems():
        for cat, cat_result in category.iteritems():
            if 'error' in cat_result:
                is_error = True
                break
    if is_error:
        return HTTPResponse(retry, httplib.INTERNAL_SERVER_ERROR)
    else:
        return retry


def insert_daily_fitness_data_thread(bucket_name, retry, username):
    error_reporting_client = error_reporting.Client()
    http_context = error_reporting.HTTPContext(method='GET', url='/v1/insert_daily_fitness',
                                               user_agent='cron job for user {}'.format(username))
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    http_auth, timezone = get_google_http_auth_n_user_timezone(username)
    # get today's local date - 1 day
    yesterday_local = datetime.now(pytz.timezone(timezone)) - timedelta(days=1)
    yesterday_local_str = yesterday_local.strftime(backend.DATE_FORMAT)
    df = backend.UserDataFlow(username, http_auth, yesterday_local.year,
                              yesterday_local.month,
                              yesterday_local.day, backend.current_milli_time(), timezone)
    retry[username] = {}
    categories = {'heartrate', 'activities', 'steps'}
    for category in categories:
        retry[username][category] = {}
        # countdown is the number of retries
        retry[username][category]['countdown'] = 1
        gs_path_get = '{}/{}/{}.json'.format(username, yesterday_local_str, category)
        gs_path_insert = '{}/{}/{}_inserted_count.json'.format(username, yesterday_local_str, category)
        get_result = None
        insert_result = None

        # start of the retry logic
        while retry[username][category]['countdown'] >= 0:
            try:
                if category == 'heartrate':
                    # get and insert heart rate data
                    insert_result = df.get_and_post_heart_rate()
                    get_result = insert_result['heart_datasets']
                elif category == 'activities':
                    # get and insert activities data
                    get_result = df.get_activities()
                    insert_result = df.post_activities()
                elif category == 'steps':
                    # get and insert step counts
                    get_result = df.get_steps()
                    insert_result = df.post_steps()
                # set to None upon success of getting API data and inserting to BigQuery
                retry[username][category]['countdown'] = None
            except client.HttpAccessTokenRefreshError as err:
                http_context.responseStatusCode = httplib.UNAUTHORIZED
                user_token_err = '{} has invalid refresh token'.format(username)
                error_reporting_client.report_exception(http_context=http_context,
                                                        user=user_token_err)
                retry[username][category]['error'] = "{}: {}".format(user_token_err, err)
                # can't recover; abandon retry
                retry[username][category]['countdown'] = -2
            except googleapiclient.errors.HttpError as err:
                http_context.responseStatusCode = err.resp.status
                error_reporting_client.report_exception(http_context=http_context,
                                                        user='Google API HttpError for user {}'.format(username))
                retry[username][category]['error'] = str(err)
                if err.resp.status in (
                        httplib.BAD_REQUEST, httplib.UNAUTHORIZED, httplib.NOT_FOUND, httplib.FORBIDDEN):
                    # can't recover; abandon retry
                    retry[username][category]['countdown'] = -2
            except Exception as err:
                # https://googleapis.github.io/google-cloud-python/latest/error-reporting/usage.html
                error_reporting_client.report_exception(http_context=http_context,
                                                        user='get and insert {} data for {} failed'.format(category,
                                                                                                           username))
                retry[username][category]['error'] = str(err)

            # if retry for user on category isn't None, recoverable failure happened, decrement the retry count
            if retry[username][category]['countdown'] is not None:
                retry[username][category]['countdown'] -= 1
            else:
                # exiting while loop because None >= 0 is False
                pass

        # per category, putting the get, insert results on Cloud Storage upon success
        if retry[username][category]['countdown'] is None:
            retry[username][category]['gs://'] = []
            blob_get_result = bucket.blob(gs_path_get)
            blob_get_result.upload_from_string(json.dumps(get_result))
            retry[username][category]['gs://'].append("{}/{}".format(bucket_name, gs_path_get))
            blob_insert_result = bucket.blob(gs_path_insert)
            blob_insert_result.upload_from_string(json.dumps(insert_result))
            retry[username][category]['gs://'].append("{}/{}".format(bucket_name, gs_path_insert))

        retry[username][category].pop('countdown')


port = int(os.environ.get('PORT', 8080))
prefix = os.environ.get('PREFIX', None)
if prefix:
    app.mount(prefix=prefix, app=app)

if __name__ == "__main__":
    try:
        try:
            app.run(host='0.0.0.0', port=port, debug=True, server='gunicorn', workers=8, timeout=1200)
        except ImportError:
            app.run(host='0.0.0.0', port=port, debug=True)
    except Exception as e:
        print >> sys.stderr, "error: {}".format(e)
