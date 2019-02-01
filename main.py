#!/usr/bin/env python
import json
from collections import OrderedDict

import bottle_mysql
import httplib2
from bottle import *
from google.cloud import datastore
from googleapiclient.discovery import build
from oauth2client import client
from datetime import datetime
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

# Google Cloud Stackdriver Debugger
try:
    print("running code: import googleclouddebugger")
    import googleclouddebugger

    googleclouddebugger.enable()
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
        abort(httplib.INTERNAL_SERVER_ERROR, "database connection is in a bad state")


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
    entity = datastore.Entity(key=ds.key('credentials', u['email']))
    now = datetime.utcnow()
    entity.update({
        'refresh_token': creds.refresh_token,
        'google_id': u['id'],
        'gender': u.get('gender'),
        'picture': u['picture'],
        'last_updated': now
    })
    ds.put(entity)
    response.content_type = 'application/json'

    # required to serialize entity
    entity['last_updated'] = now.strftime('%Y-%m-%d %H:%M:%S %Z')
    return json.dumps(entity.items())


@app.post('/v1/users/<username>/steps')
def insert_steps(username):
    steps = get_steps(username)
    insert_result = {
        'inserted_count': backend.insert_steps(username, steps),
        'steps': steps
    }

    response.content_type = 'application/json'
    return insert_result


@app.get('/v1/users/<username>/steps')
def get_steps(username):
    assert_headers_apikey()
    http_auth = get_http_auth_google_apis(username)
    end_time_millis, start_date = extract_header_dates()

    # end_time_millis in headers data is optional
    if end_time_millis is None:
        steps = backend.get_daily_steps(http_auth, start_date['year'], start_date['month'], start_date['day'])
    else:
        steps = backend.get_daily_steps(http_auth,
                                        start_date['year'], start_date['month'], start_date['day'], end_time_millis)
    response.content_type = 'application/json'
    return steps


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
    assert_forms_apikey()
    auth_uri = flow.step1_get_authorize_url()
    redirect(auth_uri)


def assert_headers_apikey():
    if 'apikey' not in request.headers or request.headers['apikey'] != backend.API_key:
        abort(httplib.UNAUTHORIZED, "invalid API key in {}".format("request.headers['apikey']"))


def assert_forms_apikey():
    if 'apikey' not in request.forms or request.forms['apikey'] != backend.API_key:
        abort(httplib.UNAUTHORIZED, "invalid API key in {}".format("request.forms['apikey']"))


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
    activities = get_user_activities(username)

    insert_result = {
        'inserted_count': backend.insert_activities(username, activities),
        'activities': activities
    }

    response.content_type = 'application/json'
    return insert_result


@app.get('/v1/users/<username>/activities')
def get_user_activities(username):
    assert_headers_apikey()
    http_auth = get_http_auth_google_apis(username)
    end_time_millis, start_date = extract_header_dates()

    # end_time_millis in headers data is optional
    if end_time_millis is None:
        activities = backend.get_activities(http_auth, start_date['year'], start_date['month'], start_date['day'])
    else:
        activities = backend.get_activities(http_auth,
                                            start_date['year'], start_date['month'], start_date['day'], end_time_millis)

    response.content_type = 'application/json'
    return activities


def extract_header_dates():
    # parse headers data in request body
    start_date = {'year': request.headers.get('start_year', None), 'month': request.headers.get('start_month', None),
                  'day': request.headers.get('start_day', None)}
    end_time_millis = request.headers.get('end_time_millis', None)
    if end_time_millis is not None:
        end_time_millis = int(end_time_millis)
    if start_date['year'] is None or start_date['month'] is None or start_date['day'] is None:
        abort(httplib.BAD_REQUEST, "headers did not contain start_year, start_month, start_day")
    else:
        start_date['year'] = int(start_date['year'])
        start_date['month'] = int(start_date['month'])
        start_date['day'] = int(start_date['day'])

    return end_time_millis, start_date


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
    assert_headers_apikey()
    http_auth = get_http_auth_google_apis(username)
    end_time_millis, start_date = extract_header_dates()

    # end_time_millis in form data is optional
    if end_time_millis is None:
        result = backend.get_and_insert_heart_rate(http_auth, username,
                                                   start_date['year'], start_date['month'], start_date['day'])
    else:
        result = backend.get_and_insert_heart_rate(http_auth, username,
                                                   start_date['year'], start_date['month'], start_date['day'],
                                                   end_time_millis)
    response.content_type = 'application/json'
    return result


def get_http_auth_google_apis(user):
    with open(backend.client_secret_file) as f:
        client_secret_json = json.load(f)
        client_id = client_secret_json['web']['client_id']
        client_secret = client_secret_json['web']['client_secret']
    ds = datastore.Client()
    key = ds.key('credentials', user)
    user_creds = ds.get(key)
    assert user_creds.key.id_or_name == user
    refresh_token = user_creds['refresh_token']
    creds = client.GoogleCredentials(None, client_id, client_secret, refresh_token, None,
                                     "https://accounts.google.com/o/oauth2/token", "Python")
    http_auth = creds.authorize(httplib2.Http())
    return http_auth


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
