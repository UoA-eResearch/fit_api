#!/usr/bin/env python
import json
from collections import OrderedDict

import bottle_mysql
import httplib2
from bottle import *
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


port = int(os.environ.get('PORT', 8080))
prefix = os.environ.get('PREFIX', None)
if prefix:
    app.mount(prefix=prefix, app=app)

if __name__ == "__main__":
    try:
        try:
            app.run(host='0.0.0.0', port=port, debug=True, server='gunicorn', workers=2, timeout=1200)
        except ImportError:
            app.run(host='0.0.0.0', port=port, debug=True)
    except Exception as e:
        print >> sys.stderr, "error: {}".format(e)
