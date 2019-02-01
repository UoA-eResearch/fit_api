# fit_api
A web interface to gain consent from a user, to obtain their Google Fit data from the API, such that it can be resurfaced more easily via API  

## Installation
1. Follow the instructions to create an OAuth 2.0 Client ID and secret: [https://developers.google.com/fit/rest/v1/get-started]
2. set APP_CONFIG environment variables to a configuration filename in the project directory. It's app.config by default.
 Set each values in the configuration file without quotes or double quotes.
3. Set CLIENT_SECRET environment variables to a path/filename downloaded from the prior step of Client ID for Web application.
 You can download the file from [https://console.developers.google.com/apis/credentials]. Verify the redirect URIs are correct in the file.

### Python2
`sudo apt-get install python-mysqldb` (for debian based distros), or `sudo yum install MySQL-python` (for rpm-based)  
`sudo pip install -r requirements.txt`

### Python3
`sudo apt-get install python3-mysqldb` or `sudo yum install MySQL-python3`  
`sudo pip3 install -r requirements.txt`  

### Create tables in Database and BigQuery

`sudo mysql < structure.sql` to create the database and tables. Create the tables in Google Cloud BigQuery ->
**activities table**

|Field name|Type|Mode|
|----------|----|----|
|username|STRING|REQUIRED|
|recordedLocalDate|DATE|REQUIRED|
|activity_type|INTEGER|REQUIRED|
|seconds|INTEGER|NULLABLE|
|segments|INTEGER|NULLABLE|

**activity_segments**

|Field name|Type|Mode|
|----------|----|----|
|username|STRING|REQUIRED|
|recordedLocalDate|DATE|REQUIRED|
|activity_type|INTEGER|REQUIRED|
|startTimeNanos|STRING|NULLABLE|
|endTimeNanos|STRING|NULLABLE|
|originDataSourceId|STRING|NULLABLE|

**activity_types**

|Field name|Type|Mode|
|----------|----|----|
|id|INTEGER|REQUIRED|
|name|STRING|NULLABLE|

**heartrate**

|Field name|Type|Mode|
|----------|----|----|
|username|STRING|REQUIRED|
|recordedTimeNanos|INTEGER|REQUIRED|
|recordedLocalDate|DATE|NULLABLE|
|bpm|INTEGER|REQUIRED|

insert acticity types into activity_types table by running the section of structure.sql:
```INSERT INTO `activity_types` (name, id) VALUES```.
Modify the syntax to fit [https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax]

## Deploy to Google App Engine Flexible
1. Initialize and ready Google Cloud Platform App Engine, BigQuery from Google Cloud Console. Create a service account for local debugging that has the roles of
 BigQuery Data Editor,
BigQuery Job User,
Cloud Datastore User.
2. Add the same roles to the service account created by App Engine: `service-0000000000@gae-api-prod.google.com.iam.gserviceaccount.com`, 'App Engine Flexible Environment Service Agent'.
3. app.yaml: set service to the default service or desired service in App Engine
4. app.yaml: set cloud_sql_instances: Your Instance connection name
5. `gcloud app deploy` [https://cloud.google.com/appengine/docs/flexible/python/testing-and-deploying-your-app]

### Environment variables:
1. client secret filename in the project directory as CLIENT_SECRET: client_secret_file.json
2. set the application default credential to the service account that has BigQuery Data Editor, BigQuery Job User, Cloud Datastore User role as GOOGLE_APPLICATION_CREDENTIALS: /path/to/GCP_service_account.json. Avoid putting the path in the same directory to reduce the chances of pushing it to source control.
3. app.yaml: change CLIENT_SECRET environment variables to client secret filename in the project directory.
4. app.yaml: change APP_CONFIG environment variables to a configuration filename in the project directory.

### Python packages
Add contents in requirements-gae.txt to requirements.txt for App Engine to download the dependent packages.


## Running

`python main.py` for Python2 or `python3 main.py` for Python3
