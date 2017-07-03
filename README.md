# fit_api
A web interface to gain consent from a user, to obtain their Google Fit data from the API, such that it can be resurfaced more easily via API  

## Installation

Copy config.py.example to config.py, replacing with your database credentials  
`sudo apt-get install python-mysqldb` (for debian based distros), or `sudo yum install MySQL-python` (for rpm-based)
`sudo pip install -r requirements.txt`  
Download your OAuth 2.0 client ID JSON file from https://console.developers.google.com/apis/credentials. Also configure this with the redirect URIs you will be using  

## Running

`python web_server.py`
