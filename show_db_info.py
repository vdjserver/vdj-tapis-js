#
# Show some info about the database and collections.
#

import json
import os
import airr
import yaml
import requests
import argparse
from tapipy.tapis import Tapis

# Setup
def getConfig():
    cfg = {}
    cfg['api_server'] = os.getenv('TAPIS_V3_HOST')
    cfg['username'] = os.getenv('VDJ_SERVICE_ACCOUNT')
    cfg['password'] = os.getenv('VDJ_SERVICE_ACCOUNT_SECRET')
    cfg['dbname'] = os.getenv('MONGODB_DB')
    return cfg

# Fetches a user token based on the supplied auth object
# and returns the auth object with token data on success
def getToken(config):
    # Create python Tapis client for user
    t = Tapis(base_url= "https://" + config['api_server'],
              username=config['username'],
              password=config['password'])

    # Call to Tokens API to get access token
    t.get_tokens()
    #print(t.access_token)

    return t.access_token

# show collections
def showCollections(token, config):
    headers = {
        "Content-Type":"application/json",
        "Accept": "application/json",
        "X-Tapis-Token": token.access_token
    }

    url = 'https://' + config['api_server'] + '/v3/meta/' + config['dbname']
    resp = requests.get(url, headers=headers)
    print(json.dumps(resp.json(), indent=2))

# show indexes
def showIndexes(token, config, collection):
    headers = {
        "Content-Type":"application/json",
        "Accept": "application/json",
        "X-Tapis-Token": token.access_token
    }

    url = 'https://' + config['api_server'] + '/v3/meta/' + config['dbname'] + '/' + collection + '/_indexes'
    resp = requests.get(url, headers=headers)
    print(json.dumps(resp.json(), indent=2))

# main entry
if (__name__=="__main__"):
    parser = argparse.ArgumentParser(description='Show database info.')
    args = parser.parse_args()

    if args:
        config = getConfig()
        token = getToken(config)

        print('')
        print('**** Collections')
        print('')
        showCollections(token, config)

        print('')
        print('**** tapis_meta Indexes')
        print('')
        showIndexes(token, config, 'tapis_meta')
