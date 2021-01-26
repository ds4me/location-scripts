import requests
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import LegacyApplicationClient
from uuid import uuid4
import geopandas as gpd
import geojson
from time import sleep
import os
import argparse
import configparser

def get_oauth_token():
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)),'config.ini'))
    token_url = config['local_reveal']['token_url']
    username = config['local_reveal']['username']
    password = config['local_reveal']['password']
    client_id = config['local_reveal']['client_id']
    client_secret = config['local_reveal']['client_secret']
    oauth = OAuth2Session(client=LegacyApplicationClient(client_id=client_id))
    return oauth.fetch_token(token_url=token_url,username=username, password=password,client_id=client_id, client_secret=client_secret)

def get_feature(token, externalId):
    url = f'https://servermhealth.ddc.moph.go.th/opensrp/rest/location/findByProperties?is_jurisdiction=true&return_geometry=true&properties_filter=externalId:{externalId}'
    headers = {"Authorization": "Bearer {}".format(token['access_token'])}
    r = requests.get(url, headers=headers)
    res = r.json()

    if len(res) == 0:
        raise Exception(f'Cannot find the location with the following externalId: {externalId}')
    elif len(res) > 1:
        print('More than one location returned, looping through to return the first status="Active" focus')
        return [x for x in res if x['properties']['status'] == "Active"][0]
    else:
        return res[0]



def update_feature(token, feat):
    url = 'https://servermhealth.ddc.moph.go.th/opensrp/rest/location?is_jurisdiction=true'
    headers = {"Authorization": "Bearer {}".format(token['access_token'])}
    r = requests.put(url, headers=headers, json=feat[0])
    print(r.text)
    status = r.status_code

    if status == 201:
        print(f'Jurisdiction created successfully for externalId {feat[0]["properties"]["externalId"]}. Status code: {status}')
    else:
        print(f'There was an issue creating the new jursidiction for externalId {feat[0]["properties"]["externalId"]}. Status code: {status}')


def main():
    token = get_oauth_token()
    feature = get_feature(token, 0)
    feature['geometry']['coordinates'] = []
    update_feature(token, [feature])



if __name__ == '__main__':
    main()
