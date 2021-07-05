
import geojson
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import LegacyApplicationClient
import configparser
from retry_requests import retry
from requests import Session
import os

def get_oauth_token(config, server):
    token_url = config[f'{server}_reveal']['token_url']
    username = config[f'{server}_reveal']['username']
    password = config[f'{server}_reveal']['password']
    client_id = config[f'{server}_reveal']['client_id']
    client_secret = config[f'{server}_reveal']['client_secret']
    oauth = OAuth2Session(client=LegacyApplicationClient(client_id=client_id))
    return oauth.fetch_token(token_url=token_url,username=username, password=password,client_id=client_id, client_secret=client_secret)


def main():

    # Get the config details from config.ini to be used throughout
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)),'config.ini'))

    # Get the token for downloading the Reveal hierarchy and uploading the edits/new foci
    token = get_oauth_token(config, 'local')

    # Create the requests session with retrys and backoff
    retrySession = retry(Session(), retries=10, backoff_factor=0.1)
    headers = {"Authorization": "Bearer {}".format(token['access_token'])}
    url = f'{config["local_reveal"]["base_url"]}/opensrp/rest/location?is_jurisdiction=false'

    # Load the geojson to fix
    with open('C:/Users/efilip/Desktop/Work Projects/location-scripts/local_osm/reveal_structures_local.geojson', encoding='utf8') as f:
        data = geojson.load(f)


    toFix = []

    index = 1
    # Loop through the features and upload them
    for idx, feat in enumerate(data["features"]):
        geom = feat['geometry']
        if(geom != None and geom['type'] == 'Point' and geom['coordinates'][0] < 80):
            toFix.append(feat)
            # newCoords = [geom['coordinates'][1], geom['coordinates'][0]]
            # fixedFeat = feat
            # fixedFeat['geometry']['coordinates'] = newCoords
            # print(f'{index}: externalId: {feat["properties"]["externalId"]}')
            # retrySession.put(url, headers=headers, json=fixedFeat, timeout=30)
            # index = index + 1


    for index, feat in enumerate(toFix[2:]):
        newCoords = [feat['geometry']['coordinates'][1], feat['geometry']['coordinates'][0]]
        fixedFeat = feat
        fixedFeat["geometry"]["coordinates"] = newCoords
        print(f'{index + 1}/{len(toFix[2:])}: Fixing ID {feat["id"]}')
        retrySession.put(url, headers=headers, json=fixedFeat)

if __name__ == '__main__':
    main()
