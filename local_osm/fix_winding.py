# from geojson_rewind import rewind
# import geojson
#
# with open('C:/Users/efilip/Desktop/geojson_validator/toFix.geojson', encoding='utf8') as f:
#     data = geojson.load(f)
#
#
# fixed = []
# for feat in data["features"]:
#     fixed.append(rewind(feat))
#
# fc = geojson.FeatureCollection(fixed)
#
# with open('C:/Users/efilip/Desktop/rewound.geojson', 'w', encoding='utf8') as f:
#     geojson.dump(fc,f, ensure_ascii=False)
#


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
    url = f'{config["local_reveal"]["base_url"]}/opensrp/rest/location?is_jurisdiction=true'

    # Load the geojson to fix
    with open('C:/Users/efilip/Desktop/rewound.geojson', encoding='utf8') as f:
        data = geojson.load(f)

    # Loop through the features and upload them
    for idx, feat in enumerate(data["features"][2:]):
        print(f'{idx + 1}/{len(data["features"][2:])}: externalId: {feat["properties"]["externalId"]}')
        retrySession.put(url, headers=headers, json=feat, timeout=30)

if __name__ == '__main__':
    main()
