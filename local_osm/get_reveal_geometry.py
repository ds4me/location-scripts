import json
import pandas as pd
import requests
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import LegacyApplicationClient
import geojson
import argparse
import os


def get_oauth_token():
    token_url = 'https://keycloakmhealth.ddc.moph.go.th/auth/realms/reveal-thailand-production/protocol/openid-connect/token'
    username = 'nifi-user'
    password = '2dm647ahCWpb7aZ29DZeATWDa'
    client_id = 'reveal-thailand-production'
    client_secret = 'e076fd42-a301-43c5-a465-7c855bf2e9eb'
    oauth = OAuth2Session(client=LegacyApplicationClient(client_id=client_id))
    return oauth.fetch_token(token_url=token_url,username=username, password=password,client_id=client_id, client_secret=client_secret)


def api_get_request(url, token):
    headers = {"Authorization": "Bearer {}".format(token['access_token'])}
    r = requests.get(url, headers=headers)
    return r.json()


def path_with_geojson(saveLoc):
    separator = "/" if "/" in saveLoc else "\\"
    path = saveLoc.rsplit(separator,1)[0]
    if os.path.isdir(path):
        if not saveLoc.endswith('.geojson'):
            raise argparse.ArgumentTypeError("Output file must end with '.geojson'")
        else:
            return saveLoc
    else:
        raise argparse.ArgumentTypeError(f'The save location ({saveLoc}) is not on a valid path')


def main():
    parser = argparse.ArgumentParser(description="Script to extract locations from Reveal")
    parser.add_argument('-s', '--saveLocation', dest='saveFile', type=path_with_geojson, default=os.path.join(os.getcwd(),"reveal.geojson") help="Save location of the file, defaults to the current folder")
    args = parser.parse_args()


    token = get_oauth_token()
    url = 'https://servermhealth.ddc.moph.go.th/opensrp/rest/location/getAll?is_jurisdiction=true&serverVersion=0&limit=50000&return_geometry=true'
    res = api_get_request(url, token)

    # j = pd.json_normalize(res)
    # j.to_excel("C:/users/efilip/desktop/testing2.xlsx")

    feats = geojson.FeatureCollection(res)
    with open(args.saveFile, 'w') as f:
        geojson.dump(feats, f)


if __name__ == '__main__':
    main()
