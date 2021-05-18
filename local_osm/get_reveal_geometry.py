import json
import pandas as pd
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import LegacyApplicationClient
import geojson
import argparse
import os
import configparser
from time import sleep
from retry_requests import retry
from requests import Session

# Get a session to be used through to ensure requests are retried instead of throwing errors
retrySession = retry(Session(), retries=10, backoff_factor=0.1)


def get_oauth_token(server):
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)),'config.ini'))
    token_url = config[f'{server}_reveal']['token_url']
    username = config[f'{server}_reveal']['username']
    password = config[f'{server}_reveal']['password']
    client_id = config[f'{server}_reveal']['client_id']
    client_secret = config[f'{server}_reveal']['client_secret']
    oauth = OAuth2Session(client=LegacyApplicationClient(client_id=client_id))
    return oauth.fetch_token(token_url=token_url,username=username, password=password,client_id=client_id, client_secret=client_secret)


def api_get_request(url, token):
    headers = {"Authorization": "Bearer {}".format(token['access_token'])}
    r = retrySession.get(url, headers=headers)
    if(r.status_code != 200):
        raise Exception(f'The following error occurred getting locations from {url}: {r.text}')
    else:
        return r.json()

def get_locations(geometry, server, jurisdiction):
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)),'config.ini'))
    baseUrl = config[f'{server}_reveal']['base_url']

    token = get_oauth_token(server)
    serverVersion = 0
    limit = 5000
    allLocations = []

    print(f'Looping through the api to get {"features" if jurisdiction == "true" else "structures"}, this may take a few minutes...')
    while serverVersion != -1:
        # print(f'serverVersion: {serverVersion}')
        url = url = f'{baseUrl}/opensrp/rest/location/getAll?is_jurisdiction={jurisdiction}&serverVersion={serverVersion}&limit={limit}&return_geometry={geometry}'
        locations = api_get_request(url, token)
        # print(f'Num plans returned: {len(locations)}')
        # sleep(5)
        if len(locations) == limit:
            serverVersion = locations[len(locations)-1]['serverVersion']
            [allLocations.append(l) for l in locations[0:len(locations)-1]]
        else:
            [allLocations.append(l) for l in locations]
            serverVersion = -1

    return allLocations


def check_path(saveFile, type):
    separator = "/" if "/" in saveFile else "\\"
    path = saveFile.rsplit(separator,1)[0]
    if os.path.isdir(path):
        if type == 'geojson':
            if not saveFile.endswith('.geojson'):
                raise argparse.ArgumentTypeError("For geojson output, file must end with '.geojson'")
            else:
                return True
        if type == 'xlsx':
            if not saveFile.endswith('.xlsx'):
                raise argparse.ArgumentTypeError("for xlsx output, file must end with '.xlsx'")
            else:
                return True
    else:
        raise argparse.ArgumentTypeError(f'The save location ({saveFile}) is not on a valid path')


def main():
    parser = argparse.ArgumentParser(description="Script to extract locations from Reveal")
    parser.add_argument('-s', '--server', dest='server', choices=['local','training'], default='local', help='Server to pull the locations from. Defaults to local')
    parser.add_argument('-o', '--saveLocation', dest='saveFile', default=None, help="Save location of the file, defaults to the current folder. Defaults to script location")
    parser.add_argument('-t' '--type', dest='type', choices=['geojson', 'xlsx'], default='geojson', help='Type of export, defaults to geojson, xlsx is normalized. Defaults to geojson')
    parser.add_argument('-j', '--jurisdiction', dest='jurisdiction', choices=['true', 'false'], default='true', help='Whether to get admin boundaries (true) or structures (false). Defaults to true')
    parser.add_argument('-g', '--geometry', dest='geometry', choices=['true', 'false'], default='true', help='Whether to retrieve the geometry of the features/structures. Defaults to true')
    args = parser.parse_args()

    saveFile = args.saveFile
    if args.saveFile == None:
        saveFile = os.path.join(os.path.dirname(os.path.realpath(__file__)),f'reveal_{"features" if args.jurisdiction == "true" else "structures"}_{args.server}.geojson') if args.type == 'geojson' else os.path.join(os.getcwd(),f'reveal_{"features" if args.jurisdiction == "true" else "structures"}_{args.server}.xlsx')

    if check_path(saveFile, args.type):
        # get_geometry = 'true' if args.type == 'geojson' else 'false'

        res = get_locations(args.geometry, args.server, args.jurisdiction)

        print(f'Found {len(res)} {"features" if args.jurisdiction == "true" else "structures"}')

        if args.type == 'geojson':
            feats = geojson.FeatureCollection(res)
            with open(saveFile, 'w') as f:
                geojson.dump(feats, f)

        elif args.type == 'xlsx':
            norm = pd.json_normalize(res)
            norm.to_excel(saveFile)

        print(f'Saved {args.type} file to {saveFile}')

if __name__ == '__main__':
    main()
