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
import tempfile


def get_oauth_token(config, server):
    token_url = config[f'{server}_reveal']['token_url']
    username = config[f'{server}_reveal']['username']
    password = config[f'{server}_reveal']['password']
    client_id = config[f'{server}_reveal']['client_id']
    client_secret = config[f'{server}_reveal']['client_secret']
    oauth = OAuth2Session(client=LegacyApplicationClient(client_id=client_id))
    return oauth.fetch_token(token_url=token_url,username=username, password=password,client_id=client_id, client_secret=client_secret)


def api_get_request(url, token):
    headers = {"Authorization": "Bearer {}".format(token['access_token'])}
    r = requests.get(url, headers=headers)
    return r.json()


def api_post_request(url, token, json):
    headers = {"Authorization": "Bearer {}".format(token['access_token'])}
    r = requests.post(url, headers=headers, json=json)
    # print(r.text)
    return r.status_code


def api_put_request(url, token, json):
    headers = {"Authorization": "Bearer {}".format(token['access_token'])}
    r = requests.put(url, headers=headers, json=json)
    # print(r.text)
    return r.status_code


def get_location(token, externalId, baseUrl):
    url = f'{baseUrl}/opensrp/rest/location/findByProperties?is_jurisdiction=true&return_geometry=true&properties_filter=externalId:{externalId}'
    res = api_get_request(url, token)
    if len(res) == 0:
        raise Exception(f'Cannot find the location with the following externalId: {externalId}')
    elif len(res) > 1:
        print('More than one location returned, looping through to return the first status="Active" focus')
        return [x for x in res if x['properties']['status'] == "Active"][0]
    else:
        return res[0]


def create_reveal_feature(token, feat, baseUrl):
    # IDs for the different hierarchy levels
    locationIds = [6,1,4,5,3,2]
    locationNames = ['Country','Province','District','Canton','Village','Operational Area']

    geoLevel = feat['properties']['geographicLevel']
    parent = get_location(token, feat["properties"]["externalParentId"], baseUrl)
    externalId = feat['properties']['externalId']

    return [{
        "type": "Feature",
        "id": str(uuid4()),
        "geometry": feat['geometry'] if feat['geometry'] != None else {"type": "Polygon", "coordinates": []},
        "properties": {
            "status": "Active",
            "parentId": parent["id"],
            "name": f'{feat["properties"]["name"]} ({externalId})',
            "geographicLevel": geoLevel,
            "version": 0,
            "externalId": externalId,
            "name_en": f' ({externalId})'
            # "name_en": f'{feat["properties"]["name_en"]} ({externalId})'
        },
        "locationTags": [
            {
                "id": locationIds[geoLevel],
                "name": locationNames[geoLevel]
            }
        ]
    }]


def update_reveal_feature_geometry(token, feat, baseUrl):
    location = get_location(token, feat["properties"]["externalId"], baseUrl)
    location["geometry"] = feat['geometry']
    return [location]


def send_to_reveal(token, feat, index, numFeats, type, baseUrl, server):
    if type == 'update':
        url = f'{baseUrl}/opensrp/rest/location?is_jurisdiction=true'
        status = api_put_request(url, token, feat[0])
    else:
        url = f'{baseUrl}/opensrp/rest/location/add?is_jurisdiction=true'
        status = api_post_request(url, token, feat)

    if status == 201:
        print(f'{index + 1}/{numFeats}: Jurisdiction {type}d successfully on the {server} server for externalId {feat[0]["properties"]["externalId"]}. Status code: {status}')
    else:
        print(f'{index + 1}/{numFeats}: There was an issue {type[0:len(type)-1]}ing the new jursidiction on the {server} server for externalId {feat[0]["properties"]["externalId"]}. Status code: {status}')


def valid_geojson(gjson):
    if os.path.isfile(gjson):
        if gjson.endswith('.geojson'):
            return gjson
        else:
            raise argparse.ArgumentTypeError(f'File must end with .geojson')
    else:
        raise argparse.ArgumentTypeError(f'{gjson} is not a valid file')


def main():

    # Note that locations cannot be deleted once pushed. They can be deactivated by
    # settings the status to Inactive, changing the externalId and name to something
    # else, and replacing the ending of the parent ID with some zeros. Send it as JSON
    # as part of a PUT request to this endpoint: https://servermhealth.ddc.moph.go.th/opensrp/rest/location?is_jurisdiction=true
    # TODO: Add option to deactivate foci?

    parser = argparse.ArgumentParser(description="Script to upload or edit foci using the Reveal API")
    parser.add_argument('geojson', type=valid_geojson, help="GeoJSON file with features to upload/update")
    parser.add_argument('type', choices=['create','update'], help="Type of operation - either creating new foci or updating existing ones")
    parser.add_argument('server', choices=['local','training'], help="Server in which the feature additions/changes should be made")
    args = parser.parse_args()

    # Get the config values, namely the base_url
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)),'config.ini'))
    baseUrl = config[f'{args.server}_reveal']['base_url']

    # Set a variable for the location of the GeoJSON file used for the upload
    geojsonLoc = args.geojson

    # If new features are being created, sort by geographic level to ensure that
    # hierarchy members are created in the correct order
    if args.type == 'create':
        df = gpd.read_file(args.geojson)
        sorted = df.sort_values(by=['geographicLevel']).reset_index(drop=True)
        geojsonLoc = os.path.join(tempfile.gettempdir(), 'sorted.geojson')
        sorted.to_file(geojsonLoc, driver='GeoJSON')

    # Get the required oauth token
    token = get_oauth_token(config, args.server)

    # Load the GeoJSON file
    with open(geojsonLoc, encoding='utf8') as f:
        data = geojson.load(f)

    # Get the number of features to upload
    numFeats = len(data['features'])

    # Loop through the features in the GeoJSON file - note that they should be sorted by geographicLevel
    for index, feat in enumerate(data['features']):

        # Switch on the type of operation
        reveal_feature = []
        if args.type == 'create':
            reveal_feature = create_reveal_feature(token, feat, baseUrl)
        elif args.type == 'update':
            reveal_feature = update_reveal_feature_geometry(token, feat, baseUrl)

        # print(reveal_feature)

        # Send the new location and check the status
        send_to_reveal(token, reveal_feature, index, numFeats, args.type, baseUrl, args.server)

        # Sleep to prevent overloading the server
        sleep(5)

    # Clean up temp file, if necessary
    if args.geojson != geojsonLoc:
        os.remove(geojsonLoc)

if __name__ == '__main__':
    main()
