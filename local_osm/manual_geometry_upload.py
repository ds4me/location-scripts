import requests
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import LegacyApplicationClient
from uuid import uuid4
import geopandas as gpd
import geojson
from time import sleep

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


def api_post_request(url, token, json):
    headers = {"Authorization": "Bearer {}".format(token['access_token'])}
    r = requests.post(url, headers=headers, json=json)
    return r.status_code


def api_put_request(url, token, json):
    headers = {"Authorization": "Bearer {}".format(token['access_token'])}
    r = requests.put(url, headers=headers, json=json)
    print(r.text)
    return r.status_code


def get_location(token, externalId):
    url = f'https://servermhealth.ddc.moph.go.th/opensrp/rest/location/findByProperties?is_jurisdiction=true&return_geometry=true&properties_filter=externalId:{externalId}'
    res = api_get_request(url, token)
    if len(res) == 0:
        raise Exception(f'Cannot find the location with the following externalId: {externalId}')
    elif len(res) > 1:
        print('More than one location returned, looping through to return the first status="Active" focus')
        return [x for x in res if x['properties']['status'] == "Active"][0]
    else:
        return res[0]


def create_reveal_feature(token, feat):
    # IDs for the different hierarchy levels
    locationIds = [6,1,4,5,3,2]
    locationNames = ['Country','Province','District','Canton','Village','Operational Area']

    geoLevel = feat['properties']['geographicLevel']
    parent = get_location(feat["properties"]["externalParentId"])
    externalId = feat['properties']['externalId']

    return [{
        "type": "Feature",
        "id": str(uuid4()),
        "geometry": feat['geometry'],
        "properties": {
            "status": "Active",
            "parentId": parent["id"],
            "name": f'{feat["properties"]["name"]} ({externalId})',
            "geographicLevel": geoLevel,
            "version": 0,
            "externalId": externalId,
            "name_en": f'{feat["properties"]["name_en"]} ({externalId})'
        },
        "locationTags": [
            {
                "id": locationIds[geoLevel],
                "name": locationNames[geoLevel]
            }
        ]
    }]


def update_reveal_feature_geometry(token, feat):
    location = get_location(token, feat["properties"]["externalId"])
    location["geometry"] = feat['geometry']
    # location['properties']['version'] = location['properties']['version'] + 1
    return [location]


def send_to_reveal(token, feat, index, numFeats):
    url = 'https://servermhealth.ddc.moph.go.th/opensrp/rest/location/add?is_jurisdiction=true'
    # status = api_post_request(url, token, feat)
    status = api_put_request(url, token, feat)
    if status == 201:
        print(f'{index + 1}/{numFeats}: Jurisdiction created successfully for externalId {feat[0]["properties"]["externalId"]}. Status code: {status}')
    else:
        print(f'{index + 1}/{numFeats}: There was an issue creating the new jursidiction for externalId {feat[0]["properties"]["externalId"]}. Status code: {status}')


def main():

    # Sort the upload geographic level to ensure that lower levels can find uploaded features
    # df = gpd.read_file('C:/Users/efilip/Desktop/b1_addition.geojson')
    # sorted = df.sort_values(by=['geographicLevel']).reset_index(drop=True)
    # sorted.to_file('C:/Users/efilip/Desktop/B1_additions_sorted.geojson', driver="GeoJSON")

    # Get the required oauth token
    token = get_oauth_token()

    # Load the GeoJSON file
    with open('C:/Users/efilip/Desktop/shape_update.geojson', encoding='utf8') as f:
        data = geojson.load(f)

    # Get the number of features to upload
    numFeats = len(data['features'])

    # Loop through the features in the GeoJSON file - note that they should be sorted by geographicLevel
    for index, feat in enumerate(data['features']):

        # Create the feature from the
        # reveal_feature = create_reveal_feature(token, feat)
        reveal_feature = update_reveal_feature_geometry(token, feat)
        print(reveal_feature)

        # Send the new location and check the status
        send_to_reveal(token, reveal_feature, index, numFeats)

        # Sleep to prevent overloading the server
        sleep(5)



# Note that locations cannot be deleted once pushed. They can be deactivated by
# settings the status to Inactive, changing the externalId and name to something
# else, and replacing the ending of the parent ID with some zeros. Send it as JSON
# as part of a PUT request to this endpoint: https://servermhealth.ddc.moph.go.th/opensrp/rest/location?is_jurisdiction=true



if __name__ == '__main__':
    main()
