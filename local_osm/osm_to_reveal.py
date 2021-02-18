import configparser
import os
import requests
import re
import geopandas as gpd
import numpy as np
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import LegacyApplicationClient
import geojson
import math
import pandas as pd
import rtree
import shapely
import tempfile
from uuid import uuid4

'''
    Full upload script for pulling from OSM, validing the foci, and pushing updates to the server
    1. Download foci from OSM
        1.1. Validate that the geometry is valid
        1.2. Allow user input on which foci need to be uploaded/edited and filter accordingly
    2. Run validation, allow user input to add missing hierarchy members, prompt for Thai names for members
    3. Upload
'''

def print_hierarchy_details(gdf, name):
    print(f'Loaded {name} with {len(gdf)} features:')
    print(f'{len(gdf.loc[gdf.externalId.astype(str).str.len() == 2])} provinces')
    print(f'{len(gdf.loc[gdf.externalId.astype(str).str.len() == 4])} districts')
    print(f'{len(gdf.loc[gdf.externalId.astype(str).str.len() == 6])} sub-districts')
    print(f'{len(gdf.loc[gdf.externalId.astype(str).str.len() == 8])} villages')
    print(f'{len(gdf.loc[gdf.externalId.astype(str).str.len() == 10])} foci')


def get_osm_features(config, action, fociIds):
    print(f'\nDownloading all foci from bvbdosm...')
    url = config['local_osm']['url'] + '/api/get_all_ways'
    r = requests.get(url)
    fc = r.json()

    for feature in fc['features']:
        props = feature['properties']

        # Try to get the description and name, but throw an error if the focus is missing these tags in OSM
        try:
            id = int(props['description'].strip())
            name = props['name'].strip()
        except TypeError as e:
            print(f'OSM ID {feature["id"]} is missing a description or name tag. Please fix directly at https://bvbdosm.herokuapp.com/way/{feature["id"]}')

        # Catch descriptions that are less than 10 digits
        if len(str(id)) is not 10:
            raise TypeError(f'The description tag for the OSM way {feature["id"]} is not a 10-digit code: {props}. Please fix directly at https://bvbdosm.herokuapp.com/way/{feature["id"]}')

        # Set the necessary props
        props['externalId'] = id
        props['geographicLevel'] = 5
        props['externalParentId'] = id // 100
        props['name'] = name
        del props['description']

    # TODO: check if this is valid GEOJSON?
    print(f'Downloaded {len(fc["features"])} features')

    filteredOsmFeats = [x for x in fc["features"] if x['properties']['externalId'] in fociIds]
    if len(filteredOsmFeats) != len(fociIds):
        print(f'{len(filteredOsmFeats)} of the {len(fociIds)} foci could be found on bvbdosm, double check that they are mapped and have the correct description')

    osmGdf = gpd.GeoDataFrame.from_features(filteredOsmFeats, crs="EPSG:4326")
    print(f'Filtered to the {len(fociIds)} foci to {action}')
    return osmGdf


def download_reveal_jurisdictions(token):

    # Get the location of the where the reveal.geojson file will be stored - local folder
    jurisdictionFile = os.path.join(os.path.dirname(os.path.realpath(__file__)), "reveal_features_local.geojson")

    # Start the server version at 0 for pagination, set the request limit, and create a list for the locations
    serverVersion = 0
    limit = 5000
    allLocations = []

    # Loop through paginated requests until explicitly told to stop
    print('\nDownloading all Reveal locations - this may take a few minutes...')
    while serverVersion != -1:

        # print(f'serverVersion: {serverVersion}')

        # Set the URL and headers
        url = url = f'https://servermhealth.ddc.moph.go.th/opensrp/rest/location/getAll?is_jurisdiction=true&serverVersion={serverVersion}&limit={limit}&return_geometry=true'
        headers = {"Authorization": "Bearer {}".format(token['access_token'])}

        # Request from the server
        r = requests.get(url, headers=headers)

        # If there's an error, throw an exception with information
        if(r.status_code != 200):
            raise Exception(f'The following error occurred getting locations from {url}: {r.text}')

        # Otherwise, get a JSON of the response
        locations = r.json()

        # If the number of returned locations equals the limit, there are more to get
        if len(locations) == limit:
            # Update the serverVersion and append locations to the list
            serverVersion = locations[len(locations)-1]['serverVersion']
            [allLocations.append(l) for l in locations[0:len(locations)-1]]

        # Otherwise, append the last locations to the list and stop the loop
        else:
            [allLocations.append(l) for l in locations]
            serverVersion = -1

    # Save these to a file in the current folder
    feats = geojson.FeatureCollection(allLocations)
    with open(jurisdictionFile, 'w') as f:
        geojson.dump(feats, f)

    print('Reveal GeoJSON successfully downloaded')

    # return the location of the downloaded file
    return jurisdictionFile

def check_size(gdf, min_area, max_area):

    print(f'\nChecking foci that are smaller than {round(min_area)}m^2 or larger than {round(max_area/1000000)}km^2...')

    # Convert the CRS to measure size in meters
    if gdf.crs is not "EPSG:3857": gdf = gdf.to_crs("EPSG:3857")

    # Get just the foci
    justFoci = gdf.loc[(gdf.geographicLevel.astype(str) == '5')]

    # Filter out any foci that meet the criteria
    smallFoci = justFoci.loc[justFoci['geometry'].area <= min_area].externalId
    largeFoci = justFoci.loc[justFoci['geometry'].area >= max_area].externalId

    # TODO: would be good to print the 10 digit ID and the area of the location so that users are aware just how big/small it is
    if len(smallFoci):
        smallFoci = smallFoci.sort_values()
        singleLineSmallFoci = "\n".join(map(str,smallFoci))
        print(f'Verify that the following {len(smallFoci)} small foci are correct: \n{singleLineSmallFoci}')
    if len(largeFoci):
        largeFoci = largeFoci.sort_values()
        singleLineLargeFoci = "\n".join(map(str, largeFoci))
        print(f'Verify that the following {len(largeFoci)} large foci are correct: \n{singleLineLargeFoci}')
    if not len(smallFoci) and not len(largeFoci):
        print("Foci sizes are okay!")
    print()

def print_missing_hierarchy_members(list,type):
    if len(list):
        list.sort()
        singleLineVals = "\n".join(map(str,list))
        print(f'Missing {len(list)} {type}: \n{list}')



def add_missing_hierarchy_members(gdf, missing):
    feats = []
    for x in missing:
        ex_id = x
        ex_parent_id = str(int(x) // 100) if len(x) > 2 else 0
        geo_level = len(x) // 2

        hierarchyMemberType = None
        if geo_level == 1:
            hierarchyMemberType = 'province'
        elif geo_level == 2:
            hierarchyMemberType = 'district'
        elif geo_level == 3:
            hierarchyMemberType = 'subdistrict'
        elif geo_level == 4:
            hierarchyMemberType = 'village'
        elif geo_level == 5:
            hierarchyMemberType = 'focus'

        print(f'Please enter the Thai name of the new {hierarchyMemberType} ({ex_id})')
        focusName = input(": ")


        feat = geojson.Feature(
            geometry={
                "type": "Polygon",
                "coordinates": []
            },
            properties={
                "externalId": ex_id,
                "name": focusName,
                "externalParentId": ex_parent_id,
                "geographicLevel": geo_level
            },
        )
        feats.append(feat)
    for _, row in gdf.iterrows():
        feat = geojson.Feature(
            geometry={
                "type": "Polygon",
                "coordinates": [[[x,y] for x,y in row['geometry'].exterior.coords]]
            },
            properties={
                "externalId": row['externalId'],
                "name": row['name'],
                "externalParentId": row['externalParentId'],
                "geographicLevel": int(row['geographicLevel'])
            },
        )
        feats.append(feat)
    return gpd.GeoDataFrame.from_features(feats, crs="EPSG:4326")
    # coll = geojson.FeatureCollection(feats)
    # saveFile = os.path.join(os.getcwd(),'validation_combined.geojson')
    # with open(saveFile, 'w', encoding='utf8') as f:
    #     geojson.dump(coll, f, ensure_ascii=False)
    # print(f'Saved geojson with needed empty hierarchy members to {saveFile}')


def check_hierarchy(gdf, rgdf):

    print('Checking for gaps in the hierarchy...')

    # Get hierarchy needs
    justFoci = gdf.loc[(gdf.geographicLevel.astype(str) == '5')]
    uniqueProvs = justFoci.externalId.astype(str).str.slice(0,2).unique()
    uniqueDists = justFoci.externalId.astype(str).str.slice(0,4).unique()
    uniqueSubDists = justFoci.externalId.astype(str).str.slice(0,6).unique()
    uniqueVills = justFoci.externalId.astype(str).str.slice(0,8).unique()

    # Create an empty list for missing externalIds in reveal and overall
    revealMissing = []
    missing = []

    # Check them against what is in Reveal
    for uni in [uniqueProvs, uniqueDists, uniqueSubDists, uniqueVills]:
        for v in uni:
            if len(rgdf.loc[pd.to_numeric(rgdf.externalId) == pd.to_numeric(v)]) == 0: revealMissing.append(v)

    # If not in Reveal, check against itself as well
    for v in revealMissing:
        if len(gdf.loc[pd.to_numeric(gdf.externalId) == pd.to_numeric(v)]) == 0: missing.append(v)

    # Print the number of missing hierarcy members by category and list all the members
    if len(missing) == 0:
        print('No missing hierarchy members!')
        return gdf
    else:
        print(f'There are {len(missing)} missing hierarchy members: {", ".join(missing)}. Do you want to automatically generate empty geography for these members?')
        addMissing = None
        while addMissing is None:
            addMissing = input('(y/n): ')
            if addMissing not in ['y','n']:
                print(f'"{addMissing}" is not a valid option, please type either "y" or "n"')
                addMissing = None
        if addMissing == 'y':
            newGdf = add_missing_hierarchy_members(gdf, missing)
            return newGdf
        else:
            raise TypeError(f'Please add in the following hierarchy members manually before trying again: {", ".join(missing)}')



def check_overlaps(gdf, rgdf):
    print('\nChecking for self overlaps...' if gdf.equals(rgdf) else 'Checking for overlaps with current Reveal foci...')

    # Filter for foci only
    gdf = gdf.loc[gdf['geographicLevel'].astype(str) == "5"]
    rgdf = rgdf.loc[rgdf['geographicLevel'].astype(str) == "5"]

    # Check for self intersections on the imported GeoJSON gdf and raise an error if there are any
    invalidPolyIDs = [gdf['externalId'].iloc[i] for i,v in enumerate(gdf['geometry']) if not v.is_valid]
    if len(invalidPolyIDs) > 0:
        raise shapely.geos.TopologicalError(f"The following subvillages are not valid polygons, check for overlaps: {invalidPolyIDs}")

    # Create an rtree index for Reveal geometry
    index = rtree.index.Index()
    for i,g in enumerate(rgdf['geometry']):
        index.insert(i,g.bounds)

    # Create a list of foci with overlaps
    overlaps = []
    overlappedFoci = []

    # Loop over the geometry in the GeoJSON gdf
    for ind,geo in enumerate(gdf['geometry'].tolist()):

        # Buffer any points, if necessary, get externalId
        if geo.geom_type == "Point": geo = geo.buffer(25)
        geoID = gdf['externalId'].iloc[ind]

        # Get ids of any potential rtree intersections
        potentialIntersectionIndexes = [i for i in index.intersection(geo.bounds)]

        # Create an empty list for overlapping foci
        overlappingFoci = []

        # Loop through the potential intersections to see if they're real
        for intersectInd in potentialIntersectionIndexes:

            # Get some needed variables
            indexGeo = rgdf['geometry'].iloc[intersectInd]
            indexType = indexGeo.geom_type
            indexID = rgdf['externalId'].iloc[intersectInd]

            # If subvillage IDs don't match, there's an intersection, and it's
            # not simply the touching of boundaries, add to overlappingFoci
            if geoID != indexID and geo.intersects(indexGeo) and not geo.touches(indexGeo):
                overlappingFoci.append(indexID)

        # If there are overlapping foci and the geoID hasn't been involved in
        # any previous overlap entries, add it to the overlaps list
        if len(overlappingFoci) > 0 and geoID not in overlappedFoci:
            overlappedFoci.extend(overlappingFoci)
            centroid = gdf.iloc[ind]['geometry'].centroid
            overlaps.append({"externalId": geoID, "overlaps": overlappingFoci, "focusCentroid": (centroid.y, centroid.x)})

    overlaps.sort(key=lambda x: x['externalId'])
    singleLineOverlaps = "\n".join(map(str, overlaps))
    print("No overlaps!") if len(overlaps) == 0 else print(f'Fix the following {len(overlaps)} overlapping foci: \n{singleLineOverlaps}')
    print()

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


def send_to_reveal(token, feat, index, numFeats, action, baseUrl, server):
    if action == 'edit':
        url = f'{baseUrl}/opensrp/rest/location?is_jurisdiction=true'
        status = api_put_request(url, token, feat[0])
    elif action == 'upload':
        url = f'{baseUrl}/opensrp/rest/location/add?is_jurisdiction=true'
        status = api_post_request(url, token, feat)

    if status == 201:
        print(f'{index + 1}/{numFeats}: Jurisdiction {action}ed successfully on the {server} server for externalId {feat[0]["properties"]["externalId"]}. Status code: {status}')
    else:
        print(f'{index + 1}/{numFeats}: There was an issue {action}ing the new jursidiction on the {server} server for externalId {feat[0]["properties"]["externalId"]}. Status code: {status}')



def get_action_and_foci():
    print('\n***************************************************************')
    print('Script to pull foci from bvbdosm and upload them into Reveal')
    print('***************************************************************\n')
    print('To get started, what would you like to do?')
    print('    1. Upload new foci')
    print('    2. Edit existing foci')
    action = None
    while action is None:
        action = input('(upload/edit): ')
        if action not in ['upload', 'edit']:
            print(f'"{action}" is not a valid option, please type either "upload" or "edit" to start')
            action = None

    print(f'\nWhich foci would you like to {action}? Please enter as a comma seperated list')
    fociIds = None
    while fociIds is None:

        fociIds = input('(xxxxxxxxxx, xxxxxxxxxx...): ')
        fociIdList = re.findall(r"(\d{10})(?:,|$)",fociIds)
        if len(fociIdList) != len(fociIds.split(',')) or len(set(fociIdList)) != len(fociIds.split(',')):
            print(f'Length of foci found and number of items in the list do not match, please check the formatting and remove duplicates')
            fociIds = None
        else:
            fociIds = [int(x) for x in fociIdList]

    return action, fociIds


def get_reveal_gdf(config, action, fociIds, token):
    print('\nShould an updated version of the Reveal hierarchy be downloaded for comparison?')
    getRevealJurisdictions = None
    while getRevealJurisdictions is None:
        getRevealJurisdictions = input('(y/n): ')
        if getRevealJurisdictions not in ['y','n']:
            print(f'"{getRevealJurisdictions}" is not a valid option, please type either "y" or "n"')
            getRevealJurisdictions = None

    jurisdictionFile = './reveal_features_local.geojson'
    if getRevealJurisdictions == 'y':
        jurisdictionFile = download_reveal_jurisdictions(token)

    print(f'\nLoading Reveal hierarchy ({jurisdictionFile}) into memory...')
    rgdf = gpd.read_file(jurisdictionFile)

    # Remove those without an externalId, convert externalId to the correct dtype, and print the details and return the gdf
    rgdf = rgdf.dropna(subset=['externalId'])
    rgdf['externalId'] = rgdf['externalId'].astype(np.int64)
    print_hierarchy_details(rgdf, 'Reveal jurisdictions')

    # Check whether the foci exist in reveal (if edit) or don't exist (if create)
    inReveal = []
    notInReveal = []
    for focusId in fociIds:
        if focusId not in rgdf.externalId.values:
            notInReveal.append(focusId)
        else:
            inReveal.append(focusId)
    if action == 'edit' and len(notInReveal) > 0:
        raise ValueError(f'The following foci are not in Reveal yet - did you mean to upload them instead? ({", ".join([str(x) for x in notInReveal])})')
    elif action == 'upload' and len(inReveal) > 0:
        raise ValueError(f'The following foci are already in Reveal - did you mean to edit them instead? ({", ".join([str(x) for x in inReveal])})')
    else:
        return rgdf

def push_changes_to_reveal(config, osmGdf, action, token):
    pd.set_option('display.max_rows', osmGdf.shape[0]+1)
    print(osmGdf[['name', 'externalId', 'geographicLevel', 'externalParentId']])
    print(f'Are you sure you want to push the {osmGdf.shape[0]} {action.upper()}{"S" if osmGdf.shape[0] > 1 else ""} above to Reveal?')
    push = None
    while push == None:
        push = input('(y/n): ')
        if push not in ['y','n']:
            print(f'{push} is not a valid option, please type either "y" or "n"')
            push = None

    if push == 'y':

        baseUrl = config['local_reveal']['base_url']

        # Save the files so they're uploaded in the correct order for new foci and save temp file so
        # it can be imported as GeoJSON
        sorted = osmGdf.sort_values(by=['geographicLevel']).reset_index(drop=True)
        geojsonLoc = os.path.join(tempfile.gettempdir(), 'sorted.geojson')
        sorted.to_file(geojsonLoc, driver='GeoJSON')

        # Load the GeoJSON file
        with open(geojsonLoc, encoding='utf8') as f:
            data = geojson.load(f)

        # Get the length of the features to be uploaded
        numFeats = len(data['features'])

        # Loop through the features in the GeoJSON file - note that they should be sorted by geographicLevel
        for index, feat in enumerate(data['features']):

            # Switch on the type of operation
            reveal_feature = []
            if action == 'upload':
                reveal_feature = create_reveal_feature(token, feat, baseUrl)
            elif action == 'edit':
                reveal_feature = update_reveal_feature_geometry(token, feat, baseUrl)

            # print(reveal_feature)

            # Send the new location and check the status
            send_to_reveal(token, reveal_feature, index, numFeats, action, baseUrl, 'local')

            # Sleep to prevent overloading the server
            # sleep(5)

        # Clean up temp file
        os.remove(geojsonLoc)

    else:
        print('No foci pushed to Reveal')

def main():

    # Get the config details from config.ini to be used throughout
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)),'config.ini'))

    # Get the token for downloading the Reveal hierarchy and uploading the edits/new foci
    token = get_oauth_token(config, 'local')

    # Get user input on actions and user foci
    action, fociIds = get_action_and_foci()

    # Get the filtered features from bvbdosm
    osmGdf = get_osm_features(config, action, fociIds)

    # Get the entire hierarachy from Reveal
    rgdf = get_reveal_gdf(config, action, fociIds, token)

    # Check for size. min_area equal to the size of buffered b1b2 foci, max area is 5km x 5km
    check_size(osmGdf, math.pi * 25 ** 2, 5000 * 5000)

    # Check hierarchy - replace the osm GDF with the new one if new hierarchy members were added
    osmGdf = check_hierarchy(osmGdf,rgdf)

    # Check self overlaps
    check_overlaps(osmGdf,osmGdf)

    # TODO: Filter Reveal geometry to remove any geometry that is going to be changed - i.e., updating boundaries
    # Check Reveal overlaps
    check_overlaps(osmGdf,rgdf)

    # Upload the changes
    push_changes_to_reveal(config, osmGdf, action, token)

    # TODO: add details of the uploads to an Excel file? Or a folder with .geojson files?s


if __name__ == '__main__':
    main()
