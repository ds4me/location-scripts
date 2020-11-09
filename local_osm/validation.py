import argparse
import paramiko
from scp import SCPClient
import os
import sys
import geopandas as gpd
import math
import rtree
import shapely
import pandas as pd
import json
import geojson
import numpy as np

import requests
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import LegacyApplicationClient

# def check_nulls_and_line_endings(gdf, colName):
#     # Get a list of null values for the column
#     nulls = gdf.loc[gdf[colName].isnull()]
#
#     # If there are nulls, print them and thrown an error
#     if not nulls.empty:
#         print(f'The {colName} column contains the following null values:')
#         print(nulls)
#         raise TypeError(f'The {colName} cannot contain null values')
#
#     # Otherwise, remove line endings for the name and convert numeric columns
#     gdf[colName] = gdf[colName].str.strip() if colName == 'name' else pd.to_numeric(gdf[colName])
#
#     # Return updated GeoDataFrame
#     return gdf
#
#
# def name_check(gdf):
#     # Check if the name column exists already, if not raise an error
#     if 'name' not in list(gdf):
#         raise TypeError(f'The input GeoJSON must contain a "name" column')
#     else:
#         # Otherwise, check for nulls and remove line endings
#         gdf = check_nulls_and_line_endings(gdf,'name')
#
#     # Return updated GeoDataFrame
#     return gdf
#
#
# def external_id_check(gdf):
#     # Check for externalId and description columns
#     if 'externalId' not in list(gdf) and 'description' not in list(gdf):
#         TypeError('The input GeoJSON must contain an "externalId" or "description" column')
#
#     # Change col name from description to externalId
#     if 'externalId' not in list(gdf) and 'description' in list(gdf):
#         gdf = gdf.rename(columns={'description': 'externalId'})
#
#     # Check for null values and fix line endings
#     gdf = check_nulls_and_line_endings(gdf,'externalId')
#
#     # Return updated GeoDataFrame
#     return gdf
#
#
# def external_parent_id_check(gdf):
#
#     # Add an externalParentId, as needed
#     if 'externalParentId' not in list(gdf):
#         gdf['externalParentId'] = gdf['externalId'] // 100
#     else:
#         # Otherwise check for nulls and fix line endings
#         gdf = check_nulls_and_line_endings(gdf, 'externalParentId')
#
#     # Return updated GeoDataFrame
#     return gdf
#
#
# def geographic_level_check(gdf):
#
#     # Add a geographicLevel column, if necessary
#     if 'geographicLevel' not in list(gdf):
#         gdf['geographicLevel'] = 5
#     else:
#         # Otherwise check for nulls and fix line endings
#         gdf = check_nulls_and_line_endings(gdf, 'geographicLevel')
#
#     # Return updated GeoDataFrame
#     return gdf

def print_hierarchy_details(gdf, name):
    print(f'Loaded {name} with {len(gdf)} features:')
    print(f'{len(gdf.loc[gdf.externalId.astype(str).str.len() == 2])} provinces')
    print(f'{len(gdf.loc[gdf.externalId.astype(str).str.len() == 4])} districts')
    print(f'{len(gdf.loc[gdf.externalId.astype(str).str.len() == 6])} sub-districts')
    print(f'{len(gdf.loc[gdf.externalId.astype(str).str.len() == 8])} villages')
    print(f'{len(gdf.loc[gdf.externalId.astype(str).str.len() == 10])} foci')

def load_and_validate_geojson(gjsonLoc):

    # Get the GeoJSON from the file specified on the command line
    gdf = gpd.read_file(gjsonLoc)

    # # Check for a name column
    # gdf = name_check(gdf)
    #
    # # Check the externalId/description column
    # gdf = external_id_check(gdf)
    #
    # # Check the externalParentId column
    # gdf = external_parent_id_check(gdf)
    #
    # # Check the geographicLevel column
    # gdf = geographic_level_check(gdf)

    # Check that the columns are correct, there are no null values, and that there
    # aren't any random newline characters in the fields
    missingCols = []
    colsWithNulls = []
    colsWithNewLineIssues = []
    for i in ['name','externalId','externalParentId','geographicLevel']:
        if i not in list(gdf):
            missingCols.append(i)
        else:
            if not gdf.loc[gdf[i].isnull()].empty:
                colsWithNulls.append(i)
            if gdf[i].astype(str).str.contains('\n').any():
                colsWithNewLineIssues.append(i)

    # Raise errors if any of the above situations exist
    if missingCols:
        raise TypeError(f'The input GeoJSON must include the following missing columns: {", ".join(missingCols)}')
    if colsWithNulls:
        raise TypeError(f'Some values in the following columns are null: {", ".join(colsWithNulls)}')
    if colsWithNewLineIssues:
        raise TypeError(f'Some values in the following columns include newline characters: {", ".join(colsWithNewLineIssues)}')

    # Return the GeoDataFrame
    print_hierarchy_details(gdf, 'geojson')
    return gdf


def download_reveal_jurisdictions():
    # Get the location of the where the jurisdiction.csv file will be stored - local folder
    jurisdictionFile = os.path.join(os.path.dirname(os.path.realpath(__file__)), "jurisdictions.csv")

    # Remove the jurisdictions file if it's already there
    if os.path.exists(jurisdictionFile):
        os.remove(jurisdictionFile)

    # Get the private key location and create RSAKey, note that this will need to be changed for other computers
    # pkey_loc = "C:\\Users\\elija\\AppData\\Local\\Packages\\CanonicalGroupLimited.UbuntuonWindows_79rhkp1fndgsc\\LocalState\\rootfs\\home\\ubuntu\\.ssh\\id_rsa"
    pkey_loc = "C:/Users/efilip/.ssh/thai_upload"
    pkey = paramiko.RSAKey.from_private_key_file(pkey_loc)

    # Create a ssh client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Connect to the staging server
    ssh.connect(hostname="134.209.198.201", username="root", pkey=pkey)

    # Run the get_csv_thailand.sh script to get the most recent data from Reveal
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("cd location-scripts/reveal_upload && ./get_csv_thailand.sh")

    # While loop until the command is completed
    while not ssh_stdout.channel.exit_status_ready():
        continue
    # [print(line,end="") for line in iter(ssh_stdout.readline, "")]
    # [print(line,end="") for line in iter(ssh_stderr.readline, "")]

    # Track scp download progress and get the filename
    def progress(filename, size, sent):
        progressString = f'{filename}\'s progress: {round(100 * (sent/size), 1)}%'
        print(progressString, end="\r", flush=False)

    # Get the SCPClient
    scp = SCPClient(ssh.get_transport(), progress=progress)

    # Get the jurisdictions.csv and print a new line
    scp.get('/root/location-scripts/reveal_upload/toimport/location/th-pr/jurisdictions.csv', jurisdictionFile)
    print(' ')

    # Close both the scp and ssh connections
    scp.close()
    ssh.close()

    # return the location of the downloaded file
    return jurisdictionFile






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


def get_locations_from_api():
    token = get_oauth_token()
    url = 'https://servermhealth.ddc.moph.go.th/opensrp/rest/location/getAll?is_jurisdiction=true&serverVersion=0&limit=50000&return_geometry=true'
    res = api_get_request(url, token)




def get_reveal_gdf(jurisdictionFile):

    if not jurisdictionFile:
        jurisdictionFile = download_reveal_jurisdictions()

    # Convert CSV coodinates to GeoJSON polygons
    def get_polygon(row):
        feat = geojson.Feature(geometry={
                # "type": "Polygon",
                "type": row['geometry.type'],
                "coordinates": row['geometry.coordinates']})
                # "coordinates": json.loads(row.coordinates)})
        return shapely.geometry.Polygon(list(geojson.utils.coords(feat)))


    # revealCSV = get_locations_from_api()
    # feats = []
    # for f in revealCSV:
    #     feats.append(geojson.Feature(f))
    # coll = geojson.FeatureCollection(feats)
    # print(coll)

    # j = pd.read_json(jurisdictionFile)
    with open(jurisdictionFile, encoding='utf8') as f:
        data = json.load(f)
    j = pd.json_normalize(data)
    # j.to_excel("C:/users/efilip/desktop/testing.xlsx")
    geom = j.apply(lambda row: get_polygon(row), axis=1)

    # j.to_excel('C:/users/efilip/desktop/testing.xlsx')



    # Load the CSV file
    # revealCSV = pd.read_csv(jurisdictionFile, delimiter='|')

    # Create the geom
    # geom = revealCSV.apply(lambda row: get_polygon(row),axis=1)

    # Create the GeoDataFrame
    # rgdf = gpd.GeoDataFrame(revealCSV, crs="EPSG:4326", geometry=geom)
    rgdf = gpd.GeoDataFrame(j, crs="EPSG:4326", geometry=geom)
    rgdf.to_excel('C:/Users/efilip/desktop/testing.xlsx')

    # rename the columns
    # rgdf = rgdf.rename({'externalid': 'externalId', 'parentid': 'parentId', 'geographiclevel': 'geographicLevel'}, axis=1)
    rgdf = rgdf.rename({'properties.externalId': 'externalId', 'properties.parentId': 'parentId', 'properties.geographicLevel': 'geographicLevel'}, axis=1)

    # Remove those without an externalId
    rgdf = rgdf.dropna(subset=['externalId'])

    # Convert externalId to the correct dtype
    rgdf['externalId'] = rgdf['externalId'].astype(np.int64)

    # print the details and return the gdf
    print_hierarchy_details(rgdf, 'Reveal jurisdictions')
    return rgdf


def check_size(gdf, min_area, max_area):

    # Convert the CRS to measure size in meters
    if gdf.crs is not "EPSG:3857": gdf = gdf.to_crs("EPSG:3857")

    # Get just the foci
    justFoci = gdf.loc[(gdf.geographicLevel.astype(str) == '5')]

    # Filter out any foci that meet the criteria
    smallFoci = justFoci.loc[justFoci['geometry'].area <= min_area].externalId
    largeFoci = justFoci.loc[justFoci['geometry'].area >= max_area].externalId

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

def print_missing_hierarchy_members(list,type):
    if len(list):
        list.sort()
        singleLineVals = "\n".join(map(str,list))
        print(f'Missing {len(list)} {type}: \n{list}')

def check_hierarchy(gdf, rgdf):

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
    else:
        print_missing_hierarchy_members([x for x in missing if len(str(x)) == 2], 'provinces')
        print_missing_hierarchy_members([x for x in missing if len(str(x)) == 4], 'districts')
        print_missing_hierarchy_members([x for x in missing if len(str(x)) == 6], 'sub-districts')
        print_missing_hierarchy_members([x for x in missing if len(str(x)) == 8], 'villages')


def check_overlaps(gdf, rgdf):

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


def valid_geojson(gjson):
    if os.path.isfile(gjson):
        if gjson.endswith('.geojson'):
            return gjson
        else:
            raise argparse.ArgumentTypeError(f'File must end with .geojson')
    else:
        raise argparse.ArgumentTypeError(f'{gjson} is not a valid file')


def valid_csv(csv):
    if os.path.isfile(csv):
        return csv
        # if csv.endswith('.csv'):
        #     return csv
        # else:
            # raise argparse.ArgumentTypeError(f'File must end with .csv')
    else:
        raise argparse.ArgumentTypeError(f'{csv} is not a valid file')


def main():
    parser = argparse.ArgumentParser(description="Script to validate GeoJSON foci before uploading to Reveal")
    parser.add_argument('geojson', type=valid_geojson, help="GeoJSON file to validate")
    parser.add_argument('-j','--jurisdictions', dest="jurisdictionFile", type=valid_csv, help="Optional file location for pre-downloaded jurisdictions file")
    args = parser.parse_args()

    # Load GeoJSON
    print('Loading the supplied GeoJSON...')
    gdf = load_and_validate_geojson(args.geojson)
    print('')

    # Get a GeoDataFrame of what is currently in Reveal
    print('Loading the current Reveal foci...')
    rgdf = get_reveal_gdf(args.jurisdictionFile)
    print('')

    # Check for size. min_area equal to the size of buffered b1b2 foci, max area is 5km x 5km
    min_area = math.pi * 25 ** 2
    max_area = 5000 * 5000
    print(f'Checking foci that are smaller than {round(min_area)}m^2 or larger than {round(max_area/1000000)}km^2...')
    check_size(gdf, min_area, max_area)
    print('')

    # Check hierarchy
    print('Checking for gaps in the hierarchy...')
    check_hierarchy(gdf,rgdf)
    print('')

    # Check self overlaps
    print('Checking for self overlaps...')
    check_overlaps(gdf,gdf)
    print('')

    # Check Reveal overlaps
    print('Checking for overlaps with current Reveal foci...')
    check_overlaps(gdf,rgdf)
    print('')


if __name__ == '__main__':
    main()
