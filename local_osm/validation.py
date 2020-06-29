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

'''
Validate on:
1. Overlap
    - Check in GeoJSON first, then check against Reveal?
    - Option to move B1B2?
2. Size
    - Limits set by user?
3. Hierarchy check?
'''

def load_and_validate_geojson(gjsonLoc):

    # Get the GeoJSON from the file specified on the command line
    gdf = gpd.read_file(gjsonLoc)

    # Validate that the geojson includes the necessary fields
    for i in ['name','externalId','externalParentId','geographicLevel']:
        if i not in list(gdf):
            raise TypeError(f'The input GeoJSON is missing the {i} column')

    # Return the GeoDataFrame
    return gdf


def get_reveal_gdf():

    # Get the location of the where the jurisdiction.csv file will be stored - local folder
    jurisdictionFile = os.path.join(os.path.dirname(os.path.realpath(__file__)), "jurisdictions.csv")

    # Remove the jurisdictions file if it's already there
    if os.path.exists(jurisdictionFile):
        os.remove(jurisdictionFile)

    # Get the private key location and create RSAKey, note that this will need to be changed for other computers
    priv_key_loc = "C:\\Users\\elija\\AppData\\Local\\Packages\\CanonicalGroupLimited.UbuntuonWindows_79rhkp1fndgsc\\LocalState\\rootfs\\home\\ubuntu\\.ssh\\id_rsa"
    k = paramiko.RSAKey.from_private_key_file(priv_key_loc)

    # Create a ssh client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Connect to the staging server
    ssh.connect(hostname="134.209.198.201", username="root", pkey=k)

    # Run the get_csv_thailand.sh script to get the most recent data from Reveal
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("cd location-scripts/reveal_upload && ./get_csv_thailand.sh")

    # While loop until the command is completed
    while not ssh_stdout.channel.exit_status_ready():
        continue
    # [print(line,end="") for line in iter(ssh_stdout.readline, "")]
    # [print(line,end="") for line in iter(ssh_stderr.readline, "")]

    # Track scp download progress
    def progress(filename, size, sent):
        sys.stdout.write("%s\'s progress: %.2f%%   \r" % (filename, float(sent)/float(size)*100) )

    # Get the SCPClient
    scp = SCPClient(ssh.get_transport(), progress=progress)

    # Get the jurisdictions.csv and print a new line
    scp.get('/root/location-scripts/reveal_upload/toimport/locations/jurisdictions.csv')
    sys.stdout.write('\n')

    # Close both the scp and ssh connections
    scp.close()
    ssh.close()

    # Convert CSV coodinates to GeoJSON polygons
    def get_polygon(row):
        feat = geojson.Feature(geometry={
                "type": "Polygon",
                "coordinates": json.loads(row.coordinates)})
        return shapely.geometry.Polygon(list(geojson.utils.coords(feat)))

    # Load the CSV file
    revealCSV = pd.read_csv(jurisdictionFile, delimiter='|')

    # Create the geom
    geom = revealCSV.apply(lambda row: get_polygon(row),axis=1)

    # Create the GeoDataFrame
    rgdf = gpd.GeoDataFrame(revealCSV, crs="EPSG:4326", geometry=geom)

    # return the GDF with renamed columns
    return rgdf.rename({'externalid': 'externalId', 'parentid': 'parentId', 'geographiclevel': 'geographicLevel'}, axis=1)


def check_size(gdf, min_area, max_area):

    # Convert the CRS to measure size in meters
    if gdf.crs is not "EPSG:3857": gdf = gdf.to_crs("EPSG:3857")

    # Filter out any foci that meet the criteria
    fociToCheck = gdf.loc[(gdf['geometry'].area <= min_area) | (gdf['geometry'].area >= max_area)].externalId
    print("Foci sizes okay!") if len(fociToCheck) == 0 else print(f'Check the size of the following {len(fociToCheck)} foci: {[i for i in fociToCheck]}')


def check_hierarchy(gdf, rgdf):

    # Get hierarchy needs
    justFoci = gdf.loc[gdf.geographicLevel == '5']
    uniqueProvs = justFoci.externalId.str.slice(0,2).unique()
    uniqueDists = justFoci.externalId.str.slice(0,4).unique()
    uniqueSubDists = justFoci.externalId.str.slice(0,6).unique()
    uniqueVills = justFoci.externalId.str.slice(0,8).unique()

    # Create an empty list for missing externalIds
    missing = []

    # Check them against what is in Reveal
    for uni in [uniqueProvs,uniqueDists, uniqueSubDists, uniqueVills]:
        for v in uni:
            if len(rgdf.loc[rgdf.externalId.astype(str) == v]) == 0: missing.append(v)

    # If not in Reveal, check against itself as well
    for v in missing:
        if len(gdf.loc[gdf.externalId == v]) > 0: missing.remove(v)

    # Return a list of missing hierarchy IDs
    print("No missing hierarchy members!") if len(missing) == 0 else print(f"Missing {len(missing)} hierarchy members: {missing}")


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
            overlaps.append({"externalId": geoID, "overlaps": overlappingFoci})

    print("No overlaps!") if len(overlaps) == 0 else print(f"Fix the following {len(overlaps)} overlapping foci: {overlaps}")


def valid_path(gjson):
    if os.path.isfile(gjson):
        return gjson
    else:
        raise argparse.ArgumentTypeError(f'{gjson} is not a valid file')


def main():
    parser = argparse.ArgumentParser(description="Script to validate GeoJSON foci before uploading to Reveal")
    parser.add_argument('geojson', type=valid_path, help="GeoJSON file to validate")
    args = parser.parse_args()

    # Load GeoJSON
    print('Loading the supplied GeoJSON...')
    gdf = load_and_validate_geojson(args.geojson)

    # Get a GeoDataFrame of what is currently in Reveal
    print('Loading the current Reveal foci...')
    rgdf = get_reveal_gdf()

    # Check for size. min_area equal to the size of buffered b1b2 foci, max area is 5km x 5km
    min_area = math.pi * 25 ** 2
    max_area = 5000 * 5000
    print(f'Checking foci that are smaller than {round(min_area)}m^2 or larger than {round(max_area/1000000)}km^2...')
    check_size(gdf, min_area, max_area)

    # Check hierarchy
    print('Checking for gaps in the hierarchy...')
    check_hierarchy(gdf,rgdf)

    # Check self overlaps
    print('Checking for self overlaps...')
    check_overlaps(gdf,gdf)

    # Check Reveal overlaps
    print('Checking for overlaps with current Reveal foci...')
    check_overlaps(gdf,rgdf)


if __name__ == '__main__':
    main()
