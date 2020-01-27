
# Outside file locations and global variables
THAI_DISTRICT_LOCATIONS = "C:/Users/MM-PC0L6BTX/Box/GMAL Region Asia/Malaria Thailand/Reveal Pilot/Data/Thai Boundary/District77_region.shp"
MASTERLIST_LOCATION = "C:/Users/MM-PC0L6BTX/Box/Elijah Filip/Countries/Thailand/Reveal/Reveal Pilot Masterlist.xlsx"
SAVE_FILE_LOCATION = "C:/Users/MM-PC0L6BTX/Desktop/overlap"
PROJECTED_CRS = "EPSG:32647"
ORIGINAL_CRS = "EPSG:4326"


import overpass
from shapely.geometry import Polygon, Point
from shapely.strtree import STRtree
import pandas as pd
import geopandas as gpd
import numpy as np
import os
import sys

def printToSameLine(string, percentComplete):
    end = '\n' if percentComplete == 1 else ''
    print('\r' + string, end=end)


def getA1A2Foci(sourceRegex):

    # Print what is going on
    print(f'Extracting A1A2 foci from OSM with the source tag {sourceRegex} and a description tag...')

    # Create the query and api
    op_query = f'area["name:en"="Thailand"]->.a;way["source"~"{sourceRegex}"]["description"~"."](area.a);(._;>;)'
    op_api = overpass.API(endpoint="https://lz4.overpass-api.de/api/interpreter", timeout=600)

    # Get the response from Overpass
    r = op_api.get(op_query, verbosity='geom', responseformat='geojson')

    # Fliter just the LineStrings to get the polygons
    ways = [f for f in r.features if f.geometry['type'] == "LineString"]

    # Create a list of polygons
    polygons = [Polygon(w.geometry.coordinates) for w in ways]

    # Create a list of properties for each polygon
    properties = [{'id': x.id, 'description': int(x.properties.get('description').strip()), 'landuse': x.properties.get('landuse'), 'name': x.properties.get('name'), 'source': x.properties.get('source')} for x in ways]

    # Add the polygons and properties to a GeoDataFrame and change the CRS to match the project
    a1a2_osm = gpd.GeoDataFrame(properties, geometry=polygons, crs=ORIGINAL_CRS)
    a1a2_osm = a1a2_osm.to_crs(PROJECTED_CRS)

    # Save the file
    # a1a2_osm.to_file(os.path.join(SAVE_FILE_LOCATION,"foci.geojson"), driver='GeoJSON', encoding='utf-8')

    # Import the file
    # a1a2_osm = gpd.read_file(os.path.join(SAVE_FILE_LOCATION, "foci.geojson"))

    # Print the number of foci extracted and return the GDF
    print(f'{len(a1a2_osm)} A1A2 foci imported from OSM\n')
    return a1a2_osm


def a1a2Overlap(a1a2_osm):

    # Print what is going on
    print('Checking to see if A1A2 boundries overlap...')

    # Create an empty list and loop through the geometry
    overlaps = []
    for i,p in enumerate(a1a2_osm['geometry']):

        # Loop through the geometry again
        for ind,poly in enumerate(a1a2_osm['geometry']):

            # If the indexes don't match...
            if i != ind:

                # Check to see if any part of the polygon fall within another polygons bounds
                if p.within(poly):

                    # If they do, append the ids to the overlaps list
                    overlaps.append([a1a2_osm.iloc[i].id, a1a2_osm.iloc[ind].id])

    # If there are any overlaps, alert the user and print the overlaps
    if len(overlaps) > 0:
        print(f'There are approximately {len(overlaps)/2} polygons, fix the items in OSM below before proceeding')
        print(overlaps)
        return True
    else:
        print('No A1A2 boundaries overlap!\n')
        return False


def linkA1A2FociToMasterlist(a1a2_osm, masterlist):

    # Print what is happening
    print('Linking masterlist and OSM export...')

    # Filter the masterlist for only A1A2 foci
    a = masterlist[masterlist.area_code_2020.isin(["A1", "A2"])]

    # Alert the user if the numbers do not match between lists -  note they don't need to match
    if len(a1a2_osm) != len(a):

        # Print a warning and stop execution
        sys.exit(f'Error: Number of OSM A1A2 foci ({len(a1a2_osm)}) do not match the number of masterlist A1A2 foci ({len(a)})!')

    else:

        # Ensure the same data type and Join the lists on the OSM description tag
        a1a2 = a.join(a1a2_osm.set_index('description'), on="VILLAGE_ID", rsuffix='_osm')

        # Clean up the DataFrame to just the minimum required data
        a1a2 = cleanDataFrameForExport(a1a2,'a')

        # Return the GDF
        print(f'All {len(a1a2_osm)} foci successfully linked to the masterlist\n')
        return a1a2


def getB1B2Foci(masterlist):

    # Print what is happening
    print('Extracting B1B2 foci from the masterlist...')

    # Filter for b1b2 foci only
    b = masterlist[masterlist.area_code_2020.isin(["B1","B2"])]

    # Filter out points missing lat long information and notify the user of the number filtered out
    b1b2 = b.loc[b['lat_long'].str.len() > 0]
    print(f'Filtered out {len(b) - len(b1b2)} foci that {"was" if len(b) - len(b1b2) else "were"} missing lat long info')

    # Split the lat_long column by comma, creating two new columns
    b1b2 = b1b2.assign(lat=b1b2["lat_long"].str.split(',', expand=True)[0].astype(float))
    b1b2 = b1b2.assign(long=b1b2["lat_long"].str.split(',', expand=True)[1].astype(float))

    # Create points from the new columns
    points = [Point(x,y) for x,y in zip(b1b2['long'], b1b2['lat'])]

    # Put the information into a GeoDataFrame and convert to the project CRS
    b1b2 = gpd.GeoDataFrame(b1b2, geometry=points, crs=ORIGINAL_CRS)
    b1b2 = b1b2.to_crs(PROJECTED_CRS)

    # Print what happened and return the GDF
    print(f'{len(b1b2)} B1B2 foci imported from the masterlist\n')
    return b1b2


def createAndFilterPoints(boundary, points, bbox_size, step_size, a1a2, b1b2):

    # Get the geometry of the first point in the list and create a bounding box
    point = points['geometry'].iloc[0]
    min_x = point.x - bbox_size / 2
    max_x = point.x + bbox_size / 2
    min_y = point.y - bbox_size / 2
    max_y = point.y + bbox_size / 2

    # Create points spaced step_size apart across the bounding box
    potential_points = []
    for x in np.arange(min_x, max_x, step_size):
        for y in np.arange(min_y, max_y, step_size):
            potential_points.append(Point(x,y))

    # Get a GeoDataFrame of all the created points
    all_points = gpd.GeoDataFrame(geometry=potential_points, crs="EPSG:32647")

    # Clip the created points to just those within the district
    boundary_points = all_points[all_points.buffer(25).within(boundary.unary_union)]

    # Get the A1A2 foci in the district and filter out points whose buffers intersect with A1A2 foci
    boundary_a1a2 = a1a2[a1a2.intersects(boundary.unary_union)]
    boundary_points_no_a1a2 = boundary_points[~boundary_points.buffer(25).intersects(boundary_a1a2.unary_union)]

    # Get the B1B2 foci in the district and filter out points whose buffers intersect with their buffers
    boundary_b1b2 = b1b2[b1b2.buffer(25).intersects(boundary.unary_union)]
    boundary_points_no_a1a2_b1b2 = boundary_points_no_a1a2[~boundary_points_no_a1a2.buffer(25).intersects(boundary_b1b2.buffer(25).unary_union)]

    # Find the closest remaining points based on the length of the points provided
    closest_points = boundary_points_no_a1a2_b1b2.distance(point).sort_values()
    closest_points = closest_points[:len(points)]

    # Get a GeoDataFrame of all the closest points selected and return them
    return boundary_points_no_a1a2_b1b2[boundary_points_no_a1a2_b1b2.index.isin(closest_points.index.tolist())]


def b1b2Overlap(a1a2, b1b2):

    # Print that the code is now checking overlaps
    print(f'Checking all {len(b1b2)} B1B2 for overlaps with A1A2 foci and B1B2 buffers...')

    # Get a list of points and village ids
    geom = b1b2['geometry'].tolist()
    ids = b1b2['VILLAGE_ID'].tolist()

    # Get a list of buffered points with their respective ids
    geom_ids = []
    for i,g in enumerate(geom):
        g = g.buffer(25)
        g.name = ids[i]
        geom_ids.append(g)

    # Create an rtree object to traverse
    tree = STRtree(geom_ids)

    # Create empty arrays for the overlaps, the buffers overlapped, and geometry for all a1a2 points
    overlaps = []
    b1b2_sublist = []
    u = a1a2.unary_union

    # Loop over the B1B2 geometry
    for i,g in enumerate(geom_ids):

        # Print what's happening in the loop given how long this takes
        printToSameLine(f'Checking B1B2 foci {i+1} of {len(geom_ids)}', (i+1)/len(geom_ids))

        # Get the B1B2 overlaps by queriying the rtree, ignoring self overlaps
        b1b2_overlaps = [o.name for o in tree.query(g) if o.name != g.name]

        # Get a boolean for whether the b1b2 buffer intersects with an a1a2 foci
        a1a2_overlap = True if g.intersects(u) else False

        # If the sublist doesn't already contain the village_id and either the
        # buffer overlaps at least one B1B2 point or overlaps an A1A2 foci
        if g.name not in b1b2_sublist and (len(b1b2_overlaps) > 0 or a1a2_overlap):

            # Add the overlapping items to the sublist so they can be easily queried
            b1b2_sublist.extend(b1b2_overlaps)

            # Append a dict of the village id, the overlapping B1B2 foci, and whether it overlaps with an A1A2 foci
            overlaps.append({'village_id': g.name, 'b1b2overlaps': b1b2_overlaps, 'a1a2overlap': a1a2_overlap})

    # Sort the list over overlaps by village_id and return it
    print(f'Found {len(overlaps)} groups of foci that need to be edited\n')
    return sorted(overlaps, key=lambda k: k['village_id'])


def getChanges(overlaps, boundaries, a1a2, b1b2):

    # Print what's happening
    print(f'Getting new points for the {len(overlaps)} overlapping B1B2 foci...')

    # Create an empty array for the changesets and empty variables for the boundary
    # to prevent these from being pulled in each loop
    changes = []
    boundary = ""
    boundary_id = ""

    # Loop over the overlaps
    for i,o in enumerate(overlaps):

        # Print where in the loop you are
        printToSameLine(f'Fixing foci group {i+1} of {len(overlaps)}', (i+1)/len(overlaps))

        # Get the required variables from the dict
        village_id = o.get('village_id')
        dist_id = village_id // 1000000
        b1b2_overlaps = o.get('b1b2overlaps')
        a1a2_overlap = o.get('a1a2overlap')

        # Get the boundary, if necessary
        if boundary_id != dist_id:
            boundary_id = dist_id
            boundary = boundaries[boundaries["DIST_CODE"] == dist_id]

        # Create a lambda function to extract a DataFrame of the overlapping points
        get_points = lambda x : b1b2[b1b2["VILLAGE_ID"].isin(x)]

        # Separate points into four categories: Those that overlap both B1B2 and A1A2 foci,
        # those that overlap only B1B2 foci, those that overlap only A1A2 foci, and
        # those that don't overlap any foci
        if len(b1b2_overlaps) > 0:
            if a1a2_overlap:
                # Move all B1B2 foci to the nearest points
                points = get_points([village_id] + b1b2_overlaps)
            else:
                # Move all but the first point to the nearest points
                points = get_points(b1b2_overlaps)
        else:
            if a1a2_overlap:
                # Move single point to nearest point
                points = get_points([village_id])

        # Get a list of the closest points
        closest_points = createAndFilterPoints(boundary, points, 10000, 500, a1a2, b1b2)

        # Loop trough a list of the village ids
        for i,p in enumerate(points["VILLAGE_ID"].tolist()):

            # Get the the closest points in order
            closest_point = closest_points['geometry'].iloc[i]

            # Append changes as a dict to the "changes" list
            changes.append({"village_id": p, "lat": closest_point.y, "long": closest_point.x})

    # Return a DataFrame of the changes
    df = pd.DataFrame(changes)

    # Print the results and return a GeoDataFrame of them
    print(f'Changed the coordinates of {len(df)} foci\n')
    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.long, df.lat))


def cleanDataFrameForExport(gdf, type):

    # Switch on whether a1a2 foci or b1b2 foci are being cleaned
    if type == "a":

        # Get only the relevant columns from the GeoDataFrame
        tmp_gdf = gdf[['VILLAGE_ID', 'VILLAGE_NAME_TH', 'VILLAGE_NAME_EN', 'MU_NAME_TH', 'MU_NAME_EN', 'area_code_2020', 'geometry']]

        # Rename the columns to fit the naming conventions used
        tmp_gdf.columns = ['externalId', 'name', 'name_en', 'parentName', 'parentName_en', 'areaCode2020', 'geometry']

        # Create an externalParentId column
        tmp_gdf = tmp_gdf.assign(externalParentId=lambda x: x['externalId'] // 100)

        # Add the geographic level
        tmp_gdf['geographicLevel'] = "5"

        # Create a final GeoDataFrame with the correct columns
        final_gdf = gpd.GeoDataFrame(tmp_gdf.drop(['geometry'], axis=1), geometry= tmp_gdf['geometry'], crs=PROJECTED_CRS)

    if type == "b":

        # Get only the relevant columns from the GeoDataFrame
        tmp_gdf = gdf[['VILLAGE_ID', 'VILLAGE_NAME_TH', 'VILLAGE_NAME_EN', 'MU_NAME_TH', 'MU_NAME_EN', 'area_code_2020', 'geometry', 'geometry_changes']]

        # Rename the columns to fit the naming conventions used
        tmp_gdf.columns = ['externalId', 'name', 'name_en', 'parentName', 'parentName_en', 'areaCode2020', 'geometry', 'geometry_changes']

        # Create an externalParentId column
        tmp_gdf = tmp_gdf.assign(externalParentId=lambda x: x['externalId'] // 100)

        # Change the items in the geometry column where necessary and create the 25m buffer
        tmp_gdf["geometry"] = tmp_gdf.apply(lambda row: row['geometry'].buffer(25) if row['geometry_changes'] == None else row['geometry_changes'].buffer(25), axis=1)

        # Add the geographic level
        tmp_gdf['geographicLevel'] = "5"

        # Create a final GeoDataFrame with the correct columns
        final_gdf = gpd.GeoDataFrame(tmp_gdf.drop(['geometry_changes', 'geometry'], axis=1), geometry= tmp_gdf['geometry'], crs=PROJECTED_CRS)

    # Return the GDF
    return final_gdf



''' ------------------------ MAIN CODE BEGINS HERE ------------------------ '''

# Get all the A1A2 foci from OSM
a1a2_osm = getA1A2Foci('BVBDMAY2019|WHO2019')

# Make sure there are no A1A2 overlaps before moving on
if not a1a2Overlap(a1a2_osm):

    # Get the masterlist
    masterlist = pd.read_excel(MASTERLIST_LOCATION)

    # Try to link A1A2 foci to those on the masterlist
    a1a2 = linkA1A2FociToMasterlist(a1a2_osm, masterlist)

    # Get all the B1B2 foci
    b1b2 = getB1B2Foci(masterlist)

    # # TODO: Remove, filtering for testing
    # b1b2 = b1b2[b1b2['VILLAGE_ID'] // 1000000 == 6307]

    # Import the Thai district boundaries for filtering
    boundaries = gpd.read_file(THAI_DISTRICT_LOCATIONS)

    # Get a list of dicts of overlapping foci
    overlaps = b1b2Overlap(a1a2, b1b2)
    # pd.DataFrame(overlaps).to_excel("C:/Users/MM-PC0L6BTX/Desktop/overlaps.xlsx")

    # Get the changes that need to be made the overlapping foci
    changes = getChanges(overlaps, boundaries, a1a2, b1b2)

    # Join those changes to the original B1B2 foci list
    combined = b1b2.join(changes.set_index('village_id'), on="VILLAGE_ID", rsuffix='_changes')

    # Clean the data for export
    b1b2_final = cleanDataFrameForExport(combined, 'b')

    # Add the a1a2 foci to the DataFrame
    final = b1b2_final.append(a1a2, ignore_index=True)

    # Convert the geometries back to the original CRS
    final = final.to_crs(ORIGINAL_CRS)

    # Check one last time to ensure there are no overlaps given the changes
    # This may happen as foci group changes are processed independent of eachother
    o = []
    for ind,geo in enumerate(final['geometry'].tolist()):
        printToSameLine(f'Rechecking for overlaps, foci {ind+1} of {len(final)}', (ind+1)/len(final))
        for i,g in enumerate(final['geometry'].tolist()):
            if ind != i:
                if geo.within(g):
                    o.append([final.iloc[ind].externalId, final.iloc[i].externalId])

    # If there are no overlaps, save and exit
    if len(o) == 0:

            # Save the final result to a geoJSON
            final.to_file(os.path.join(SAVE_FILE_LOCATION,"final.geojson"), driver='GeoJSON', encoding='utf-8')

            # Finish
            print('All done!')

    # Otherwise, alert the user and print the overlaps
    else:
        print(f'{len(o)} foci are still overlapping...')
        print(o)
