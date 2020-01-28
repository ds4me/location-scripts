
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
import rtree

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

    # # Save the file
    # a1a2_osm.to_file(os.path.join(SAVE_FILE_LOCATION,"foci.geojson"), driver='GeoJSON', encoding='utf-8')

    # Import the file
    # a1a2_osm = gpd.read_file(os.path.join(SAVE_FILE_LOCATION, "foci.geojson"))

    # Print the number of foci extracted and return the GDF
    print(f'{len(a1a2_osm)} A1A2 foci imported from OSM\n')
    return a1a2_osm


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
        a1a2 = cleanDataFrame(a1a2)

        # Return the GDF
        print(f'All {len(a1a2_osm)} foci successfully linked to the masterlist\n')
        return a1a2


def getB1B2Foci(masterlist):

    # Print what is happening
    print('\nExtracting B1B2 foci from the masterlist...')

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

    # Print what happened and return the cleaned dataframe with the needed variables
    print(f'{len(b1b2)} B1B2 foci imported from the masterlist\n')
    return cleanDataFrame(b1b2)


def cleanDataFrame(gdf):

    # Get only the relevant columns from the GeoDataFrame
    tmp_gdf = gdf[['VILLAGE_ID', 'VILLAGE_NAME_TH', 'VILLAGE_NAME_EN', 'MU_NAME_TH', 'MU_NAME_EN', 'area_code_2020', 'osm_code', 'geometry']]

    # Rename the columns to fit the naming conventions used
    tmp_gdf.columns = ['externalId', 'name', 'name_en', 'parentName', 'parentName_en', 'areaCode2020', 'osmId', 'geometry']

    # Create an externalParentId column
    tmp_gdf = tmp_gdf.assign(externalParentId=lambda x: x['externalId'] // 100)

    # Add the geographic level
    tmp_gdf['geographicLevel'] = "5"

    # Create a final GeoDataFrame with the correct columns
    return gpd.GeoDataFrame(tmp_gdf.drop(['geometry'], axis=1), geometry= tmp_gdf['geometry'], crs=PROJECTED_CRS)

    # Change the items in the geometry column where necessary and create the 25m buffer
    # tmp_gdf["geometry"] = tmp_gdf.apply(lambda row: row['geometry'].buffer(25) if row['geometry_changes'] == None else row['geometry_changes'].buffer(25), axis=1)


def getOverlaps(df1, df2):

    # Get the type which dictates how to return overlaps - only supports a1a2 vs a1a2 and b1b2 vs ab
    type = 'all' if df1['geometry'].iloc[0].geom_type == "Point" else 'a'

    # Print what is happening
    print(f'Checking for overlaps between {len(df1)} {"B1B2" if type == "all" else "A1A2"} foci and {len(df2)} {"total" if type == "all" else "A1A2"} foci...')

    # Create an index for the rtree
    index = rtree.index.Index()

    # Loop through all foci, buffering where necessary, and add them to the index
    for i,g in enumerate(df2['geometry']):
        if g.geom_type == "Point": g = g.buffer(25)
        index.insert(i,g.bounds)

    # Create lists for the actual overlaps and the b1b2 foci overlaps
    actual_overlaps = []
    sub = []

    # Loop over just the points
    for ind,g in enumerate(df1['geometry'].tolist()):

        # Buffer the points and get the village_id for each one
        if g.geom_type == "Point": g = g.buffer(25)
        g_id = df1['externalId'].iloc[ind]

        # Look for intersections with the buffered point and the rtree
        ids = [i for i in index.intersection(g.bounds)]

        # Create a list for b1b2 overlaps and a boolean for a1a2 overlap
        b1b2_overlaps = []
        a1a2_overlap = False

        # Loop through the ids from the rtree intersection
        for id in ids:

            # Get the geometry, the geometry type, and the village_id
            index_geom = df2['geometry'].iloc[id]
            geom_type = index_geom.geom_type
            index_id = df2['externalId'].iloc[id]

            # Buffer the geometry if it's a point
            if geom_type == "Point": index_geom = index_geom.buffer(25)

            # If the vilage ids don't match, there is an intersection and not just a touching of boundaries...
            if g_id != index_id and g.intersects(index_geom) and not g.touches(index_geom):

                # If comparing b1b2 to ab and it's a polygon, change the a1a2 overlap boolean, otherwise add the village id to b1b2_overlaps
                if type == "all" and geom_type == "Polygon":
                    a1a2_overlap = True
                else:
                    b1b2_overlaps.append(index_id)

        # If the village_id isn't already in a b1b2_overlaps list and there are either b1b2 overlaps or an a1a2 overlap...
        if g_id not in sub and (b1b2_overlaps or a1a2_overlap):

            # Extend the sublist and append a dict of the overlap in question
            sub.extend(b1b2_overlaps)

            # Switch on the type - if b1b2 vs ab, get all three variables, only two if a1a2 vs a1a2
            if type == "all":
                actual_overlaps.append({'vill_id': g_id, 'b1b2overlaps': b1b2_overlaps, 'a1a2overlap': a1a2_overlap})
            else:
                actual_overlaps.append({'vill_id': g_id, 'overlapfoci': b1b2_overlaps})

    # Print what happened and return the list of overlaps
    print(f'Found {len(actual_overlaps)} overlapping foci groups')
    return actual_overlaps


def getChanges(overlaps, boundaries, ab):

    # Print what's happening
    print(f'Getting new points for the {len(overlaps)} overlapping B1B2 foci groups...')

    # Split the masterlist into it's a1a2 and b1b2 components
    a1a2 = ab.loc[ab['geometry'].geom_type == "Polygon"]
    b1b2 = ab.loc[ab['geometry'].geom_type == "Point"]

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
        vill_id = o.get('vill_id')
        dist_id = vill_id // 1000000
        b1b2_overlaps = o.get('b1b2overlaps')
        a1a2_overlap = o.get('a1a2overlap')

        # Get the boundary, if necessary
        if boundary_id != dist_id:
            boundary_id = dist_id
            boundary = boundaries[boundaries["DIST_CODE"] == dist_id]

        # Create a lambda function to extract a DataFrame of the overlapping points
        get_points = lambda x : b1b2[b1b2["externalId"].isin(x)]

        # Separate points into four categories: Those that overlap both B1B2 and A1A2 foci,
        # those that overlap only B1B2 foci, those that overlap only A1A2 foci, and
        # those that don't overlap any foci
        if len(b1b2_overlaps) > 0:
            if a1a2_overlap:
                # Move all B1B2 foci to the nearest points
                points = get_points([vill_id] + b1b2_overlaps)
            else:
                # Move all but the first point to the nearest points
                points = get_points(b1b2_overlaps)
        else:
            if a1a2_overlap:
                # Move single point to nearest point
                points = get_points([vill_id])

        # Get a list of the closest points - note, size and step are arbitrary... may be best to make a square slightly larger than the bbox of the largest foci?
        closest_points = createAndFilterPoints(boundary, points, 10000, 500, a1a2, b1b2)

        # Loop trough a list of the village ids
        for i,p in enumerate(points["externalId"].tolist()):

            # Get the the closest points in order
            closest_point = closest_points['geometry'].iloc[i]

            # Append changes as a dict to the "changes" list
            changes.append({"externalId": p, "lat": closest_point.y, "long": closest_point.x})

    # Get a pandas DF of the changes
    df = pd.DataFrame(changes)

    # Print the results and return a GeoDataFrame of them
    print(f'Changed the coordinates of {len(df)} foci')
    return gpd.GeoDataFrame(df['externalId'], geometry=gpd.points_from_xy(df.long, df.lat))


# TODO: See if using rtree can speed up the filtering out of ab foci
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
    all_points = gpd.GeoDataFrame(geometry=potential_points, crs=PROJECTED_CRS)

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


''' ------------------------ MAIN CODE BEGINS HERE ------------------------ '''

# Get all the A1A2 foci from OSM
a1a2_osm = getA1A2Foci('BVBDMAY2019|WHO2019')

# Get the masterlist
masterlist = pd.read_excel(MASTERLIST_LOCATION)

# Try to link A1A2 foci to those on the masterlist
a1a2 = linkA1A2FociToMasterlist(a1a2_osm, masterlist)

# Make sure there are no A1A2 overlaps before moving on
a1a2_overlaps = getOverlaps(a1a2,a1a2)
if a1a2_overlaps:

    # Tell the user they need to fix things in OSM and print the overlaps
    print('Fix the following features in OSM to ensure there are no overlaps before proceeding')
    print(a1a2_overlaps)

else:

    # Get all the B1B2 foci
    b1b2 = getB1B2Foci(masterlist)

    # Join the two cleaned dataframes
    ab = b1b2.append(a1a2, ignore_index=True)

    # Get a list of dicts of overlapping foci
    overlaps = getOverlaps(b1b2, ab)

    # Import the Thai district boundaries for filtering - note that these boundaries are not great and result in points outside of the country...
    boundaries = gpd.read_file(THAI_DISTRICT_LOCATIONS)

    # Using a while loop over the
    while_loop_num = 1
    while overlaps:

        print(f'\nAttempt {while_loop_num} to fix overlaps:')

        # Get a GDF of the changes that need to be made
        changes = getChanges(overlaps, boundaries, ab)

        # Merge the changes into the cleaned, full masterlist and recreate the GDF
        ab = ab.merge(changes, on="externalId", how="left")
        ab['geometry'] = ab.apply(lambda x: x['geometry_x'] if x['geometry_y'] == None else x['geometry_y'], axis=1)
        ab.drop(["geometry_y", "geometry_x"], inplace=True, axis=1)
        ab = gpd.GeoDataFrame(ab.drop(['geometry'], axis=1), geometry= ab['geometry'], crs=PROJECTED_CRS)

        # Check if there are any remaining overlaps and increment the while loop counter
        overlaps = getOverlaps(ab.loc[ab["geometry"].geom_type == "Point"], ab)
        while_loop_num = while_loop_num + 1

    print('\nThere are no more overlapping features!')

    # Buffer the points
    ab['geometry'] = ab.apply(lambda x: x['geometry'].buffer(25) if x['geometry'].geom_type == "Point" else x['geometry'], axis=1)

    # Change the CRS
    ab = ab.to_crs(ORIGINAL_CRS)

    # Save the final result
    ab.to_file(os.path.join(SAVE_FILE_LOCATION,"ab.geojson"), driver='GeoJSON', encoding='utf-8')

    print(f'All done! Final geojson saved to {os.path.join(SAVE_FILE_LOCATION,"ab.geojson")}')
