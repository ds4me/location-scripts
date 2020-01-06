# import overpass
from shapely.geometry import Point
import geopandas as gpd
import pandas as pd
import numpy as np

# TODO: Pull the files using overpass-turbo's API
# TODO: Import from masterlist format - remove need to separate out lat/long

# Get the items based on source name for all of Thailand
# query_string = 'way["source"~"BVBD|VBDU|VBDD|VBVD|VBBD|DVBD",i](11.555380,102.249756,12.785018,102.950134);(._;>;);out geom;'
# overpass_api = overpass.API(timeout=600)
# response = overpass_api.get(query_string, verbosity='geom', responseformat='geojson')
# features = [f for f in response.features if f.geometry['type'] == "LineString"]

# TODO: Change these values based on the location of the files. Note that there
# must be separate "lat" and "long" columns in the master list
OSM_QUERY_LOCATION = "C:/Users/MM-PC0L6BTX/Desktop/Misc Thai Foci/osm_query/osm.shp"
MASTERLIST_LOCATION = "C:/Users/MM-PC0L6BTX/Desktop/ubon.xlsx"
SUBDISTRICT_SHAPEFILE_LOCATION = "C:/Users/MM-PC0L6BTX/Desktop/Misc Thai Foci/Thai Boundary/Subdistrict_region.shp"
FINAL_FILE_LOCATION = "C:/Users/MM-PC0L6BTX/Desktop/validation_output.xlsx"

# Load OSM shape file
osm_shape = gpd.read_file(OSM_QUERY_LOCATION)

# Load the master list
master_list = pd.read_excel(MASTERLIST_LOCATION)

# Import the subdistrict shape file, change the subdistrict code to Int64, and
# convert to WGS84
subdist_shape = gpd.read_file(SUBDISTRICT_SHAPEFILE_LOCATION)
subdist_shape['SDIST_CODE'] = subdist_shape['SDIST_CODE'].astype(np.int64)
subdist_shape = subdist_shape.to_crs({'init': 'epsg:4326'})

# Loop through the master_list items and osm_shape file to see if the point in
# the master_list falls within the boundaries of an osm_shape
contained_matches = []
for i, r in master_list.iterrows():

    # Convert the GPS coord to a Shapely Point
    p = Point(r['long'], r['lat'])

    # Loop through the osm_shapes and add the ones that match to the
    # contained_matches list
    for index, row in osm_shape.iterrows():

        # If the point falls in a polygon, add the VILLAGE_ID, osm_id, and _name
        # to the contained_matches list
        if row['geometry'].contains(p):
            contained_matches.append([r['VILLAGE_ID'], row['id'], row['name']])

# Convert contained_matches to a DataFrame and set the column names
contained_matches_df = pd.DataFrame(contained_matches)
contained_matches_df.columns = ['VILLAGE_ID', 'contained_osm_id', 'contained_name']

# Merge the two dataframes based on the VILLAGE_ID
contained_merge = pd.merge(master_list, contained_matches_df, on='VILLAGE_ID', how='left')

# Loop through foci that do not fall within an osm_shape polygon and find the
# osm_shape polygon that is nearest to the GPS point in the master_list
near_matches = []
for i, r in contained_merge[contained_merge['contained_name'].isnull()].iterrows():

    # Convert the GPS coord to a Shapely Point
    p = Point(r['long'], r['lat'])

    # Get the closest polygon
    min_poly = min(list(osm_shape['geometry']), key=p.distance)

    # Return the osm_shape row whose geometry matches that of the min_poly
    matching_poly_row = osm_shape[osm_shape['geometry'] == min_poly]

    # Add the VILLAGE_ID, osm_id, and _name to the near_matches list
    near_matches.append([r['VILLAGE_ID'], matching_poly_row['id'].values[0], matching_poly_row['name'].values[0]])

# Convert the near_matches list to a dataframe, rename the columns, and merge
# it with the contained_merge DataFrame from earlier
near_matches_df = pd.DataFrame(near_matches)
near_matches_df.columns = ['VILLAGE_ID', 'near_osm_id', 'near_name']
near_merge = pd.merge(contained_merge, near_matches_df, on='VILLAGE_ID', how='left')

# Subdistrict and name validation
near_merge['SDIST_CODE'] = near_merge['VILLAGE_ID']//10000

# Get a list of the foci with their subdistrict code, village id, village name,
# and subdistrict polygon
foci_subdist = near_merge.merge(subdist_shape[['SDIST_CODE', 'geometry']], on="SDIST_CODE", how="inner")

# Then, find all osm foci that intersect/overlap with the shapefile and pull
# their names
matches = []
for i, r in foci_subdist.iterrows():
    for index, row in osm_shape.iterrows():

        # If there is an intersection,
        if r['geometry'].intersects(row['geometry']):

            # Check whether the osm_shape name contains the foci_subdist name
            # and add to matches list, if so
            if row['name'] is not None and r['VILLAGE_NAME_TH'] in row['name']:
                matches.append([r['VILLAGE_ID'], row['id'], row['name']])

# Convert the list to a dataframe, change the column names, and export to Excel
match_df = pd.DataFrame(matches)
match_df.columns = ["VILLAGE_ID", "SDIST_VAL_OSM_ID", "SDIST_VAL_OSM_NAME"]
match_df = match_df.groupby('VILLAGE_ID', as_index=False).agg(lambda x: ', '.join(set(x.astype(str))))
final_merge = near_merge.merge(match_df, on="VILLAGE_ID", how="left")

# Save the final DataFrame as an Excel file
final_merge.to_excel(FINAL_FILE_LOCATION)
