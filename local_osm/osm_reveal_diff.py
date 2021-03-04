import geopandas as gpd
import numpy as np
import collections
import subprocess

# Get the most updated version of the location hierarchies
subprocess.run(['python', 'C:/Users/elija/Desktop/Work Projects/location-scripts/local_osm/get_geojson.py', '-f'])
subprocess.run(['python', 'C:/Users/elija/Desktop/Work Projects/location-scripts/local_osm/get_reveal_geometry.py'])
print('')

# Load them into memory
print('Loading hierarchies into memory...')
reveal = gpd.read_file('C:/Users/elija/Desktop/Work Projects/location-scripts/local_osm/reveal_features_local.geojson')
reveal.externalId = reveal.externalId.astype(np.int64)
osm = gpd.read_file('C:/Users/elija/Desktop/Work Projects/location-scripts/local_osm/bvbdosm.geojson')
print('')

# notInReveal = osm[~osm.externalId.isin(reveal.externalId.to_list())]
# print(notInReveal.externalId.to_list())

# Get the number of duplicate IDs in OSM - this should now be zero going forward
osmDupes = [x for x, y in collections.Counter(osm.externalId.values).items() if y > 1]
print(f'Duplicate descriptions in bvbdosm:', osmDupes)


# Loop though the OSM features and see what is not in Reveal, what may have been modified, and whether there are any duplicate active foci in Reveal
notInReveal = []
modified = []
revealDupes = []
for index, row in osm.iterrows():

    revealMatch = reveal[reveal.externalId == row.externalId]

    if len(revealMatch) == 0:
        notInReveal.append(row.externalId)

    elif len(revealMatch) == 1:
        percentCoverage = row.geometry.area / revealMatch.geometry.area.values[0]
        if percentCoverage < .99:
            # print(f'Difference in area between OSM and Reveal for externalId {row.externalId} = {percentCoverage}')
            modified.append({'externalId': row.externalId, 'percentMatch': percentCoverage})

    else:
        # print(f'There are {len(revealMatch)} matches for externalId {row.externalId}...')
        # print(revealMatch)
        revealDupes.append({'externalId': row.externalId, 'revealIds': [revealMatch[['id']].to_list()]})

# Print the results
print(f'Foci not in Reveal: {notInReveal}')
print(f'Foci to potentially update: {modified}')
print(f'Duplicates in Reveal: {revealDupes}')
