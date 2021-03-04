import geopandas as gpd
import numpy as np
import collections
import subprocess
import os
import pandas as pd

# Get the most updated version of the location hierarchies
subprocess.run(['python', os.path.join(os.path.dirname(os.path.realpath(__file__)),'get_geojson.py'), '-f'])
subprocess.run(['python', os.path.join(os.path.dirname(os.path.realpath(__file__)),'get_reveal_geometry.py')])
print('')

# Load them into memory
print('Loading hierarchies into memory...\n')
reveal = gpd.read_file(os.path.join(os.path.dirname(os.path.realpath(__file__)),'reveal_features_local.geojson'))
reveal.externalId = reveal.externalId.astype(np.int64)
osm = gpd.read_file(os.path.join(os.path.dirname(os.path.realpath(__file__)),'bvbdosm.geojson'))

# notInReveal = osm[~osm.externalId.isin(reveal.externalId.to_list())]
# print(notInReveal.externalId.to_list())

# Get the number of duplicate IDs in OSM - this should now be zero going forward
osmDupes = [x for x, y in collections.Counter(osm.externalId.values).items() if y > 1]

# Loop though the OSM features and see what is not in Reveal, what may have been modified, and whether there are any duplicate active foci in Reveal
notInReveal = []
modified = []
revealDupes = []
for index, row in osm.iterrows():

    revealMatch = reveal[reveal.externalId == row.externalId]

    if len(revealMatch) == 0:
        notInReveal.append({'externalId': row.externalId})

    elif len(revealMatch) == 1:
        percentCoverage = row.geometry.area / revealMatch.geometry.area.values[0]
        if percentCoverage < .99 or percentCoverage > 1.01:
            # print(f'Difference in area between OSM and Reveal for externalId {row.externalId} = {percentCoverage}')
            modified.append({'externalId': row.externalId, 'percentAreaMatch': percentCoverage})

    else:
        # print(f'There are {len(revealMatch)} matches for externalId {row.externalId}...')
        # print(revealMatch)
        revealDupes.append({'externalId': row.externalId, 'revealIds': [revealMatch[['id']].to_list()]})

# Print the results
pd.set_option('display.max_rows', None)

print('Duplicate descriptions in bvbdosm:')
print(pd.DataFrame([{'externalId': x} for x in osmDupes]))

print('\nFoci to potentially upload:')
print(pd.DataFrame(notInReveal))

print('\nFoci to potentially edit:')
print(pd.DataFrame(modified))

print('\nDuplicates in Reveal:')
print(pd.DataFrame(revealDupes))
