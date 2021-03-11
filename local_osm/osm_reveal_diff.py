import geopandas as gpd
import numpy as np
import collections
import subprocess
import os
import pandas as pd

# Get the most updated version of the location hierarchies
subprocess.run(['python', os.path.join(os.path.dirname(os.path.realpath(__file__)),'get_geojson.py'), '-f'])
print('')
subprocess.run(['python', os.path.join(os.path.dirname(os.path.realpath(__file__)),'get_reveal_geometry.py')])
print('')

# Load them into memory
print('Loading hierarchies into memory...\n')
reveal = gpd.read_file(os.path.join(os.path.dirname(os.path.realpath(__file__)),'reveal_features_local.geojson'))
reveal.externalId = reveal.externalId.astype(np.int64)
osm = gpd.read_file(os.path.join(os.path.dirname(os.path.realpath(__file__)),'bvbdosm.geojson'))

# notInReveal = osm[~osm.externalId.isin(reveal.externalId.to_list())]
# print(notInReveal.externalId.to_list())

print('Analyzing differences between the hierarchies...\n')

# Get the number of duplicate IDs in OSM - this should now be zero going forward
osmDupes = [x for x, y in collections.Counter(osm.externalId.values).items() if y > 1]

# Loop though the OSM features and see what is not in Reveal, what may have been modified, and whether there are any duplicate active foci in Reveal
notInReveal = []
modified = []
revealDupes = []
for index, row in osm.iterrows():

    revealMatch = reveal[reveal.externalId == row.externalId]

    if len(revealMatch) == 0:
        notInReveal.append({'wayId': row.osmid, 'externalId': row.externalId, 'lastEdited': row.last_edit_date, 'lastEditedBy': row.last_edit_user})

    elif len(revealMatch) == 1:
        percentCoverage = row.geometry.area / revealMatch.geometry.area.values[0]
        if percentCoverage < .99 or percentCoverage > 1.01:
            # print(f'Difference in area between OSM and Reveal for externalId {row.externalId} = {percentCoverage}')
            modified.append({'wayId': row.osmid, 'externalId': row.externalId, 'percentAreaMatch': percentCoverage, 'lastEdited': row.last_edit_date, 'lastEditedBy': row.last_edit_user})

    else:
        # print(f'There are {len(revealMatch)} matches for externalId {row.externalId}...')
        # print(revealMatch)
        revealDupes.append({'wayId': row.osmid, 'externalId': row.externalId, 'revealIds': [revealMatch[['id']].to_list()]})

# Print the results
pd.set_option('display.max_rows', None)

print('Duplicate descriptions in bvbdosm:')
dupesDf = pd.DataFrame([{'externalId': x} for x in osmDupes])
print(dupesDf) if len(dupesDf) == 0 else print(dupesDf.sort_values(by=['externalId']).reset_index(drop=True))

print('\nFoci to potentially upload (in bvbdosm but not Reveal):')
uploadDf = pd.DataFrame(notInReveal)
print(uploadDf) if len(uploadDf) == 0 else print(uploadDf.sort_values(by=['lastEdited', 'externalId']).reset_index(drop=True))

print('\nFoci to potentially edit (area is either smaller or larger than what is in Reveal):')
editDf = pd.DataFrame(modified)
print(editDf) if len(editDf) == 0 else print(editDf.sort_values(by=['lastEdited', 'externalId']).reset_index(drop=True))

print('\nDuplicates in Reveal:')
revealDupesDf = pd.DataFrame(revealDupes)
print(revealDupesDf) if len(revealDupesDf) == 0 else print(revealDupesDf.sort_values(by=['externalId']).reset_index(drop=True))
