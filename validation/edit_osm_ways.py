import osmapi
import pandas as pd


# Function to update the foci/way by correcting the name and source tag, and
# adding the subvillage id
def updateWay(osmApi, osmId, subvillId, subvillName, sourceTag):

    # Get the foci/way
    ogWay = osmApi.WayGet(osmId)

    # Get the current tags and modify them to add the correct name, source, and
    # description
    tag = ogWay['tag']
    tag['description'] = subvillId
    tag['name'] = subvillName
    tag['source'] = sourceTag

    # Create updated way to include the new tag, keep everything else constant
    updatedWay = {
        'id': ogWay['id'],
        'nd': ogWay['nd'],
        'tag': tag,
        'version': ogWay['version']
    }

    # Update the foci/way on OSM - if update worked, OSM returns corrected way
    osmResponse = osmApi.WayUpdate(updatedWay)
    print(osmResponse)


# TODO: Change these values depending on who is editing, what they're editing,
# where the masterlist is located, and what you want the source tag to be
OSM_USERNAME = "elijahfilip@gmail.com"
OSM_PASSWORD = "Xh2^2pO%3OqT"
CHANGESET_DESCRIPTION = "Fixed all name and source tags, added subvillage_id to description to Ubon A1A2 foci"
MASTERLIST_LOCATION = "C:/Users/MM-PC0L6BTX/Desktop/a1a2_2020_ubon.xlsx"
SOURCE_TAG = "BVBDMAY2019"

# Log in to OSM
osmApi = osmapi.OsmApi(username=u"%s" % OSM_USERNAME, password=u"%s" % OSM_PASSWORD)

# Create the changeset
osmApi.ChangesetCreate({u"comment": u"%s" % CHANGESET_DESCRIPTION})

# Import the excel file as a DataFrame
df = pd.read_excel(MASTERLIST_LOCATION)

# Loop through the rows of the DataFrame and update the foci for each row
for index, row in df.iterrows():

    # Get the OSM id - must slice as it's written "way/####" on the masterlist
    osmId = row['OSM Code'][4:]

    # Get the subvillage id - must be converted to a string
    subvillId = str(row['VILLAGE_ID'])

    # Get the subvillage name in Thai
    subvillName = row['VILLAGE_NAME_TH']

    # run the updateWay function
    updateWay(osmApi, osmId, subvillId, subvillName, SOURCE_TAG)

# Close the changeset
osmApi.ChangesetClose()
