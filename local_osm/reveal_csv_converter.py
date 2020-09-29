import pandas as pd
import geojson
import json
import argparse
import os

# Create an empty global variable list for features
feats = []
def create_and_add_features(row):

    # Load the string coordinates as Python objects
    loaded = json.loads(row.coordinates)

    # create the feature
    feat = geojson.Feature(
        geometry={
            "type": row.type,
            "coordinates": loaded
        },
        properties={
            "id": str(row.id),
            "externalId": str(row.externalid),
            "name": str(row['name']),
            "geographicLevel": str(row.geographiclevel)
        },
    )

    # Append it ot the list
    feats.append(feat)


def main():

    # Parse the command line arguments of the script
    parser = argparse.ArgumentParser(description='Script to convert Reveal CSV to different formats')
    parser.add_argument('type', choices=["xlsx","geojson"], help='Type of conversion to perform')
    parser.add_argument('input_file', help='Input file location')
    parser.add_argument('-o', '--output_file', help='Output file location')
    parser.add_argument('-d', dest='delimiter', default='|',  help="Delimiter for the csv input file, default is '|'")
    parser.add_argument('-l', dest='level', default='all', choices=["0","1","2","3","4","5","all"], help="Select the level you want to filter for, default is 'all'")
    args = parser.parse_args()

    # Check to see if the file exists
    if not os.path.exists(args.input_file):
        return "Input file path doesn't exist"

    # Create the data frame and print it
    df = pd.read_csv(args.input_file, delimiter=args.delimiter)

    # Filter the df if the level argument is included
    if args.level != 'all':
        df = df.loc[df.geographiclevel == int(args.level)]

    # Print the number of features to be exported
    print(f'Number of features: {len(df)}')

    # Switch on the type of conversion
    if args.type == 'xlsx':

        # Set the output_file to the local folder if not specified
        output_file = './jurisdictions.xlsx' if not args.output_file else args.output_file

        # Raise a ValueError if the file doesn't end in with .xlsx
        if output_file[-5:] != '.xlsx':
            raise ValueError("Output file name must end with '.xlsx'")

        # Save to Excel
        df.to_excel(output_file, index=False)

    elif args.type == 'geojson':

        # Set the output_file to the local folder if not specified
        output_file = './jurisdictions.geojson' if not args.output_file else args.output_file

        # Raise a ValueError if the file doesn't end in with .xlsx
        if output_file[-8:] != '.geojson':
            raise ValueError("Output file name must end with '.geojson'")

        # Clear out NaNs on the coordinates
        df = df[df.coordinates.notnull()]

        # Apply the fuction to the df
        df.apply(lambda row: create_and_add_features(row), axis=1)

        # Create the feature collection from the features
        fc = geojson.FeatureCollection(feats)

        # Save the feature collection
        with open(output_file, 'w', encoding='utf8') as f:
            # geojson.dump(fc, f, ensure_ascii=False, indent=4)
            geojson.dump(fc, f, ensure_ascii=False)

    # Print the save location
    print(f'Saved to {output_file}')


if __name__ == '__main__':
    main()
