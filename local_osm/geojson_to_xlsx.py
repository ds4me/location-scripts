import pandas as pd
import json
import geojson
import argparse
import os
import json


def valid_geojson(gjson):
    if os.path.isfile(gjson):
        if gjson.endswith('.geojson') or gjson.endswith('.json'):
            return gjson
        else:
            raise argparse.ArgumentTypeError(f'Input file must end with .geojson')
    else:
        raise argparse.ArgumentTypeError(f'{gjson} is not a valid file')

def valid_xlsx(xlsx):
    if xlsx.endswith('.xlsx'):
        return xlsx
    else:
        raise argparse.ArgumentTypeError('Save file must end with .xlsx')


def main():
    parser = argparse.ArgumentParser(description="Script to convert GeoJSON to normalized XLSX files")
    parser.add_argument('geojson', type=valid_geojson, help="GeoJSON file to normalize")
    parser.add_argument('-s', '--saveLocation', type=valid_xlsx, dest='saveFile', default=None, help="Save location of the XLSX file, defaults to the current folder")
    args = parser.parse_args()

    print(args.saveFile)

    saveFile = args.saveFile
    if saveFile == None:
        saveFile = os.path.join(os.getcwd(), f'{os.path.splitext(os.path.basename(args.geojson))[0]}.xlsx')


    print(saveFile)
    with open(args.geojson, encoding='utf8') as f:
        # data = geojson.load(f)
        data = json.load(f)

    # df = pd.json_normalize(data['features'])
    print(data)
    df = pd.json_normalize(data['events'])
    df.to_excel(saveFile)


if __name__ == '__main__':
    main()
