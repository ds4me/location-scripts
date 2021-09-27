from faker import Faker
import random
from sqlalchemy import create_engine, types
import pandas as pd
from datetime import date
import argparse
import configparser
import os


def getCaseClass(siteId, sourceId):
    
    if siteId == sourceId or siteId[0:8] == sourceId[0:8]:
        return 'A'
    elif siteId[0:6] == sourceId[0:6]:
        return 'Bx'
    elif siteId[0:4] == sourceId[0:4]:
        return 'By'
    elif siteId[0:2] == sourceId[0:2]:
        return 'Bz'
    else: 
        return 'Bo'


# TODO: Check this logic once plan generation is fixed in preview
# def getSource(insertType, locations, site):
#     if insertType == 'active':
#         return site
#     else:
#         if random.random() > .5:
#             return site
#         else:
#             return locations.sample()


def main():

    # Get the number of index cases to insert
    parser = argparse.ArgumentParser(description="Script to insert index cases into Thailand Preview for testing")
    parser.add_argument('numToInsert', help="Amount of index cases to insert", type=int)
    # parser.add_argument('insertType', choices=['random','active'], help='Type of insert - completely random cases or those guaranteed to generate 1-2 reactive plans')
    args = parser.parse_args()

    # Create faker
    fake = Faker()

     # Get the config details from config.ini
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)),'config.ini'))
    usr = config['db_mhealth_preview']['username']
    pwd = config['db_mhealth_preview']['password']
    server = config['db_mhealth_preview']['server']
    db = config['db_mhealth_preview']['database']

    # Connect to the preview server
    print('Connecting to the preview server...')
    previewEngine = create_engine(f"mssql+pyodbc://{usr}:{pwd}@{server}/{db}?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True)
    previewConn = previewEngine.connect()
    print('Connected!\n')

    # Get all the locations
    print('Getting all foci...')
    # activeWhere = '' if args.insertType == 'random' else "and left(externalId,2) = '12'"
    # locations = pd.read_sql_query(sql=f"select * from holding.jurisdiction where geographicLevel = 5 {activeWhere}", con=previewConn)
    locations = pd.read_sql_query(sql=f"select * from holding.jurisdiction where geographicLevel = 5", con=previewConn)
    print(f'Got all {len(locations)} foci!\n')

    # Get the max index case plus one
    print('Getting the max ID of the index_case table')
    firstIndex = pd.read_sql_query(sql='select max(id) from index_case', con=previewConn).values[0][0] + 1
    print(f'Max ID is {firstIndex - 1}')

    # Define the possible values
    # fociClasses = ['A1', 'A2', 'B1', 'B2'] if args.insertType == 'random' else ['A1', 'A2']
    fociClasses = ['A1', 'A2', 'B1', 'B2']
    species = ['F', 'V', 'M']

    # Loop through a range of the numbers of index cases to insert 
    for indexId in range(firstIndex, firstIndex + args.numToInsert):
        site = locations.sample()
        # site = locations[locations.externalId == random.choice(['1201030201','1201030202'])] if args.insertType == 'active' else locations.sample()
        # source = getSource(args.insertType, locations, site)
        source = site if random.random() > 0.5 else locations.sample()
        today = date.today().strftime('%Y-%m-%d')
        name = fake.name()

        caseNum = random.randint(10000000, 10000000000000000000)
        siteId = site.externalId.values[0]
        siteName = site.name.values[0]
        siteClass = random.choice(fociClasses)
        sourceId = source.externalId.values[0]
        sourceName = source.name.values[0]
        sourceClass = siteClass if siteId == sourceId else random.choice(fociClasses)
        caseClass = getCaseClass(siteId,sourceId)
        householdId = ''
        bloodDrawDate = today
        investigationDate = today
        ep1CreateDate = today
        ep3CreateDate = today
        houseNum = 'H321'
        firstName = name.split(' ')[0]
        lastName = name.split(' ')[1]
        age = random.randint(1, 100)
        species = random.choice(species)

        sql = f"""insert into index_case (
            case_id,
            p_site_id,
            p_site_name,
            p_site_area,
            source_site_id,
            source_site_name,
            source_site_area,
            case_classification,
            household_id,
            blood_draw_date,
            investigtion_date,
            ep1_create_date,
            ep3_create_date,
            house_number,
            patient_name,
            patient_surname,
            patient_age,
            species,
            id
        ) values (
            {caseNum},
            '{siteId}',
            '{siteName}',
            '{siteClass}',
            '{sourceId}',
            '{sourceName}',
            '{sourceClass}',
            '{caseClass}',
            '{householdId}',
            '{bloodDrawDate}',
            '{investigationDate}',
            '{ep1CreateDate}',
            '{ep3CreateDate}',
            '{houseNum}',
            '{firstName}',
            '{lastName}',
            {age},
            '{species}',
            {indexId}
        )"""

        print(sql)

        # res = previewConn.execute(sql)
        # if res != None:
        #     print(f'Index case {indexId} inserted successfully')
        # else:
        #     print(f'Issue inserting index case {indexId}!')

    # Close the connection
    previewConn.close()

if __name__ == '__main__':
    main()