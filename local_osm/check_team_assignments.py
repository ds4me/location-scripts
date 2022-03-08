from operator import index
import pandas as pd
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import LegacyApplicationClient
import configparser
from retry_requests import retry
from requests import Session
import os
import collections
from datetime import datetime
import sys
import numpy as np
from sqlalchemy import asc


# Create the requests session with retries and backoff
retrySession = retry(Session(), retries=10, backoff_factor=0.1)


def get_oauth_token(config, server):
    token_url = config[f'{server}_reveal']['token_url']
    username = config[f'{server}_reveal']['username']
    password = config[f'{server}_reveal']['password']
    client_id = config[f'{server}_reveal']['client_id']
    client_secret = config[f'{server}_reveal']['client_secret']
    oauth = OAuth2Session(client=LegacyApplicationClient(client_id=client_id))
    return oauth.fetch_token(token_url=token_url,username=username, password=password,client_id=client_id, client_secret=client_secret)


def get_request(url, headers=[]):
    r = retrySession.get(url, headers=headers)
    if r.status_code == 200:
        j = r.json()
        return pd.DataFrame(j)
    else:
        raise ValueError(f'Could not get data from the following url: {url}')


def print_same_line(output):
    sys.stdout.write(output)
    sys.stdout.write('\r')
    sys.stdout.flush()


def retrySave(df, saveLocation):
    try:
        df.to_excel(saveLocation, index=False)
        print(f'{len(df[df.fixed == False])} outstanding team assignment issues found! For details see the following file: {saveLocation}')
    except PermissionError:
        print('The file appears to be open on your computer. Close it and try again?')
        retry = None
        while retry == None:
            retry = input('(y/n): ')
            if retry not in ['y','n']:
                print(f'{retry} is not a valid option, please type either "y" or "n"')
                retry = None
        if retry == 'y':
            retrySave(df,saveLocation)
        else:
            print('Exited without saving')


def main():
    # Load the config file and get the Oauth2 token
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)),'config.ini'))
    token = get_oauth_token(config, 'local')

    # Get the headers for OpenSRP requests
    revealHeaders = {"Authorization": "Bearer {}".format(token['access_token'])}

    # Read in the excel file with the plans and correct team assignments
    # reveal = pd.read_excel('./plans_and_assignments.xlsx') # From here: http://66.228.52.243:3000/question/451-correct-team-assignments-by-plan
    print('Getting plans from Metabase...')
    revealUrl = 'http://66.228.52.243:3000/public/question/0b22d3f7-1c77-4a41-9f45-6e21726253d8.json'
    reveal = get_request(revealUrl)

    # # Get the current organizations from the API
    print('Getting organizations from OpenSRP...')
    orgUrl = 'https://servermhealth.ddc.moph.go.th/opensrp/rest/organization'
    orgs = get_request(orgUrl, revealHeaders)

    # For each plan, hit the API to check actual assignments, also setup the dataframe for the correct assignments
    correctAssign = []
    apiAssign = []
    for index,row in reveal.iterrows():
        print_same_line(f'Looping through OpenSRP API to get the current team assignments: {index + 1}/{len(reveal)}')

        # Get assignments from the API
        url = f'https://servermhealth.ddc.moph.go.th/opensrp/rest/organization/assignedLocationsAndPlans?plan={row.identifier}'
        r = retrySession.get(url, headers=revealHeaders)
        if r.status_code == 200:
            assignments = [x['organizationId'] for x in r.json()]
            namedAssignments = [orgs[orgs['identifier'] == x].name.values[0] for x in assignments]
            namedAssignments = sorted(namedAssignments, key=str.casefold, reverse=True)
            apiAssign.append({
                'identifier': row.identifier, 
                'assignments': namedAssignments
            })

            # Reorganize SQL query data to match apiAssign array above
            correctAssign.append({
                'identifier': row.identifier, 
                'title': row.title,
                'date': row.dateCreated, 
                'status': row.status, 
                'jurisdiction': row.jurisdiction,
                'externalId': row.externalId,
                'effectivePeriod_start': row.effectivePeriod_start,
                'effectivePeriod_end': row.effectivePeriod_end,
                'assignments': [x for x in [
                    row.field_team, 
                    row.vbdc_team, 
                    row.odpc_team, 
                    row.pho_team, 
                    row.bvbd_team
                ] if str(x) != 'None'], 
                'link': row.link
            })


    # Create dataframes from the arrays above
    apiDf = pd.DataFrame(apiAssign)
    correctDf = pd.DataFrame(correctAssign)
    print()

    # Define different compoarators depending on need
    # This shows absolute differences in count and values
    # For example, [odpc_2, odpc_2, bvbd_mhealth] != [odpc_2, bvbd_mhealth]
    # compare = lambda x, y: collections.Counter(x) == collections.Counter(y)

    # This removes duplicate values from the array before comparison
    # For example, [odpc_2, odpc_2, bvbd_mhealth] == [odpc_2, bvbd_mhealth]
    compare = lambda x, y: set(x) == set(y)

    # Loop through the corrected items to find any issues between the "correct" and "api" arrays
    issues = []
    current_datetime = datetime.now()
    for index,p in correctDf.iterrows():
        correctTeams = p.assignments
        apiTeams = apiDf[apiDf.identifier == p.identifier].assignments.values[0]
        if not compare(correctTeams, apiTeams):
            issues.append({
                'date': p.date, 
                'identifier': p.identifier, 
                'title': p.title,
                'status': p.status, 
                'jurisdiction': p.jurisdiction,
                'externalId': p.externalId,
                'effectivePeriod_start': p.effectivePeriod_start,
                'effectivePeriod_end': p.effectivePeriod_end,
                'correct': correctTeams, 
                'api': apiTeams, 
                'link': p.link,
                'dateIssueDetected': current_datetime,
                'fixed': False
            })


    # Create a dataframe with the issues
    issuesDf = pd.DataFrame(issues)

    # Get the location the Excel file will be saved
    saveLocation = os.path.join(os.path.dirname(os.path.realpath(__file__)),'team_assignment_issues.xlsx')

    # If there is a previously run version the Excel...
    if os.path.exists(saveLocation):

        # Load the most recently fetched version of issues
        oldIssuesDf = pd.read_excel(saveLocation)

        # Merge the dataframes to see where the overlaps exist
        merged = pd.merge(oldIssuesDf, issuesDf, on='identifier', how='outer', indicator=True)

        # Print some summary statistics about the newly pulled data
        print(f'Fixed issues since last run: {len(merged[merged._merge == "left_only" & merged.fixed_x == False])}')
        print(f'New issues since last run: {len(merged[merged._merge == "right_only"])}')

        # Set the fixed value to true - use fixed_x to ensure it won't be overwritten in the loop below
        merged.loc[merged._merge == 'left_only', 'fixed_x'] = True

        # Loop through all the _x columns
        for x_col_name in ['date_x', 'title_x', 'status_x', 'jurisdiction_x', 'externalId_x', 'effectivePeriod_start_x', 'effectivePeriod_end_x', 'correct_x', 'api_x', 'link_x', 'dateIssueDetected_x', 'fixed_x']:
            
            # Get the column names
            plain_col_name = x_col_name.rsplit('_',1)[0]
            y_col_name = plain_col_name + '_y'

            # Change all the dates to datetime64 for easy sorting in Excel
            if plain_col_name in ['date', 'effectivePeriod_start', 'effectivePeriod_end', 'dateIssueDetected']:
                merged[x_col_name] = merged[x_col_name].astype('datetime64')
                merged[y_col_name] = merged[y_col_name].astype('datetime64')


            # For the status and api columns, always take the new value over the old one so they're up-to-date
            # For non-status columns, always take the old value over the new one
            if plain_col_name in ('status', 'api'):
                merged[x_col_name] = merged[y_col_name].fillna(merged[x_col_name])
            else:
                merged[x_col_name] = merged[x_col_name].fillna(merged[y_col_name])

            # Drop all the _y columns
            merged.drop([y_col_name], inplace=True, axis=1)

            # Rename all the _x columns by removing the _x
            merged.rename(columns={x_col_name: plain_col_name}, inplace=True)
        
        # Drop the _merge column
        merged.drop(['_merge'], inplace=True, axis=1)

        # Sort the final result according to when the issue was detected and the date, descending
        merged.sort_values(by=['dateIssueDetected', 'date'], ascending=False, inplace=True)

        # Save the updated dataframe
        retrySave(merged, saveLocation)
    
    else:

        # Sort by date descending
        issuesDf.sort_values(by='date', ascending=False, inplace=True)

        # Save the new dataframe
        retrySave(issuesDf, saveLocation)


if __name__ == "__main__":
    main()
