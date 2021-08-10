import pandas as pd
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import LegacyApplicationClient
import configparser
from retry_requests import retry
from requests import Session
import os
import json
from datetime import datetime, timedelta
import math
import ast



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


def create_team_assignment_json(idList, planId, jurisdictionId, fromDate, toDate):
    assignArray = []
    for val in idList:
        assignArray.append({
            "organization": val,
            "jurisdiction": jurisdictionId,
            "plan": planId,
            "fromDate": fromDate,
            "toDate": toDate
        })
    return assignArray


def main():

    # Load the config file and get the Oauth2 token
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)),'config.ini'))
    token = get_oauth_token(config, 'local')
    revealHeaders = {"Authorization": f'Bearer {token["access_token"]}'}

    # Get the current orgs from the API
    orgUrl = 'https://servermhealth.ddc.moph.go.th/opensrp/rest/organization'
    orgs = get_request(orgUrl, revealHeaders)

    # Load the issues and filter out those with some API values
    df = pd.read_excel('./team_assignment_issues.xlsx')
    # dfEmpty = df[df.api == '[]']
    # dfEmpty = dfEmpty.reset_index()




    # Loop through the dataframe and push new assignments
    for index, row in df.iterrows():

        print(f'{index + 1}/{len(df)}: fixing team assignments for planID {row.identifier}')

        # Evaluate the rows into Python lists
        correctNamedAssignments = ast.literal_eval(row.correct)
        apiNamedAssignments = ast.literal_eval(row.api)

        # Get the values that are missing on the 
        missingNamedAssignments = list(set(correctNamedAssignments) - set(apiNamedAssignments))
        # extraNamedAssignments = list(set(apiNamedAssignments) - set(correctNamedAssignments))
        
        # # Loop through assignment names and get the correct IDs for each organization
        idAssignments = [orgs[orgs.name == name.strip()].identifier.values[0] for name in missingNamedAssignments]

        # If there are current API assignments, get the start and end dates of the current assignments - NOTE: might not be needed?
        # if apiNamedAssignments != []:
        #     currentAssignUrl = f'https://servermhealth.ddc.moph.go.th/opensrp/rest/organization/assignedLocationsAndPlans?plan={row.identifier}'
        #     res = retrySession.get(currentAssignUrl, headers=revealHeaders)
        #     if res.status_code != 200:
        #         raise ValueError('Issue getting current team assignment for plan')
        #     currentAssign = res.json()
        #     fromDate = currentAssign[0]['fromDate']
        #     toDate = currentAssign[0]['toDate']

        # # Else use the current timestamp and 5 years from now
        # else:

        fromDate = math.floor(datetime.now().timestamp()) * 1000
        toDate = math.floor((datetime.now() + timedelta(days=5*365)).timestamp()) * 1000

        # Create the team assignment JSON
        j = create_team_assignment_json(idAssignments, row.identifier, row.jurisdiction, fromDate, toDate)
        print(json.dumps(j, indent=2))

        assignUrl = 'https://servermhealth.ddc.moph.go.th/opensrp/rest/organization/assignLocationsAndPlans'
        r = retrySession.post(assignUrl, json=j, headers=revealHeaders)
        print(r.status_code)
        if r.status_code != 200:
            raise ValueError(f'Response code from the server: {r.status_code}')

        

if __name__ == '__main__':
    main()
    