import psycopg2
import requests
import json
import uuid
import logging
import sys
import getopt
import os
import configparser
from datetime import datetime
import csv
from importlib import reload
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauth2 import OAuth2BearerToken
from traceback import format_exc
import uuid

reload(sys)

logging.basicConfig(filename='./logs/app.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

config = configparser.ConfigParser()


ou_table = ''
locations_total = 0
locations_progress = 0
jurisdiction_depth = 0
level = -1
test_run = False
jurisdiction_only = False
put = False
cnconf = ''
conn = ''
firstrun = True
url_sd = ''
country = ''
skip_csv = 0

revealtemplate = {
    "type": "Feature", "id": "$id_opensrp", "serverVersion": 0,
    "geometry": {"type": "$type", "coordinates": 1111111111},
    "properties": {"status": "Active", "parentId": "$id_parent_opensrp", "name": "$name", "name_en": "$nam_en", "geographicLevel": 2222222222, "version": 0, "externalId": "$id_external", "OpenMRS_Id": "$id_openmrs"}}

revealtemplate_structure = {
    "type": "Feature", "id": "$id_opensrp", "serverVersion": 0,
    "geometry": {"type": "$type", "coordinates": 1111111111},
    "properties": {"status": "Active", "parentId": "$id_parent_opensrp", "geographicLevel": 2222222222, "version": 0, "externalId": "$id_external"}}

revealtemplate_parent = {
    "type": "Feature", "id": "$id_opensrp",  "serverVersion": 0,
    "geometry": {"type": "$type", "coordinates": 1111111111},
    "properties": {"status": "Active", "name": "$name", "geographicLevel": 2222222222, "version": 0, "externalId": "$id_external"}}

reveal_template = {"type": "Feature", "serverVersion": 0, "geometry": {
    "type": "$type"}, "properties": {"status": "Active", "version": 0}}

openmrstemplate = {"name": "$name", "tags": [{"uuid": "$hierarchytag"}], "parentLocation": {
    "uuid": "$parentuuid"}, "childLocations": [], "resourceVersion": "2.0"}

url_get_reveal_jurisdiction = "https://{0}.smartregister.org/opensrp/rest/location/{1}?is_jurisdiction=true"
url_get_reveal_jurisdictions = "https://{0}.smartregister.org/opensrp/rest/location/findByProperties?is_jurisdiction=true&return_geometry=true&properties_filter=parentId:{1}"
url_get_reveal_structure = "https://{0}.smartregister.org/opensrp/rest/location/findByProperties?is_jurisdiction=false&return_geometry=true&properties_filter=parentId:{1}"
url_get_openmrs_locations = "https://openmrs.{0}.smartregister.org/openmrs/ws/rest/v1/location/{1}"
url_post_reveal_jurisdiction = "https://{0}.smartregister.org/opensrp/rest/location/?is_jurisdiction=true"
url_post_reveal_structure = "https://{0}.smartregister.org/opensrp/rest/location/?is_jurisdiction=false"
url_post_openmrs_location = "https://openmrs.{0}.smartregister.org/openmrs/ws/rest/v1/location"
url_put_openmrs_location = "https://openmrs.{0}.smartregister.org/openmrs/ws/rest/v1/location/{1}"
url_get_openmrs_locations_name = "https://openmrs.{0}.smartregister.org/openmrs/ws/rest/v1/location?q={1}"
url_get_openmrs_attrbutes = "http://openmrs.{0}.smartregister.org/openmrs/ws/rest/v1/location/{1}/attribute/{2}"
url_post_reveal_structure_batch = "https://{0}.smartregister.org/opensrp/rest/location/add?is_jurisdiction=false"

config.read('./config/config.ini')

def xstr(s):
    if s is None:
        return ''
    return str(s)

def get_request(URL):
    auth = eval(cnconf['openmrs_auth']) if 'openmrs' in URL else eval(
        cnconf['reveal_auth'])
    response = requests.get(URL, auth=auth)
    return response

def post_request(URL, body, operation):
    logging.info(type(body))
    if type(body) == 'str':
        body = body.encode('utf-8')
    logging.info(type(body))
    auth = eval(cnconf['openmrs_auth']) if 'openmrs' in URL else eval(
        cnconf['reveal_auth'])
    response = ''
    if operation == 'i':
        response = requests.post(URL, data=body, auth=auth, headers={"Content-Type": "application/json"})
    elif operation == 'u':
        response = requests.put(URL, data=body.encode('utf-8'), auth=auth, headers={
                                "Content-Type": "application/json"})
    return response

def post_reveal_location(id_opensrp, coordinates, id_parent_opensrp, location_name, location_name_en, geographicLevel, id_external, geometry_type, id_openmrs, operation):
    logging.debug(locals())
    revealpost = json.dumps(revealtemplate)

    URL = url_post_reveal_jurisdiction.format(url_sd)
    revealpost = revealpost.replace('$name', xstr(location_name))
    revealpost = revealpost.replace('$nam_en', xstr(location_name_en))
    revealpost = revealpost.replace('$id_openmrs', xstr(id_openmrs))
    revealpost = revealpost.replace('$id_opensrp', xstr(id_opensrp))
    revealpost = revealpost.replace('1111111111', xstr(coordinates))
    revealpost = revealpost.replace('$id_parent_opensrp', xstr(id_parent_opensrp))
    revealpost = revealpost.replace('2222222222', xstr(geographicLevel))
    revealpost = revealpost.replace('$id_external', id_external)
    revealpost = revealpost.replace('$type', xstr(geometry_type))
    # todo: clean up string replacement above
    logging.debug('POST REVEAL URL: ' + URL)
    logging.debug('POST REVEAL BODY: ' + revealpost)

    if test_run == False:
        response = post_request(URL, revealpost.encode('utf-8'), operation)
        logging.info(response.text)
    else:
        logging.debug("Skipping Reveal post - test run")

    # todo assuming this is successful - check status and deal with all cases
    # should stop iterating if there is an error
    return

def write_openmrs_id_to_database(openmrs_uuid, opensrp_uuid):
    if test_run == False:
        cur = conn.cursor()
        sql = ("update mergeset set openmrs_id = '{0}' where opensrp_id = '{1}'").format(
            openmrs_uuid, opensrp_uuid)
        logging.debug(sql)
        cur.execute(sql)
        conn.commit()

def update_database(opensrp_uuid):
    if test_run == False:
        dt = datetime.now()
        cur = conn.cursor()
        sql = ("update mergeset set processed_date= '{1}', processed = true where opensrp_id = '{0}'").format(
            opensrp_uuid, dt)
        logging.debug(sql)
        cur.execute(sql)
        conn.commit()

def total_locations(id_parent_external):
    #sql = 'SELECT count(*)  from '+ ou_table  + ' where issue = false and externalParentId =' + "'" + id_parent_external + "'"
    sql = 'SELECT count(*)  from mergeset where operation is not null and processed is null'
    cur = conn.cursor()
    cur.execute(sql)
    counts = cur.fetchall()
    cur.close()
    return counts[0][0]

def load_jurisdictions(id_parent_external, id_parent_opensrp='', id_parent_openmrs=''):

    # ROOT NODES NEED TO BE CREATED FIRST AND UUIDS SUPPLIED IN CONFIG FILE
    global locations_total
    global locations_progress
    global jurisdiction_only
    global put
    global test_run
    global jurisdiction_depth
    global firstrun

    if firstrun == True:
        id_parent_opensrp = cnconf['opensrp_root_id']
        id_parent_openmrs = cnconf['openmrs_root_id']
        logging.info("First Run: id_parent_opensrp: {0} id_parent_openmrs: {1}".format(
            id_parent_opensrp, id_parent_openmrs))
        firstrun = False

    if id_parent_opensrp == '' or id_parent_openmrs == '':
        raise Exception('A parent id is not set')

    location_type = 'jurisdiction'
    #geographic_level = parent_geographic_level + 1
    # if geographic_level == 0:
    #	locations_total = total_locations()
    #	logging.info('Total locations to add: {0}'.format(locations_total))

    sql = 'SELECT opensrp_id, externalId, externalParentId, name_en, name, geographicLevel, type, coordinates, openmrs_id, operation, processed, d_name from mergeset where externalParentId =' + "'" + id_parent_external + "'"
    logging.debug(sql)
    cur = conn.cursor()
    cur.execute(sql)
    locations = cur.fetchall()
    cur.close()

    for l in locations:
        location = {}
        location['id_opensrp'] = l[0]
        location['id_external'] = l[1]
        location['parent_id_external'] = l[2]
        location['name_en'] = l[3]
        location['name_local'] = l[4]
        location['geographic_level'] = l[5]
        location['geometry_type'] = l[6]
        location['coordinates'] = l[7]
        location['id_openmrs'] = l[8]
        location['operation'] = l[9]
        location['processed'] = l[10]
        location['d_name'] = l[11]

        if location['operation'] != None and location['processed'] != True:
            locations_progress += 1
            logging.info("{7}/{8} -- {9} id_external: {0} parent_id_exernal: {1} id_opensrp: {2}, parent_id_opensrp: {3} id_openmrs: {4} parent_id_opemmrs: {5}, db_gl: {6}".format(
                location['id_external'], location['parent_id_external'], location['id_opensrp'], id_parent_opensrp, '', id_parent_openmrs, location['geographic_level'], locations_progress, locations_total, location['operation']))

        if location['operation'] == 'i' and location['processed'] != True:
            # insert - add to openmrs and to reveal
            # location['id_openmrs'] should be null
            if test_run == False:
                # should check openmrs here
                openMRS_id = post_openmrs_location(
                    id_parent_openmrs, location['name_local'], location['geographic_level'], location['operation'])
                write_openmrs_id_to_database(
                    openMRS_id, location['id_opensrp'])
                post_reveal_location(location['id_opensrp'], location['coordinates'], id_parent_opensrp, location['name_local'], location['name_en'],
                                     location['geographic_level'], location['id_external'], location['geometry_type'], openMRS_id, location['operation'])
                update_database(location['id_opensrp'])
                logging.debug('openMRS_id')
            else:
                logging.info(
                    '{0}/{1} Test Run reveal insert'.format(locations_progress, locations_total))
                openMRS_id = 'test'
        # update db with openMRS id
        elif location['operation'] == 'u' and location['processed'] != True:
                # for now assume we don't need to update openmrs and that it exists in the database
            openMRS_id = location['id_openmrs']
            if location['d_name'] == True:
                URL = url_get_openmrs_locations.format(url_sd, openMRS_id)
                c = get_request(URL)
                mrs = json.loads(c.text)
                logging.debug(mrs)
                logging.info('{0} & {1} change to {2}'.format(
                    mrs['name'], mrs['name'], location['name_local']))
                mrs['display'] = location['name_local']
                mrs['name'] = location['name_local']
                mrspost = json.dumps(mrs)
                URL = url_put_openmrs_location.format(url_sd, openMRS_id)
                if test_run == False:
                    c = post_request(URL, mrspost.encode('utf-8'), 'i')
                    logging.info(
                        'OpenMRS post response code: {0}'.format(c.status_code))
                    logging.debug('OpenMRS post response: {0}'.format(c.text))
                else:
                    logging.info(
                        '{0}/{1} Test Run omrs update'.format(locations_progress, locations_total))
                # get openMRS location, update name, post

            if test_run == False:
                post_reveal_location(location['id_opensrp'], location['coordinates'], id_parent_opensrp, location['name_local'], location['name_en'],
                                     location['geographic_level'], location['id_external'], location['geometry_type'], openMRS_id, location['operation'])
                update_database(location['id_opensrp'])
            else:
                logging.info(
                    '{0}/{1} Test Run reveal update'.format(locations_progress, locations_total))

        else:
            # either an issue or no op needed - ignore
            openMRS_id = location['id_openmrs']

        logging.debug("depth: " + str(jurisdiction_depth))
        if int(location['geographic_level']) < int(jurisdiction_depth):
            load_jurisdictions(
                location['id_external'], location['id_opensrp'], openMRS_id)
        # add to openSRP

def load_files():
    global skip_csv
    global country

    geo_path = './toimport/geojson/{0}'.format(country)
    logging.info(geo_path)
    location_path = './toimport/location/{0}'.format(country)
    sql_path = '../sql'
    files = []
    data = ''
   
    # r=root, d=directories, f = files
    logging.info('Importing files:')
    cur = conn.cursor()
    logging.info('   Truncating database tables')
    sql = ("truncate table geojson_file; truncate table location_file; truncate table changeset; truncate table mergeset; truncate table jurisdiction_master; truncate table structure_master;")
    cur.execute(sql)
    conn.commit()
    logging.info('   Loading geoJSON files @ {0}'.format(geo_path))
    for r, d, f in os.walk(geo_path):
        for file in f:
            if not file.startswith('.'):
                logging.info('   Loading geoJSON file: {0}'.format(file))
                with open('{0}/{1}'.format(geo_path, file)) as json_file:
                    try:
                        data = json.load(json_file)
                    except ValueError:
                        logging.info("value error")
                    if data != '':
                        geo = json.dumps(data).replace("'", "")
                        sql = ("insert into geojson_file (file, file_name) values ('{0}', '{1}');").format(geo, file)
                        cur.execute(sql)
                        conn.commit()
                    else:
                        logging.info("no data")
    logging.info('   Loading location files @ {0}'.format(location_path))
    for r, d, f in os.walk(location_path):
        for file in f:
            if not file.startswith('.'):
                logging.info('   Loading location file: {0}'.format(file))
                with open('{0}/{1}'.format(location_path, file)) as json_file:
                    try:
                        data = json.load(json_file)
                    except ValueError:
                        logging.info("value error")
                    if data != '':
                        geo = json.dumps(data).replace("'", "")
                        sql = ("insert into location_file (file, file_name) values ('{0}', '{1}');").format(geo, file)
                        cur.execute(sql)
                        conn.commit()
                    else:
                        logging.info("no data")


    #logging.info('   Running SQL: {0}'.format('insert_changeset.sql'))
    #with open('{0}/{1}'.format(sql_path, 'insert_changeset.sql')) as sql_file:
     #   sql = sql_file.read()
      #  logging.debug(sql)
        #cur.execute(sql)
        #conn.commit()

   # abspath = os.path.abspath('{0}/jurisdictions.csv'.format(location_path))
    #logging.info('   Importing master jurisdiction CSV file from: {0}'.format(abspath))
    
   


   # logging.info('   Running SQL: {0}'.format('run_merge.sql'))
    #with open('{0}/{1}'.format(sql_path, 'run_merge.sql')) as sql_file:
       # sql = sql_file.read()
       # logging.debug(sql)
        #cur.execute(sql)
        #conn.commit()

    #id_sql = ''
    #if cnconf['different_external_ids'] == 1:
    #    id_sql = 'generate_opensrp_ids.sql'
    #else:
    #     id_sql = 'set_opensrp_ids_from_external.sql'
    #logging.info('   Running SQL: {0}'.format(id_sql))
    #with open('{0}/{1}'.format(sql_path, id_sql)) as sql_file:
    #    sql = sql_file.read()
    #    logging.debug(sql)
        #cur.execute(sql)
        #conn.commit()

    #if cnconf['add_name_suffix'] == 1:
     #   logging.info('   Running SQL: {0}'.format('add_suffix.sql'))
     #   with open('{0}/{1}'.format(sql_path, 'add_suffix.sql')) as sql_file:
     #       sql = sql_file.read()
     #       logging.debug(sql)
            #ur.execute(sql)
            #conn.commit()

    logging.info('Completed load_files successfully')

def total_structures(geographic_level):
    sql = "SELECT count(*)  from mergeset  \
        where operation = 'i' and geographicLevel = " + str(geographic_level)
    cur = conn.cursor()
    cur.execute(sql)
    counts = cur.fetchall()
    cur.close()
    return counts[0][0]

def create_reveal_structure_geojson(id_opensrp, coordinates, id_parent_opensrp, location_name, geographicLevel, id_external, geometry_type):
    logging.debug(locals())
    revealpost = json.dumps(revealtemplate_structure)
    URL = url_post_reveal_structure.format(url_sd)
    revealpost = revealpost.replace('$id_opensrp', str(id_opensrp))
    revealpost = revealpost.replace('1111111111', coordinates)
    revealpost = revealpost.replace(
        '$id_parent_opensrp', str(id_parent_opensrp))
    revealpost = revealpost.replace('2222222222', str(geographicLevel))
    revealpost = revealpost.replace('$id_external', id_external)
    revealpost = revealpost.replace('$type', geometry_type)
    logging.debug(revealpost)
    return json.loads(revealpost)

def load_structures():
    global ou_table
    global locations_progress
    global jurisdiction_only
    global firstrun
    global urlsd;

    geographic_level = int(jurisdiction_depth) + 1
    structures_total = total_structures(geographic_level)
    logging.info('Total structures to add: {0}'.format(structures_total))

    sql = "SELECT s.opensrp_id, s.externalId, s.externalParentId, s.name_en, s.name, s.geographicLevel, s.type, s.coordinates, 	s.openmrs_id , j.opensrp_id as opensrpparent_id	\
		from  mergeset s											\
		inner join mergeset j on j.externalId = s.externalparentId    \
		where s.operation = 'i' and s.geographicLevel = " + str(geographic_level) + " order by s.id"
    logging.debug(sql)
    cur = conn.cursor()
    cur.execute(sql)
    locations = cur.fetchall()
    cur.close()
    structures = []
    counter = 0
    logging.debug(len(locations))
    for l in locations:
        location = {}
        location['id_opensrp'] = l[0]
        location['id_external'] = l[1]
        location['parent_id_external'] = l[2]
        location['name_en'] = l[3]
        location['name_local'] = l[4]
        location['geographic_level'] = l[5]
        location['geometry_type'] = l[6]
        location['coordinates'] = l[7]
        location['id_openmrs'] = l[8]
        location['parent_id_openmrs'] = l[9]
        locations_progress += 1
        logging.debug("{7}/{8} -- id_external: {0} parent_id_exernal: {1} id_opensrp: {2}, parent_id_opensrp: {3} id_openmrs: {4} parent_id_opemmrs: {5}, db_gl: {6}".format(
            location['id_external'], location['parent_id_external'], location['id_opensrp'], location['parent_id_openmrs'], 'na', '', location['geographic_level'], locations_progress, structures_total))
        counter += 1
        structures.append(create_reveal_structure_geojson(location['id_opensrp'], location['coordinates'],  location[
                          'parent_id_openmrs'], location['name_local'], geographic_level, location['id_external'], location['geometry_type']))
        if(counter % 500 == 0) or counter == len(locations):
            URL = url_post_reveal_structure_batch.format(url_sd)
            logging.info(URL)
            c = post_request(URL, json.dumps(structures),'i')
            logging.info(str(counter) + ' ' + c.text +
                         ' ' + location['id_external'])
            structures = []
    return

def test_oauth():
    client_id = cnconf['client_id']
    client_secret = cnconf['client_secret']
    username = cnconf['username']
    password = cnconf['password']
    token_url = cnconf['token_url']
    redirect_uri =''
    authorization_response = ''
    scope = ''
    access_token = None
    print(client_id)
    print(client_secret)
    print(token_url)
    oauth = OAuth2Session(client=LegacyApplicationClient(client_id=client_id))
    token = oauth.fetch_token(token_url=token_url,
        username=username, password=password, client_id=client_id,
        client_secret=client_secret)
    r = oauth.get('https://reveal-th-preview.smartregister.org/opensrp/rest/plans/e167b3e1-1991-487b-abc7-2e880c3df564')
    print(r.text)
    mrs = json.loads(r.text)

    txt = json.dumps(mrs[0]) 
    #.encode('utf-8')
    print(txt)
    headers = {'content-type':'application/json'}
    r = oauth.put('https://reveal-th-preview.smartregister.org/opensrp/rest/plans', data=txt, headers = headers)
    #r = oauth.post('https://reveal-th-preview.smartregister.org/opensrp/rest/plans', data=mrs)
    print(r)

def add_locations_local_preview():
    local_conf = config['th-pl']
    preview_conf = config['th-pv']
    local_oauth = OAuth2Session(client=LegacyApplicationClient(client_id=local_conf['client_id']))
    preview_oauth = OAuth2Session(client=LegacyApplicationClient(client_id=preview_conf['client_id']))
    local_token = local_oauth.fetch_token(token_url=local_conf['token_url'], username=local_conf['username'], password=local_conf['password'], client_id=local_conf['client_id'], client_secret=local_conf['client_secret'])
    preview_token = preview_oauth.fetch_token(token_url=preview_conf['token_url'], username=preview_conf['username'], password=preview_conf['password'], client_id=preview_conf['client_id'], client_secret=preview_conf['client_secret'])
    
    #local_jurisdictions = local_oauth.get('https://servermhealth.ddc.moph.go.th/opensrp/rest/location/findByProperties?is_jurisdiction=true&return_geometry=true&properties_filter=geographicLevel:4,status:Active')
    local_jurisdictions = local_oauth.get('https://servermhealth.ddc.moph.go.th/opensrp/rest/location/getAll?is_jurisdiction=true&serverVersion=0&limit=50000&return_geometry=true')
    
    local_js = json.loads(local_jurisdictions.text)
    #preview_jurisdictions = preview_oauth.get('https://reveal-th-preview.smartregister.org/opensrp/rest/location/findByProperties?is_jurisdiction=true&return_geometry=true&properties_filter=geographicLevel:4,status:Active')
    preview_jurisdictions = preview_oauth.get('https://reveal-th-preview.smartregister.org/opensrp/rest/location/getAll?is_jurisdiction=true&serverVersion=0&limit=50000&return_geometry=true')
    preview_js = json.loads(preview_jurisdictions.text)
    print(len(local_js))
    print(len(preview_js))
    for ll in local_js:
        print(ll['properties']['name'])
        loc_exists = location_exists(preview_js, ll['id'])
        if loc_exists == False:
            txt = json.dumps(ll) 
            #.encode('utf-8')
           # print(txt)
            headers = {'content-type':'application/json'}
            r = preview_oauth.post('https://reveal-th-preview.smartregister.org/opensrp/rest/location/?is_jurisdiction=true', data=txt, headers = headers)
    print(len(local_js))
    print(len(preview_js))
    #https://reveal-th-preview.smartregister.org/opensrp/rest/location/findByProperties?is_jurisdiction=true&return_geometry=true&properties_filter=geographicLevel:1,status:Active

def post_user(team_id, user_id, user_name):
    local_conf = config['th-pl']
    local_oauth = OAuth2Session(client=LegacyApplicationClient(client_id=local_conf['client_id']))
    local_token = local_oauth.fetch_token(token_url=local_conf['token_url'], username=local_conf['username'], password=local_conf['password'], client_id=local_conf['client_id'], client_secret=local_conf['client_secret'])
    headers = {'content-type':'application/json'}

    #add user to Keycloak

    #get userid from Keycloak

    #set user password in Keycloak

    #join Provider group in keycloak

    #link roles in Keycloak 

    #add user to OpenSRP
    user = {}
    user["identifier"] = user_id #THIS NOW COMES FROM KEYCLOAK ABOVE 
    user["active"] = True
    user["userId"] = user_id  #THIS NOW COMES FROM KEYCLOAK ABOVE (same as idenifier)
    user["name"] = user_name
    user["username"] = user_name
    txt = json.dumps(user) 
    print(txt)
    headers = {'content-type':'application/json'}
    r = local_oauth.post('https://servermhealth.ddc.moph.go.th/opensrp/rest/practitioner', data=txt, headers = headers)
    print(r)

    #add practitioner role to OpenSRP (linking the user/practitioner to the team/organization)
    role = {}
    role['code'] = {"text": "Community Health Worker"}
    role['identifier'] = str(uuid.uuid4())
    role['active'] = True
    role['organization'] = team_id
    role['practitioner'] = user_id
    txt = json.dumps(role) 
    print(role)
    headers = {'content-type':'application/json'}
    r = local_oauth.post('https://servermhealth.ddc.moph.go.th/opensrp/rest/practitionerRole', data=txt, headers = headers)
    print(r)

def post_team(team_name):
    local_conf = config['th-pl']
    local_oauth = OAuth2Session(client=LegacyApplicationClient(client_id=local_conf['client_id']))
    local_token = local_oauth.fetch_token(token_url=local_conf['token_url'], username=local_conf['username'], password=local_conf['password'], client_id=local_conf['client_id'], client_secret=local_conf['client_secret'])
    
    team = {}
    team['identifier'] = str(uuid.uuid4())
    team['active'] = True
    team['name'] = team_name
    team['type'] = {"coding": [{"system":"http://terminology.hl7.org/CodeSystem/organization-type","code": "team","display": "Team"}]}
    
    txt = json.dumps(team) 
    print(txt)
    headers = {'content-type':'application/json'}
    r = local_oauth.post('https://servermhealth.ddc.moph.go.th/opensrp/rest/organization', data=txt, headers = headers)
    print(r)

def setup_users():
    counter = 0
    # with open('teams.csv') as ff:
    #     for line in ff:
    #         if counter % 10 == 0:
    #             logging.info("{0}".format(counter))
    #         counter+=1
    #         #team_name,team_id,user_name,user_id,practitionerrole_id
    #         i=line.rstrip().split(',')
    #         post_team(i[0])
    
    with open('users.csv') as f:
        for line in f:
            if counter % 10 == 0:
                logging.info("{0}".format(counter))
            counter+=1
            #team_id, user_id, user_name
            print(line)
            i=line.rstrip().split(',')
            post_user(i[0],i[1],i[2])
            logging.debug(line.split('\r\n')[0])



def location_exists(arr, id):
    exists = False;
    for l in arr:
        if l['id']==id:
            exists = True
            break
    return exists

def ensure(var, name):
    if var == None:
        sys.stderr.write('error: {} not found\n'.format(name))
        sys.exit(1)
    return var

def get_oauth2_session(client_id, client_secret, access_token):
    if access_token == None:
        token_url = cnconf['token_url']
        session = OAuth2Session(client = BackendApplicationClient(client_id = client_id))
        ensure(client_secret, 'client secret')
        try:
            session.fetch_token(token_url = token_url, client_id = client_id, client_secret = client_secret)
        except:
            raise Exception('failed to get access token: {}'.format(format_exc()))
    else:
        session = OAuth2Session(token = { 'access_token': access_token })
    return session

def main(argv):
    logging.info("start")
    global conn
    
    function = 'none'
    external_parent_id = ''
    geographic_level = 0
    global jurisdiction_depth
    global level
    global test_run
    openmrs_root_location_uuid_in = ''
    opensrp_root_location_uuid_in = ''
    openmrs_name = ''
    global ou_table
    global put
    global cnconf
    global locations_total
    global url_sd
    global country
    global skip_csv

    try:
        opts, args = getopt.getopt(argv, "hf:so:t:l:Tjpe:d:m:c:", [
                                   "funct=", "skip_csv", "oname=", "table=", "level=", "testrun", "jurisdiction_only", "put", "external_parent_id=", "depth=", "openmrs_root=", "country="])
    except getopt.GetoptError as e:
        print('Use e -h for help')
        print(e)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('dsme_location_management.py -f <function>')
            sys.exit()
        elif opt in ("-f", "--function"):
            function = arg
        elif opt in ("-e", "--external_parent_id"):
            external_parent_id = arg
        elif opt in ("-t", "--table"):
            ou_table = arg
        elif opt in ("-T", "--testrun"):
            test_run = True
        elif opt in ("-c" "--country"):
            country = arg
        elif opt in ("-s" "--skip_csv"):
            skip_csv = 1
    logging.info("function: {0}".format(function))
    #all of these functions require a country code to get configuration
    if function == 'locations':
        add_locations_local_preview()
        return
    if country == '':
    	print('Please specify a country and environment e.g. th-st')
    	sys.exit()
    else:
    	cnconf = config[country]

    database = '{0}_{1}'.format(config['db']['database'],country.replace('-','_'))
    logging.info('database: {0}'.format(database))
    conn = psycopg2.connect(host=config['db']['host'], database=database, user=config['db']['user'], password=config['db']['password'])

	#functions
    if function == 'load_jurisdictions':
        logging.info("start")
        jurisdiction_depth = cnconf['jurisdiction_depth']
        url_sd = cnconf['url_sd']
        logging.info("create_hierarchy")
        if external_parent_id == '':
            print('Please specify the externalid of the root location (e.g. the country external id (for thailand this is "0")')
        else:
            locations_total = total_locations(external_parent_id)
            logging.info('Total locations to add: {0}'.format(locations_total))
            load_jurisdictions(external_parent_id)
    elif function == 'load_files':
        load_files()
    elif function == 'oauth':
        test_oauth()
    elif function == 'setup_users':
        setup_users()
    elif function == 'load_structures':
        url_sd = cnconf['url_sd']
        jurisdiction_depth = cnconf['jurisdiction_depth']
       # if ou_table == '':
        if country == '':
            print('Please specify a country and environment e.g. th-st')
        else:
            load_structures()
    conn.close()


if __name__ == '__main__':
    main(sys.argv[1:])
