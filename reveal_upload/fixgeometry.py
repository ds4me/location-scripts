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
from geojson_rewind import rewind
import pprint
import geojson
from io import StringIO
import geopandas

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
url_post_reveal_jurisdiction = "https://{0}.smartregister.org/opensrp/rest/location/?is_jurisdiction=true"
url_post_reveal_structure = "https://{0}.smartregister.org/opensrp/rest/location/?is_jurisdiction=false"
url_post_reveal_structure_batch = "https://{0}.smartregister.org/opensrp/rest/location/add?is_jurisdiction=false"

url_get_reveal_jurisdiction_sync = "https://{0}.smartregister.org/opensrp/rest/location/sync?is_jurisdiction=true&serverVersion={1}"

config.read('./config/config.ini')

def get_request(URL):
    auth = eval(cnconf['openmrs_auth']) if 'openmrs' in URL else eval(
        cnconf['reveal_auth'])
    response = requests.get(URL, auth=auth)
    return response

def post_request(URL, body, operation):
    # logging.info(locals())
    auth = eval(cnconf['openmrs_auth']) if 'openmrs' in URL else eval(
        cnconf['reveal_auth'])
    response = ''
    if operation == 'i':
        response = requests.post(URL, data=body, auth=auth, headers={
                                 "Content-Type": "application/json"})
    elif operation == 'u':
        response = requests.put(URL, data=body, auth=auth, headers={
                                "Content-Type": "application/json"})
    return response

def post_reveal_location(id_opensrp, coordinates, id_parent_opensrp, location_name, location_name_en, geographicLevel, id_external, geometry_type, id_openmrs, operation):
    logging.debug(locals())
    revealpost = json.dumps(revealtemplate)

    URL = url_post_reveal_jurisdiction.format(url_sd)
    revealpost = revealpost.replace('$name', location_name)
    revealpost = revealpost.replace('$nam_en', location_name_en)
    revealpost = revealpost.replace('$id_openmrs', str(id_openmrs))
    revealpost = revealpost.replace('$id_opensrp', str(id_opensrp))
    revealpost = revealpost.replace('1111111111', str(coordinates))
    revealpost = revealpost.replace('$id_parent_opensrp', str(id_parent_opensrp))
    revealpost = revealpost.replace('2222222222', str(geographicLevel))
    revealpost = revealpost.replace('$id_external', id_external)
    revealpost = revealpost.replace('$type', str(geometry_type))
    # todo: clean up string replacement above
    logging.debug('POST REVEAL URL: ' + URL)
    logging.debug('POST REVEAL BODY: ' + revealpost)

    if test_run == False:
        response = post_request(URL, revealpost, operation)
        logging.info(response.text)
    else:
        logging.debug("Skipping Reveal post - test run")

    # todo assuming this is successful - check status and deal with all cases
    # should stop iterating if there is an error
    return



def fix_jurisdictions():
    server_version = '0'
    URL = url_get_reveal_jurisdiction_sync.format(url_sd, server_version)
    print(URL)
    c = get_request(URL)
    print(c)
    mrs = json.loads(c.text)
    total = 0
    invalid = 0
    nullgeo = 0
    while len(mrs) > 0:
        
        print(c)
        logging.info(len(mrs))
        pp = pprint.PrettyPrinter(indent=4)
        fc = {} 
        fc['type']="FeatureCollection"
        fc['name']="geojson_for_shapefile"
        features = []
        nogeomlocations = []

        for l in mrs:
            #print(l)
            coords = l
            server_version = l['serverVersion'] +1 
            total += 1
            #print(geojson.loads(json.dumps(l['geometry'])).is_valid)
            try:
                if(coords!=rewind(coords)): #and coords['properties']['geographicLevel']==0):
                    invalid += 1
                #print("needs winding")
                    #pp.pprint(coords)
                   # pp.pprint(rewind(coords))
            #    URL = url_post_reveal_jurisdiction.format(url_sd) 
            #    payload = json.dumps(rewind(coords))
                #response = post_request(URL, payload, 'u')
                #logging.info(response.text)

           #if(coords['properties']['geographicLevel']==3):
                    features.append(l)
            except:
                pp.pprint(l)
                nogeomlocations.append(l['id'])
                nullgeo += 1
        print(len(mrs))
        print(URL)  
        URL = url_get_reveal_jurisdiction_sync.format(url_sd, server_version)
        print(URL)
        c = get_request(URL)
        print(c)
        mrs = json.loads(c.text)
    print(len(nogeomlocations))
    print(total)
    print(invalid)
    print(nullgeo)

    #fc['features'] = features
    #pp.pprint(fc);
    #f = open("./l3.geojson", "w+")
    #f.write(json.dumps(fc))
    #f.close()
    #l0 = geopandas.read_file("./l0.geojson")
   #l0.to_file("l0.shp")






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
    if country == '':
    	print('Please specify a country and environment e.g. th-st')
    	sys.exit()
    else:
    	cnconf = config[country]
    #database = '{0}_{1}'.format(config['db']['database'],country.replace('-','_'))
    #logging.info('database: {0}'.format(database))
    #conn = psycopg2.connect(host=config['db']['host'], database=database, user=config['db']['user'], password=config['db']['password'])

	#functions
    if function == 'fix_jurisdictions':
        logging.info("start")
        url_sd = cnconf['url_sd']
        fix_jurisdictions()
    #conn.close()


if __name__ == '__main__':
    main(sys.argv[1:])
