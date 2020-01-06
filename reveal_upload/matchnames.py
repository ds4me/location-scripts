import psycopg2
import requests
import json
import uuid
import logging	
import sys, getopt, os
import configparser
from datetime import datetime


reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(filename='./logs/match.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s',level=logging.INFO)
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


url_get_reveal_jurisdiction= "https://{0}.smartregister.org/opensrp/rest/location/{1}?is_jurisdiction=true" 
url_get_reveal_jurisdictions ="https://{0}.smartregister.org/opensrp/rest/location/findByProperties?is_jurisdiction=true&return_geometry=true&properties_filter=parentId:{1}"#.format(url_sd, "%") 
url_get_reveal_structure = "https://{0}.smartregister.org/opensrp/rest/location/findByProperties?is_jurisdiction=false&return_geometry=true&properties_filter=parentId:{1}"#.format(url_sd, "%") 
url_get_openmrs_locations = "https://openmrs.{0}.smartregister.org/openmrs/ws/rest/v1/location/{1}"
url_post_reveal_jurisdiction = "https://{0}.smartregister.org/opensrp/rest/location/?is_jurisdiction=true"
url_post_reveal_structure = "https://{0}.smartregister.org/opensrp/rest/location/?is_jurisdiction=false"
url_post_openmrs_location = "https://openmrs.{0}.smartregister.org/openmrs/ws/rest/v1/location"
url_get_openmrs_locations_name = "https://openmrs.{0}.smartregister.org/openmrs/ws/rest/v1/location?q={1}"
url_get_openmrs_attrbutes = "http://openmrs.{0}.smartregister.org/openmrs/ws/rest/v1/location/{1}/attribute/{2}"
url_post_reveal_structure_batch = "https://{0}.smartregister.org/opensrp/rest/location/add?is_jurisdiction=false"


#conn = psycopg2.connect(host="localhost",database="test", user="postgres", password="")
config.read('./config.ini')

def get_request(URL):
	auth = eval(cnconf['openmrs_auth']) if 'openmrs' in URL  else eval(cnconf['reveal_auth'])
	logging.debug(auth)
	response = requests.get(URL, auth=auth)
	return response

def post_request(URL, body, operation):
	logging.info(locals())
	auth = eval(cnconf['openmrs_auth']) if 'openmrs' in URL  else eval(cnconf['reveal_auth'])
	response = ''
	if operation == 'i':
		response = requests.post(URL, data = body, auth=auth, headers={"Content-Type" : "application/json"})	
	elif operation == 'u':
		response = requests.put(URL, data = body, auth=auth, headers={"Content-Type" : "application/json"})	
	return response

def checkLocation(opensrpid):
	URL = url_get_reveal_jurisdiction.format(url_sd,opensrpid)
	logging.debug(URL)
	c = get_request(URL)
	if(c.status_code == 200):
		logging.debug(c.text)
		rvl = json.loads(c.text)
	 	name = rvl["properties"]["name"]
	 	name2 = ' '.join(name.split())
	 	space_removed = False
	 	openmrs_id_rvl = rvl["properties"]["OpenMRS_Id"]
	 	URL = url_get_openmrs_locations.format(url_sd,openmrs_id_rvl)
	 	c = get_request(URL)
		mrs = json.loads(c.text)
		openmrs_name = mrs["name"]
		openmrs_display = mrs["display"]
		if openmrs_name != name or openmrs_name != name:
			logging.info("UNMATCHED {0}".format(opensrpid))
	else:
		logging.info('Error: ' + c.text)

def CheckNamesFromFile():
	counter = 0
	with open('foci.csv') as f:
		for line in f:
			if counter % 10 == 0:
	 			logging.info("{0}".format(counter))
	 		counter+=1
			logging.debug(line.split('\r\n')[0])
			checkLocation(str(line.split('\r\n')[0])) 

def checkNamesFromDB():

	sql = 'SELECT opensrp_id from foci_all;'
	logging.debug(sql)
	cur = conn.cursor()
	cur.execute(sql)
	locations = cur.fetchall()
	cur.close()
	counter = 0
	logging.info(len(locations))
	url_sd = cnconf["url_sd"]
	for l in locations: 
		opensrpid = l[0]
	 	counter += 1
	 	if counter % 50 == 0:
	 		 logging.info("{0}/{1}".format(counter,len(locations)))

	 	checkLocation(opensrpid)
	 	

	return

def fixNames():
	opensrpids =  ['edcc316c-1472-4f49-bf21-7351abf70903' \
	,'5a5d7256-406d-4c99-b7ba-c77217d0f1ea' \
	,'b59e3554-2f08-42c0-80e8-f37c8d32316b' \
	,'4d0d1395-ad82-4b70-9499-efd19282da8d' \
	,'d9d85c16-bcbe-442d-a7f7-6cc8aa036889' \
	,'516f514a-ac16-499e-8d71-65600a23b34e' \
	,'358d2454-aa06-4c38-8903-629818177a63' \
	,'fc66d63e-4e84-4405-b434-054db248cfe0' \
	,'3dc47fc2-2b66-4df1-affa-c47113ece2dd' \
	,'fd49ddbc-a15c-4b10-bbb4-29af4d746340' \
	,'f405e42c-c25e-4a9a-babe-69e74fa55c73' \
	,'6c669867-f8fa-462f-b463-caecf87fc187' \
	,'c69b5d8c-2dbe-4e29-a751-0a36c959ba6c' \
	,'1731f9f8-b0bf-4c24-89d8-4febc04f8f42' \
	,'9846ebe7-ac6d-4155-a818-841c6581f1cb' \
	,'c977dbba-fcb0-4ad0-af94-cd2f07ada983' \
	,'439ba740-dd9d-4705-be70-bb5eb572ba48'] 

	for i in opensrpids:
		logging.info(i)
		URL = url_get_reveal_jurisdiction.format(url_sd,i)
		logging.debug(URL)
		c = get_request(URL)
		logging.debug(c.text)
		rvl = json.loads(c.text)
		name = rvl["properties"]["name"]

		openmrs_id_rvl = rvl["properties"]["OpenMRS_Id"]
	 	URL = url_get_openmrs_locations.format(url_sd,openmrs_id_rvl)
	 	c = get_request(URL)
		mrs = json.loads(c.text)
		mrs["name"] = name
		mrs["name"] = name
		URL =  "https://openmrs.reveal-th.smartregister.org/openmrs/ws/rest/v1/location/{0}".format(openmrs_id_rvl)
		logging.info(URL)
		logging.info(json.dumps(mrs, ensure_ascii=False).encode('utf8'))
		c = post_request(URL, json.dumps(mrs, ensure_ascii=False).encode('utf-8'), 'i')
		logging.info(c.status_code)

def fixParents():
	#sql 
	global conn 
	conn = psycopg2.connect(host=config['db']['host'],database=config['db']['database'], user=config['db']['user'], password=config['db']['password'])
	sql = 'SELECT opensrp_id, openmrs_id, new_parentopensrp_id, new_parentopenmrs_id from fix_parent  ;'
	cur = conn.cursor()
	cur.execute(sql)
	locations = cur.fetchall()
	cur.close()
	counter = 0
	logging.info(len(locations))
	url_sd = cnconf["url_sd"]
	for l in locations: 

		#get rvl
		URL = url_get_reveal_jurisdiction.format(url_sd,l[0])
		logging.debug(URL)
		c = get_request(URL)
		if(c.status_code == 200):
			logging.debug(c.text)
			rvl = json.loads(c.text)
			rvl['properties']['parentId'] = l[2]
			URL = url_post_reveal_jurisdiction.format(url_sd)
			body = json.dumps(rvl, ensure_ascii=False).encode('utf-8')
			logging.info(body)
			c = post_request(URL, body, 'u')
			logging.info('Reveal parent change status: {0}'.format(c.status_code))

			URL = url_get_openmrs_locations.format(url_sd,l[1])
			c = get_request(URL)
			if(c.status_code == 200):
				mrs = json.loads(c.text)
				#parentjson = json.loads('{"uuid": "{0}"}'.format(l[3))
				mrs['parentLocation'] = {}
				mrs['parentLocation']['uuid'] = l[3]
				body = json.dumps(mrs, ensure_ascii=False).encode('utf-8')
				logging.info(body)
				URL =  "https://openmrs.reveal-th.smartregister.org/openmrs/ws/rest/v1/location/{0}".format(l[1])
				logging.info(URL)
				c = post_request(URL, body, 'i')
				logging.info('OpenMRS parent change status: {0}'.format(c.status_code))
	conn.close()


def main(argv):	
	logging.info('start')
	#global conn 
	#conn = psycopg2.connect(host=config['db']['host'],database=config['db']['database'], user=config['db']['user'], password=config['db']['password'])
	global cnconf
	global url_sd
	country = 'th-pr'
	cnconf = config[country]
	logging.info(cnconf)
	
	fixParents()
	#conn.close()

if __name__ == '__main__':
	main(sys.argv[1:])







