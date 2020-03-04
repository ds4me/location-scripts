import requests
import pyodbc

conn = pyodbc.connect('DSN=MYMSSQL; Database=db_mhealth; UID=SA; PWD=qFRD$HH2po;')
url_get_reveal_jurisdiction= "https://{0}.smartregister.org/opensrp/rest/location/{1}?is_jurisdiction=true" 

c = conn.cursor()

c.execute('select * from focus_masterlist')
for r in c:
	print(r)
