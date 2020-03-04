DRAFT

- get the production locations 
   At present this involves logging into a remote machine, running the materialize_view script on the production opensrp database and then scp-ing the files to the local server
   run ./get_csv.sh from this directory with a whitelisted IP
- add the geojson to the toimport/geojson folder (i usually convert to .json for readability)
- insert into database
   Import the csv files structures and locations from the above step files into the database - table name masterlist 

** following 
- get the changeset and load into the database
   get the files from the gdrive request folder and add them to the toimport folder
   use the script to load the change set files into the database
   *python upload.py -f load_files*
- create mergeset in the database
   parse the geojson files into a changeset table (one row per location)
   run the merge sql to create the changeset 
      this will identify inserts and updates as well as rows that have issues and should not be uploaded
      uuids will be added if necessary
      checks will be performed
	- unique names
	- unique external ids
	- unique uuids
	- correct geographic level
 	- names exist
	- complete hierarchy
- validate that rows marked with operations i and u (insert and update) are correct
- run upload
	*python upload.py -f load_jurisdictions -c th-st -t merge_002*
	*python upload.py -f load_structures -c th-st -t merge_002*
- copy mergeset database table, export csv and upload to gdrive request folder 
