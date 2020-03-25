#!/bin/bash


	# Remove all the .csv files in the folder
	rm *.csv

	# Run materialise.sql to get new .csv files of the jurisdictions, structures, and tasks
	psql -p 5432 -U dane -h postgres.reveal-th.smartregister.org opensrp -f materialise.sql


