#!/bin/bash
<<<<<<< HEAD:reveal_upload/get_csv_thailand.sh
ssh ubuntu@13.235.9.43 'bash -s' < get_views_thailand.sh
scp ubuntu@13.235.9.43:~/*.csv ./toimport/locations
=======
ssh ubuntu@13.235.9.43 << EOF

	# Remove all the .csv files in the folder
	rm *.csv

	# Run materialise.sql to get new .csv files of the jurisdictions, structures, and tasks
	psql -p 5432 -U dane -h postgres.reveal-th.smartregister.org opensrp -f materialise.sql

EOF

scp ubuntu@13.235.9.43:~/*.csv ./toimport/locations/
>>>>>>> 884bde7788d6b41cbaec62d00963f031c95c77d9:reveal_upload/get_csv.sh
