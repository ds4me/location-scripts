#!/bin/bash
rm *.csv
psql -p 5432 -U pierre -h postgres.reveal-na.smartregister.org opensrp -f materialize.sql

