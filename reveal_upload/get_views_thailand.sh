#!/bin/bash
rm *.csv
psql -p 5432 -U dane -h postgres.reveal-th.smartregister.org opensrp -f materialise.sql

