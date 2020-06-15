#!/bin/bash
rm *.csv
psql -p 5432 -U pierre -h reveal-stage.co1dqshnixcv.eu-west-1.rds.amazonaws.com  opensrp -f materialise.sql

