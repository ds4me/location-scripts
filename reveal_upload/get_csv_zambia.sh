#!/bin/bash
ssh ubuntu@18.202.22.195 'bash -s' < get_views_zambia.sh
scp ubuntu@18.202.22.195:~/*.csv ./toimport/locations
