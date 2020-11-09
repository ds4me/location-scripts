#!/bin/bash
ssh ubuntu@34.244.73.221 'bash -s' < get_views_namibia.sh
scp ubuntu@34.244.73.221:~/*.csv ./toimport/locations/na-pr


