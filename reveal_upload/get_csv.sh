#!/bin/bash
ssh ubuntu@13.235.9.43 'bash -s' < get_views.sh
scp ubuntu@13.235.9.43:~/*.csv ./toimport/locations
