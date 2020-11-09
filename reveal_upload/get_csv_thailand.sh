#!/bin/bash
ssh ubuntu@13.232.78.16 'bash -s' < get_views_thailand.sh
scp ubuntu@13.232.78.16:~/*.csv ./toimport/location/th-pr


