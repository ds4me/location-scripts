#!/bin/bash
ssh  ubuntu@34.243.236.111 'bash -s' < get_views_thailand.sh
scp  ubuntu@34.243.236.111:~/*.csv ./toimport/locations


