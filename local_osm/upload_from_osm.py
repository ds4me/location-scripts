#!/usr/bin/env python3
# encoding: utf-8
import osmapi
import geopandas as gpd
import requests
import json
import overpy
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

# osm credentials
REAL_OSM_USER = config['public_osm']['username']
REAL_OSM_PASS = config['public_osm']['password']

# bvbdosm credentials
LOCAL_OSM_URL = config['local_osm']['url']
LOCAL_OSM_USER = config['local_osm']['username']
LOCAL_OSM_PASS = config['local_osm']['password']

# Required changeset description
CHANGESET_DESCRIPTION = 'Add all existing foci'


# TODO: Add in addition information from the masterlist - classification, english name, etc.
def add_masterlist_info(way):
    externalId = way.tags['description'].strip()
    name = way.tags["name"].strip()
    source = way.tags['source'].strip()

    newTags = {
        'area': 'yes',
        'description': externalId,
        'name': name,
        'source': source,
    }
    way.tags = newTags
    # print(f'way tags: {way.tags}')
    return way

def push_to_local_osm(ways):
    localApi = osmapi.OsmApi(api=LOCAL_OSM_URL, username=LOCAL_OSM_USER, password=LOCAL_OSM_PASS)
    localApi.ChangesetCreate({u"comment": u"%s" % CHANGESET_DESCRIPTION})

    currCount = 0
    for way in ways:
        createdNodes = []
        currCount += 1
        print(way)
        way = add_masterlist_info(way)
        for node in way.nodes:
            newNode = localApi.NodeCreate({u"lon": node.lon, u"lat": node.lat, u"tag": {}})
            createdNodes.append(newNode['id'])

        # add first node to the end so that it closes the way
        createdNodes.append(createdNodes[0])

        newWay = localApi.WayCreate({u"nd": createdNodes, u"tag": way.tags})
        print(f'newWay ({currCount}/{len(ways)}): {newWay}')

    localApi.ChangesetClose()


# TODO: Add the source string for WHO enumerated foci
def get_osm_ways():
    print('Running Overpass query to get OSM ways, note that this may take a bit...')
    overpassApi = overpy.Overpass()
    r = overpassApi.query("""
    area["name:en"="Thailand"] ->.a;
    way["source"~"BVBDMAY2019|WHO2019"]["description"~"."](area.a);
    (._;>;);
    out geom;
    """)
    print(f'num ways: {len(r.ways)}')
    return r.ways


ways = get_osm_ways()
push_to_local_osm(ways)
