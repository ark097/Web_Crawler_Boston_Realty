# -*- coding: utf-8 -*-
"""
Created on Sat Aug  5 18:08:31 2023

@author: sasre
"""

location_list = ["allston", "backbay", "bayvillage", "beaconhill", "brighton", "charlestown", "chinatown", "dorchester",
                 "eastboston", "fenway", "financialdistrict", "forthill", "hydepark", "jamaicaplain", "kenmore","leatherdistrict",
                 "midtown", "missionhill", "northend", "roslindale", "roxbury", "seaportdistrict", "southboston", "southend", 
                 "theatredistrict", "waterfront", "westend", "westroxbury", "brookline", "cambridge", "somerville"]

location_dict = {'allston': 'Allston',
                             'backbay': 'Back Bay',
                             'bayvillage': 'Bay Village',
                             'beaconhill': 'Beacon Hill',
                             'brighton': 'Brighton',
                             'charlestown': 'Charlestown',
                             'chinatown': 'Chinatown',
                             'dorchester': 'Dorchester',
                             'eastboston': 'East Boston',
                             'fenway': 'Fenway',
                             'financialdistrict': 'Financial District',
                             'forthill': 'Fort Hill',
                             'hydepark': 'Hyde Park',
                             'jamaicaplain': 'Jamaica Plain',
                             'kenmore': 'Kenmore',
                             'leatherdistrict': 'Leather District',
                             'midtown': 'Midtown',
                             'missionhill': 'Mission Hill',
                             'northend': 'North End',
                             'roslindale': 'Roslindale',
                             'roxbury': 'Roxbury',
                             'seaportdistrict': 'Seaport District',
                             'southboston': 'South Boston',
                             'southend': 'South End',
                             'theatredistrict': 'Theatre District',
                             'waterfront': 'Waterfront',
                             'westend': 'West End',
                             'westroxbury': 'West Roxbury',
                             'brookline': 'Brookline',
                             'cambridge': 'Cambridge',
                             'somerville': 'Somerville'}


def find_area_bostonpads(location):
    
    if( "brookline" in location.lower()):
        return "Brookline"
    elif("cambridge" in location.lower()):
        return "Cambridge"
    elif("somerville" in location.lower()):
        return "Somerville"
    
    loc = (location.split("-")[1]).strip().lower().replace(" ", "")
    
    if loc in location_list:
        return location_dict[loc]
    else:
        return "Boston"
    

def find_area_ygl(location):
    
    if( "brookline" in location.lower()):
        return "Brookline"
    elif("cambridge" in location.lower()):
        return "Cambridge"
    elif("somerville" in location.lower()):
        return "Somerville"
    
    
    if ("(" in location):
        loc = (location.split("(")[1]).strip().lower().replace(")", "").replace(" ", "")
        
        if loc in location_list:
            return location_dict[loc]
        else:
            return "Boston"
    
    else:
        return "Boston"
        
        
        
        
        
        
        
        
        
        
        
        