#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 31 22:35:19 2023

@author: achyut
"""

import requests
import sqlite3
from WebpageClass import WebpageData
from dateutil import parser
import re

# Set up SQLite database and server

# Create a connection to the SQLite database
conn = sqlite3.connect('Housing_Data.db')

# Create a cursor object to interact with the database
cursor = conn.cursor()

boston_pads_tags = WebpageData('date_modified', 'price', 'bed_room', 'baths',
                               'date_available', 'building_address', 'sub_area_name',
                               'agent_full_name', 'agent_email', 'agent_phone',
                               'fee', 'heat', 'hot_water', 'electricity', 'parking', 
                               'dish_washer', 'laundry_location', 'air_conditioning')

# Create a table to store the scraped data
cursor.execute('''
               CREATE TABLE IF NOT EXISTS bostonpads_data (
                   id INTEGER PRIMARY KEY,                   
                   last_updated DATETIME,
                   price INT,
                   n_beds INT,
                   n_baths INT,
                   available_date DATETIME,
                   location TEXT,
                   utilities TEXT,
                   amenities TEXT,
                   agent_name TEXT,
                   agent_email TEXT,
                   agent_phone TEXT,
                   agent_fees TEXT
                   )
               ''')
   
# Commit changes to the database
conn.commit()

def fetch_data(url):
    try:
        print("Starting to scrape data.")
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for any bad response status (4xx, 5xx)

        data = response.json()
        if data:
            # Process the fetched data
            listings = data["data"]
            print("Data available. Loading " + str(len(listings)) + " listings.")

            for listing in listings:
                site_tags = boston_pads_tags
                last_updated = parser.parse(listing.get(site_tags.last_updated)).date()
                price = int(listing.get(site_tags.price).replace("$", "").replace(",",""))
                n_beds = float(re.sub(r'[^0-9.]', '', listing.get(site_tags.n_beds))) if listing.get(site_tags.n_beds) not in [None, "0", ""] else 1
                n_baths = float(re.sub(r'[^0-9.]', '', listing.get(site_tags.n_baths))) if listing.get(site_tags.n_baths) not in [None, "0", ""] else 1
                avbl_date = parser.parse(listing.get(site_tags.avbl_date))
                if(url == "https://m.bostonpads.com/api/listings-short?location=boston&unique=1&results_per_page=4000" 
                   and listing.get(site_tags.building_address) is not None):
                    location = listing.get(site_tags.building_address)["street_address"] or listing.get(site_tags.area)
                else:
                    location = listing.get(site_tags.building_address) or listing.get(site_tags.area)
                utilities = "Utilities paid for: " + ("Heat - Yes, " if listing.get(site_tags.heat) == True else "Heat - No, ") + \
                                                     ("Hot Water - Yes, " if listing.get(site_tags.hot_water) == "1" else "Hot Water - No, ") + \
                                                     ("Electricity - Yes, " if listing.get(site_tags.electricity) == True else "Electricity - No.")
                                                     
                amenities = "Utilities paid for: " + ("Parking - " + (listing.get(site_tags.parking) if listing.get(site_tags.parking) not in ["", None] else "No") + ", ") + \
                                                     ("Dish Washer - Yes, " if listing.get(site_tags.dish_washer) == True else "Dish Washer - No, ") + \
                                                     ("Laundry - " + (listing.get(site_tags.laundry_location) if listing.get(site_tags.laundry_location) not in ["", None] else "No") + ", ") + \
                                                     ("Air Conditioning - " + (listing.get(site_tags.air_conditioning) if listing.get(site_tags.air_conditioning) not in ["", None] else "No") + ".") 
                                                    
                agent_name = listing.get(site_tags.agent_name)
                agent_email = listing.get(site_tags.agent_email)
                agent_phone = listing.get(site_tags.agent_phone)
                agent_fees = "No" if listing.get(site_tags.agent_fees) == 0 else "Yes"
                save_to_db(last_updated, price, n_beds, n_baths, avbl_date, location,
                           utilities, amenities, agent_name, agent_email, agent_phone, agent_fees)
        
        else:
            print("No data available.")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data: {str(e)}")
        return None

def save_to_db(last_updated, price, n_beds, n_baths, avbl_date, location, utilities, amenities, agent_name, agent_email, agent_phone, agent_fees):
    insert_query = 'INSERT INTO bostonpads_data (last_updated, price, n_beds, n_baths, available_date, location, utilities, amenities, agent_name, agent_email, agent_phone, agent_fees) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    values = (last_updated, price, n_beds, n_baths, avbl_date, location, utilities, amenities, agent_name, agent_email, agent_phone, agent_fees)
    conn.execute(insert_query, values)
    conn.commit()

def main():
    url = "https://m.bostonpads.com/api/listings-short?location=boston&unique=1&results_per_page=4000"
    fetch_data(url)
    
if __name__ == "__main__":
    main()
