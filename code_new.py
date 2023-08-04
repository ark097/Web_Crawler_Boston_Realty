#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 31 22:35:19 2023

@author: achyut
"""

import requests
import sqlite3
from WebpageClass import WebpageData

# Set up SQLite database and server

# Create a connection to the SQLite database
conn = sqlite3.connect('housing.db')

# Create a cursor object to interact with the database
cursor = conn.cursor()

boston_pads_tags = WebpageData('date_modified', 'price', 'bed_room', 'baths',
                               'date_available', 'building_address', 'street_address',
                               'city')

# Create a table to store the scraped data
cursor.execute('''
               CREATE TABLE IF NOT EXISTS bostonpads_data (
                   id INTEGER PRIMARY KEY,
                   apartment_name TEXT,
                   
                   last_updated TEXT,
                   price TEXT,
                   n_beds TEXT,
                   n_baths TEXT,
                   available_date TEXT,
                   location TEXT,
                   utilities TEXT,
                   amenities TEXT
                   )
               ''')
   
# Commit changes to the database
conn.commit()

def fetch_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for any bad response status (4xx, 5xx)

        data = response.json()
        if data:
            # Process the fetched data
            listings = data["data"]
            for listing in listings:
                site_tags = boston_pads_tags
                last_updated = listing.get(site_tags.last_updated)
                price = listing.get(site_tags.price)
                n_beds = listing.get(site_tags.n_beds)
                n_baths = listing.get(site_tags.n_baths)
                avbl_date = listing.get(site_tags.avbl_date)
                building_address = listing.get(site_tags.building_address, {})
                location = building_address.get(site_tags.location) or building_address.get(site_tags.city)
                utilities = None  # Fill in the correct field name if available in the data
                amenities = None  # Fill in the correct field name if available in the data
                save_to_db(last_updated, price, n_beds, n_baths, avbl_date, location, utilities, amenities)

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data: {str(e)}")
        return None

def save_to_db(last_updated, price, n_beds, n_baths, avbl_date, location, utilities, amenities):
    insert_query = 'INSERT INTO bostonpads_data (last_updated, price, n_beds, n_baths, available_date, location, utilities, amenities) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
    values = (last_updated, price, n_beds, n_baths, avbl_date, location, utilities, amenities)
    conn.execute(insert_query, values)
    conn.commit()

def main():
    url = "https://m.bostonpads.com/api/listings-short?location=boston&unique=1&results_per_page=4000"
    fetch_data(url)
    
if __name__ == "__main__":
    main()
