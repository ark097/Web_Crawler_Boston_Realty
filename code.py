#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 31 22:35:19 2023

@author: achyut
"""

import requests
import sqlite3
from dateutil import parser
from datetime import date
from bs4 import BeautifulSoup
import re

from WebpageClass import WebpageData
from SubareaFinder import find_area_bostonpads, find_area_ygl

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
               CREATE TABLE IF NOT EXISTS boston_rental_data (
                   id INTEGER PRIMARY KEY,                   
                   last_updated DATETIME,
                   price INT,
                   n_beds FLOAT,
                   n_baths FLOAT,
                   available_date DATETIME,
                   location TEXT,
                   area TEXT,
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
    if "bostonpads" in url:
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
                    if(listing.get(site_tags.building_address) is not None):
                        location = listing.get(site_tags.building_address)["street_address"] or listing.get(site_tags.area)
                    else:
                        location = listing.get(site_tags.building_address) or listing.get(site_tags.area)
                    
                    area = find_area_bostonpads(listing.get(site_tags.area))
                    utilities = "Utilities paid for: " + ("Heat - Yes, " if listing.get(site_tags.heat) == True else "Heat - No, ") + \
                                                         ("Hot Water - Yes, " if listing.get(site_tags.hot_water) == "1" else "Hot Water - No, ") + \
                                                         ("Electricity - Yes, " if listing.get(site_tags.electricity) == True else "Electricity - No.")
                                                         
                    amenities = "Amenities provided: " + ("Parking - " + (listing.get(site_tags.parking) if listing.get(site_tags.parking) not in ["", None] else "No") + ", ") + \
                                                         ("Dish Washer - Yes, " if listing.get(site_tags.dish_washer) == True else "Dish Washer - No, ") + \
                                                         ("Laundry - " + (listing.get(site_tags.laundry_location) if listing.get(site_tags.laundry_location) not in ["", None] else "No") + ", ") + \
                                                         ("Air Conditioning - " + (listing.get(site_tags.air_conditioning) if listing.get(site_tags.air_conditioning) not in ["", None] else "No") + ".") 
                                                        
                    agent_name = listing.get(site_tags.agent_name)
                    agent_email = listing.get(site_tags.agent_email)
                    agent_phone = listing.get(site_tags.agent_phone)
                    agent_fees = "No" if listing.get(site_tags.agent_fees) == 0 else "Yes"
                    save_to_db(last_updated, price, n_beds, n_baths, avbl_date, location, area,
                               utilities, amenities, agent_name, agent_email, agent_phone, agent_fees)
            
            else:
                print("No data available.")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching data: {str(e)}")
            return None
        
    else:
        page_num = 1
        last_page_num = None
        
        while True:
            # Append the current page number to the URL
            current_url = f"{url}&page={page_num}"
            
            if page_num % 10 == 0:
                print(f'Page {page_num} fetched')
            
            # Make an HTTP request to the URL
            response = requests.get(current_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the listings on the current page
            listings = soup.find('div', class_='search_results_area').find_all('div', class_='property_item')
            if not listings:
                print("All listings from url fetched.")
                break
            
            # Fetch the broker details for the listings
            agent_details = soup.find('div', class_='contacts').find_all('li')
            if agent_details:
                agent_name = agent_details[0].text.strip()
                agent_email = agent_details[2].find('a', href=True)['href'].split(':')[1]
                agent_phone = agent_details[1].find('a', href=True)['href'].split(':')[1]
            
            # Process the listings on the current page
            for listing in listings:
                listing_details = listing.find('div', class_='item_props').find_all('div', class_='column')
                try:
                    price = int(re.sub(r'[^0-9]', '', listing_details[0].text.strip()))
                except IndexError:
                    pass

                try:
                    n_beds = re.sub(r'[^0-9.]', '', listing_details[1].text.strip())
                    n_beds = float(n_beds) if n_beds not in [None, "0", ""] else round(float(1), 1)
                except IndexError:
                    pass

                try:
                    n_baths = listing_details[2].text.strip()
                    n_baths = float(re.sub(r'[^0-9.]', '', n_baths)) if n_baths not in [None, "0", ""] else round(float(1), 1)
                except IndexError:
                    pass

                try:
                    avbl_date = re.sub(r'[^0-9/]', '', listing_details[3].text.strip()) 
                    avbl_date = parser.parse(avbl_date) if avbl_date not in ["",None] else date.today()
                except IndexError:
                    pass
                
                location = listing.find('a', class_='item_title')
                if location:
                    location = location.text.strip()
                    area = find_area_ygl(location)
                    
                utilities = listing.find('a')
                if utilities:
                    utilities = utilities['href']
                amenities = listing.find('a')
                if amenities:
                    amenities = amenities['href']
                save_to_db(None, price, n_beds, n_baths, avbl_date, location, area, utilities, amenities, agent_name, agent_email, agent_phone, "Yes")
            
            # Check for the last page
            counter_div = soup.find('div', class_='counter')
            if counter_div:
                page_text = counter_div.text.strip()
                match = re.search(r'\d+', page_text)
                if match:
                    current_page = int(match.group())
                    if last_page_num is None:
                        last_page_num = current_page
                    elif current_page == last_page_num:
                        print("Reached the last page.")
                        return
            
            # Increment the page number for the next iteration
            page_num += 1
            
            # END OF fetch_data()

def save_to_db(last_updated, price, n_beds, n_baths, avbl_date, location, area, utilities, amenities, agent_name, agent_email, agent_phone, agent_fees):
    insert_query = 'INSERT INTO boston_rental_data (last_updated, price, n_beds, n_baths, available_date, location, area, utilities, amenities, agent_name, agent_email, agent_phone, agent_fees) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    values = (last_updated, price, n_beds, n_baths, avbl_date, location, area, utilities, amenities, agent_name, agent_email, agent_phone, agent_fees)
    conn.execute(insert_query, values)
    conn.commit()

def main():
    website_list = ["https://m.bostonpads.com/api/listings-short?location=boston&unique=1&results_per_page=5000", 
                    "https://ygl.is/bryn-allen-1?areas%5B%5D=Boston&areas%5B%5D=Boston%3AAllston&areas%5B%5D=Boston%3ABack+Bay&areas%5B%5D=Boston%3ABay+Village&areas%5B%5D=Boston%3ABeacon+Hill&areas%5B%5D=Boston%3ABrighton&areas%5B%5D=Boston%3ACharlestown&areas%5B%5D=Boston%3AChinatown&areas%5B%5D=Boston%3ADorchester&areas%5B%5D=Boston%3AEast+Boston&areas%5B%5D=Boston%3AFenway&areas%5B%5D=Boston%3AFinancial+District&areas%5B%5D=Boston%3AFort+Hill&areas%5B%5D=Boston%3AHyde+Park&areas%5B%5D=Boston%3AJamaica+Plain&areas%5B%5D=Boston%3AKenmore&areas%5B%5D=Boston%3ALeather+District&areas%5B%5D=Boston%3AMattapan&areas%5B%5D=Boston%3AMidtown&areas%5B%5D=Boston%3AMission+Hill&areas%5B%5D=Boston%3ANorth+End&areas%5B%5D=Boston%3ARoslindale&areas%5B%5D=Boston%3ARoxbury&areas%5B%5D=Boston%3ASeaport+District&areas%5B%5D=Boston%3ASouth+Boston&areas%5B%5D=Boston%3ASouth+End&areas%5B%5D=Boston%3ATheatre+District&areas%5B%5D=Boston%3AWaterfront&areas%5B%5D=Boston%3AWest+End&areas%5B%5D=Boston%3AWest+Roxbury&page=1",
                    "https://ygl.is/andi-bauer-1?areas%5B%5D=Brookline&areas%5B%5D=Brookline%3ABeaconsfield&areas%5B%5D=Brookline%3ABrookline+Hills&areas%5B%5D=Brookline%3ABrookline+Village&areas%5B%5D=Brookline%3AChestnut+Hill&areas%5B%5D=Brookline%3ACoolidge+Corner&areas%5B%5D=Brookline%3ALongwood&areas%5B%5D=Brookline%3AReservoir&areas%5B%5D=Brookline%3AWashington+Square&areas%5B%5D=Cambridge&areas%5B%5D=Cambridge%3AAgassiz&areas%5B%5D=Cambridge%3ACambridge+Highlands&areas%5B%5D=Cambridge%3ACambridgeport&areas%5B%5D=Cambridge%3ACentral+Square&areas%5B%5D=Cambridge%3AEast+Cambridge&areas%5B%5D=Cambridge%3AHarvard+Square&areas%5B%5D=Cambridge%3AHuron+Village&areas%5B%5D=Cambridge%3AInman+Square&areas%5B%5D=Cambridge%3AKendall+Square&areas%5B%5D=Cambridge%3AMid+Cambridge&areas%5B%5D=Cambridge%3ANeighborhood+Nine&areas%5B%5D=Cambridge%3ANorth+Cambridge&areas%5B%5D=Cambridge%3APorter+Square&areas%5B%5D=Cambridge%3ARiverside&areas%5B%5D=Cambridge%3AWellington-Harrington&areas%5B%5D=Cambridge%3AWest+Cambridge&areas%5B%5D=Somerville&areas%5B%5D=Somerville%3AAssembly+Square&areas%5B%5D=Somerville%3ABall+Square&areas%5B%5D=Somerville%3ADavis+Square&areas%5B%5D=Somerville%3AEast+Somerville&areas%5B%5D=Somerville%3AInman+Square&areas%5B%5D=Somerville%3APowderhouse+Square&areas%5B%5D=Somerville%3AProspect+Hill&areas%5B%5D=Somerville%3ASpring+Hill&areas%5B%5D=Somerville%3ATeele+Square&areas%5B%5D=Somerville%3ATen+Hills&areas%5B%5D=Somerville%3AUnion+Square&areas%5B%5D=Somerville%3AWest+Somerville&areas%5B%5D=Somerville%3AWinter+Hill&page=1"
                    ]
    for url in website_list:
        fetch_data(url)
    
if __name__ == "__main__":
    main()
