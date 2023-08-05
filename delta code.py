# Databricks notebook source
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 31 22:35:19 2023

@author: achyut
"""

import requests
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from datetime import datetime
from dateutil import parser
import re
from bs4 import BeautifulSoup

# Create a Spark session
spark = SparkSession.builder.appName("HousingData").getOrCreate()

# Delta table name
delta_table_name = "listings"
delta_table_path = "/dbfs/housing_boston/listings"

# Create or replace the Delta tabl

# Function to fetch and process data
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
        
    else:
        page_num = 1
        last_page_num = None
        
        while True:
            # Append the current page number to the URL
            current_url = f"{url}&page={page_num}"
            
            if page_num % 25 == 0:
                print(f'Page {page_num} fetched')
            
            # Make an HTTP request to the URL
            response = requests.get(current_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the listings on the current page
            listings = soup.find('div', class_='search_results_area').find_all('div', class_='property_item')
            if not listings:
                print("No listings found on this page.")
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
                    price = listing_details[0].text.strip()
                except IndexError:
                    pass

                try:
                    n_beds = listing_details[1].text.strip()
                except IndexError:
                    pass

                try:
                    n_baths = listing_details[2].text.strip()
                except IndexError:
                    pass

                try:
                    avbl_date = listing_details[3].text.strip()
                except IndexError:
                    pass
                
                location = listing.find('a', class_='item_title')
                if location:
                    location = location.text.strip()
                utilities = listing.find('a')
                if utilities:
                    utilities = utilities['href']
                amenities = listing.find('a')
                if amenities:
                    amenities = amenities['href']
                save_to_db(None, price, n_beds, n_baths, avbl_date, location, utilities, amenities, agent_name, agent_email, agent_phone, None)
            
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

# Function to save data to Delta table
def save_to_db(last_updated, price, n_beds, n_baths, avbl_date, location, utilities, amenities, agent_name, agent_email, agent_phone, agent_fees):
    # Convert date strings to datetime objects
    last_updated = datetime.strptime(last_updated, "%Y-%m-%d").date() if last_updated else None
    avbl_date = datetime.strptime(avbl_date, "%Y-%m-%d").date() if avbl_date else None
    
    # Prepare data for insertion
    data = [(last_updated, price, n_beds, n_baths, avbl_date, location, utilities, amenities, agent_name, agent_email, agent_phone, agent_fees)]
    columns = ["last_updated", "price", "n_beds", "n_baths", "available_date", "location", "utilities", "amenities", "agent_name", "agent_email", "agent_phone", "agent_fees"]
    
    # Create a Spark DataFrame
    df = spark.createDataFrame(data, columns)
    
    # Write data to Delta table
    delta_table = DeltaTable.forPath(spark, delta_table_name)
    delta_table.alias("oldData").merge(df.alias("newData"), "oldData.location = newData.location") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

# Define schema for Delta table
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType, FloatType

schema = StructType([
    StructField("last_updated", DateType(), True),
    StructField("price", IntegerType(), True),
    StructField("n_beds", FloatType(), True),
    StructField("n_baths", FloatType(), True),
    StructField("available_date", DateType(), True),
    StructField("location", StringType(), True),
    StructField("utilities", StringType(), True),
    StructField("amenities", StringType(), True),
    StructField("agent_name", StringType(), True),
    StructField("agent_email", StringType(), True),
    StructField("agent_phone", StringType(), True),
    StructField("agent_fees", StringType(), True)
])

# Create or replace the Delta table
spark.sql(f"CREATE OR REPLACE TABLE {delta_table_name} USING DELTA LOCATION 'path/to/delta/table' TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')")

# Define Boston Pads tags
class WebpageData:
    def __init__(self, last_updated, price, n_beds, n_baths, avbl_date, building_address, area, agent_name, agent_email, agent_phone, agent_fees, heat, hot_water, electricity, parking, dish_washer, laundry_location, air_conditioning):
        self.last_updated = last_updated
        self.price = price
        self.n_beds = n_beds
        self.n_baths = n_baths
        self.avbl_date = avbl_date
        self.building_address = building_address
        self.area = area
        self.agent_name = agent_name
        self.agent_email = agent_email
        self.agent_phone = agent_phone
        self.agent_fees = agent_fees
        self.heat = heat
        self.hot_water = hot_water
        self.electricity = electricity
        self.parking = parking
        self.dish_washer = dish_washer
        self.laundry_location = laundry_location
        self.air_conditioning = air_conditioning

# Create an instance of the WebpageData class with the tags for Boston Pads website
boston_pads_tags = WebpageData('date_modified', 'price', 'bed_room', 'baths',
                               'date_available', 'building_address', 'sub_area_name',
                               'agent_full_name', 'agent_email', 'agent_phone',
                               'fee', 'heat', 'hot_water', 'electricity', 'parking', 
                               'dish_washer', 'laundry_location', 'air_conditioning')

def main():
    website_list = ["https://m.bostonpads.com/api/listings-short?location=boston&unique=1&results_per_page=5000", 
                    "https://ygl.is/bryn-allen-1?areas%5B%5D=Boston&areas%5B%5D=Boston%3AAllston&areas%5B%5D=Boston%3ABack+Bay&areas%5B%5D=Boston%3ABay+Village&areas%5B%5D=Boston%3ABeacon+Hill&areas%5B%5D=Boston%3ABrighton&areas%5B%5D=Boston%3ACharlestown&areas%5B%5D=Boston%3AChinatown&areas%5B%5D=Boston%3ADorchester&areas%5B%5D=Boston%3AEast+Boston&areas%5B%5D=Boston%3AFenway&areas%5B%5D=Boston%3AFinancial+District&areas%5B%5D=Boston%3AFort+Hill&areas%5B%5D=Boston%3AHyde+Park&areas%5B%5D=Boston%3AJamaica+Plain&areas%5B%5D=Boston%3AKenmore&areas%5B%5D=Boston%3ALeather+District&areas%5B%5D=Boston%3AMattapan&areas%5B%5D=Boston%3AMidtown&areas%5B%5D=Boston%3AMission+Hill&areas%5B%5D=Boston%3ANorth+End&areas%5B%5D=Boston%3ARoslindale&areas%5B%5D=Boston%3ARoxbury&areas%5B%5D=Boston%3ASeaport+District&areas%5B%5D=Boston%3ASouth+Boston&areas%5B%5D=Boston%3ASouth+End&areas%5B%5D=Boston%3ATheatre+District&areas%5B%5D=Boston%3AWaterfront&areas%5B%5D=Boston%3AWest+End&areas%5B%5D=Boston%3AWest+Roxbury&page=1",
                    "https://ygl.is/andi-bauer-1?areas%5B%5D=Brookline&areas%5B%5D=Brookline%3ABeaconsfield&areas%5B%5D=Brookline%3ABrookline+Hills&areas%5B%5D=Brookline%3ABrookline+Village&areas%5B%5D=Brookline%3AChestnut+Hill&areas%5B%5D=Brookline%3ACoolidge+Corner&areas%5B%5D=Brookline%3ALongwood&areas%5B%5D=Brookline%3AReservoir&areas%5B%5D=Brookline%3AWashington+Square&areas%5B%5D=Cambridge&areas%5B%5D=Cambridge%3AAgassiz&areas%5B%5D=Cambridge%3ACambridge+Highlands&areas%5B%5D=Cambridge%3ACambridgeport&areas%5B%5D=Cambridge%3ACentral+Square&areas%5B%5D=Cambridge%3AEast+Cambridge&areas%5B%5D=Cambridge%3AHarvard+Square&areas%5B%5D=Cambridge%3AHuron+Village&areas%5B%5D=Cambridge%3AInman+Square&areas%5B%5D=Cambridge%3AKendall+Square&areas%5B%5D=Cambridge%3AMid+Cambridge&areas%5B%5D=Cambridge%3ANeighborhood+Nine&areas%5B%5D=Cambridge%3ANorth+Cambridge&areas%5B%5D=Cambridge%3APorter+Square&areas%5B%5D=Cambridge%3ARiverside&areas%5B%5D=Cambridge%3AWellington-Harrington&areas%5B%5D=Cambridge%3AWest+Cambridge&areas%5B%5D=Somerville&areas%5B%5D=Somerville%3AAssembly+Square&areas%5B%5D=Somerville%3ABall+Square&areas%5B%5D=Somerville%3ADavis+Square&areas%5B%5D=Somerville%3AEast+Somerville&areas%5B%5D=Somerville%3AInman+Square&areas%5B%5D=Somerville%3APowderhouse+Square&areas%5B%5D=Somerville%3AProspect+Hill&areas%5B%5D=Somerville%3ASpring+Hill&areas%5B%5D=Somerville%3ATeele+Square&areas%5B%5D=Somerville%3ATen+Hills&areas%5B%5D=Somerville%3AUnion+Square&areas%5B%5D=Somerville%3AWest+Somerville&areas%5B%5D=Somerville%3AWinter+Hill&page=1"
                    ]
    for url in website_list:
        fetch_data(url)

if __name__ == "__main__":
    main()


# COMMAND ----------

delta_table_name = "/dbfs/housing_boston/boston_rental_data"
if not DeltaTable.isDeltaTable(spark, delta_table_name):
    spark.sql(f"CREATE TABLE {delta_table_name} USING delta")
