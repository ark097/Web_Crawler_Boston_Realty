# Databricks notebook source
# MAGIC %pip install requests beautifulsoup4 pyspark delta-sharing
# MAGIC

# COMMAND ----------

import requests
from dateutil import parser
import re
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType, FloatType

# Define the schema for the Delta table
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

class WebpageData:
    def __init__(self, last_updated, price, n_beds, n_baths, avbl_date, building_address,
                 area, agent_name, agent_email, agent_phone, agent_fees, heat,
                 hot_water, electricity, parking, dish_washer, laundry_location, air_conditioning):
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

# Create an instance of WebpageData with appropriate tag names
boston_pads_tags = WebpageData('date_modified', 'price', 'bed_room', 'baths',
                               'date_available', 'building_address', 'sub_area_name',
                               'agent_full_name', 'agent_email', 'agent_phone',
                               'fee', 'heat', 'hot_water', 'electricity', 'parking', 
                               'dish_washer', 'laundry_location', 'air_conditioning')



# Define the path to the Delta table
delta_table_path = "/FileStore/tables/listings"

def fetch_data(url):
    if "bostonpads" in url:
        try:
            print("Starting to scrape data.")
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            if data:
                # Process the fetched data
                listings = data["data"]
                print("Data available. Loading " + str(len(listings)) + " listings.")
                for listing in listings:
                    site_tags = boston_pads_tags
                    last_updated = parser.parse(listing.get(site_tags.last_updated)).date()
                    price = int(listing.get(site_tags.price).replace("$", "").replace(",",""))
                    n_beds = float(re.sub(r'[^0-9.]', '', listing.get(site_tags.n_beds))) if listing.get(site_tags.n_beds) not in [None, "0", ""] else 1.0
                    n_baths = float(re.sub(r'[^0-9.]', '', listing.get(site_tags.n_baths))) if listing.get(site_tags.n_baths) not in [None, "0", ""] else 1.0
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

def save_to_db(last_updated, price, n_beds, n_baths, avbl_date, location, utilities, amenities, agent_name, agent_email, agent_phone, agent_fees):
    global temp_data  # Declare a global DataFrame to accumulate data
    
    try:
        # Convert price to integer and handle any potential parsing errors
        price = int(price)
    except (TypeError, ValueError):
        print(f"Warning: Failed to parse price value '{price}' as integer. Setting to None.")
        price = None
    
    # Append data to the temp DataFrame
    new_row = (last_updated, price, n_beds, n_baths, avbl_date, location, utilities, amenities, agent_name, agent_email, agent_phone, agent_fees)
    temp_data = temp_data.union(spark.createDataFrame([new_row], schema=schema))
    
    # Print the count of rows in the temp DataFrame
    print(f"Temp DataFrame has {temp_data.count()} rows after appending.")

# Rest of the code remains the same

    

def main():
    global temp_data
    temp_data = spark.createDataFrame([], schema=schema)  # Initialize an empty temp DataFrame
    
    website_list = ["https://m.bostonpads.com/api/listings-short?location=boston&unique=1&results_per_page=5000", 
                    "https://ygl.is/bryn-allen-1?areas%5B%5D=Boston&areas%5B%5D=Boston%3AAllston&page=1",
                    "https://ygl.is/andi-bauer-1?areas%5B%5D=Brookline&areas%5B%5D=Brookline%3ABeaconsfield&page=1"
                    ]
    
    
    for url in website_list:
        fetch_data(url)
    # Display the contents of the Delta table
    delta_table_data = spark.read.format("delta").load(delta_table_path)
    delta_table_data.show()
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()


# COMMAND ----------

delta_table_data = spark.read.format("delta").load(delta_table_path)
display(delta_table_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS boston_housing;

# COMMAND ----------

# MAGIC %sql
# MAGIC  CREATE TABLE IF NOT EXISTS boston_housing.listings
# MAGIC USING DELTA
# MAGIC LOCATION '/dbfs/FileStore/tables/listings' -- Replace with the actual path
# MAGIC

# COMMAND ----------


