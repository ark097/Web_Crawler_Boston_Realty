# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


# NOTE: Install beautifulsoup4 and requests packages beforehand

# Import necessary libraries
from bs4 import BeautifulSoup
import sqlite3
import requests

# Set up SQLite database and server

# Create a connection to the SQLite database
conn = sqlite3.connect('boston_realty_data.db')

# Create a cursor object to interact with the database
cursor = conn.cursor()

# Create a table to store the scraped data
cursor.execute('''
               CREATE TABLE IF NOT EXISTS boston_realty_data (
                   id INTEGER PRIMARY KEY,
                   listing_name TEXT,
                   listing_desc TEXT,
                   n_beds INTEGER,
                   n_baths INTEGER,
                   location TEXT,
                   price INTEGER
                   )
               ''')
   
# Commit changes to the database
conn.commit()

# Write the web scraper
def web_crawler(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        #title = soup.title.string.strip() if soup.title else 'N/A'
        target_element = soup.find('div', class_="property-rows", id="content_link", recursive=True)
        if target_element:
            listing_desc = target_element
        else:
            print('Target html element not found')
        return listing_desc
    else:
        return None

# Store data in the SQLite database
def save_to_db(content):
    insert_query = 'INSERT INTO boston_realty_data (listing_desc) VALUES (?)'
    values = (content)
    conn.execute(insert_query, values)
    conn.commit()

# Main function to initiate scraping
def main():
    url = 'https://bostonpads.com/boston-apartments/'
    listing_desc = web_crawler(url)
    
    if listing_desc:
        save_to_db(listing_desc)
        print(f"Data for '{listing_desc}' saved to the database.")
    else:
        print("Failed to fetch data from the website.")   
    
if __name__ == "__main__":
    main()