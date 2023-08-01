# -*- coding: utf-8 -*-


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
                   last_updated TEXT,
                   price INTEGER,
                   available_date TEXT,
                   location TEXT,
                   utilities TEXT
                   )
               ''')
   
# Commit changes to the database
conn.commit()

# Write the web scraper
def web_crawler(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        listing_dump = soup.find_all('div', class_="col-sm-6 col-xs-12")
        if listing_dump:
            for listing_box in listing_dump:
                last_updated = listing_box.find('div', class_='lastUpdated')
                if last_updated:
                    last_updated = last_updated.string
                price = listing_box.find('div', class_='rentPrice')
                if price:
                    price = price.string
                avbl_date = listing_box.find('div', class_='listingAvailable')
                if avbl_date:
                    avbl_date = avbl_date.string
                location = listing_box.find('div', class_='listingLocation col-xs-12')
                location = location.find('a')
                if location:
                    location = location.string
                utilities = listing_box.find('span', class_='listingHHW')
                if utilities:
                    utilities = utilities.string
                save_to_db(last_updated, price, avbl_date, location, utilities)
        else:
            print('Target html element not found')
        return last_updated, price, avbl_date, location, utilities
    else:
        print(f'Website <{url}> unavailable!')
        return None

# Store data in the SQLite database
def save_to_db(last_updated, price, avbl_date, location, utilities):
    insert_query = 'INSERT INTO boston_realty_data (last_updated, price, available_date, location, utilities) VALUES (?, ?, ?, ?, ?)'
    values = (last_updated, price, avbl_date, location, utilities)
    conn.execute(insert_query, values)
    conn.commit()

# Main function to initiate scraping
def main():
    url = 'https://bostonpads.com/boston-apartments/'
    last_updated, price, avbl_date, location, utilities = web_crawler(url)
    #save_to_db(last_updated, price, avbl_date, location, utilities)
    print(f"Data for '{location}' saved to the database.")
    
if __name__ == "__main__":
    main()