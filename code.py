# -*- coding: utf-8 -*-


# NOTE: Install beautifulsoup4 and requests packages beforehand

# Import necessary libraries
from bs4 import BeautifulSoup
import sqlite3
import requests
import re

# Set up SQLite database and server

# Create a connection to the SQLite database
conn = sqlite3.connect('housing.db')

# Create a cursor object to interact with the database
cursor = conn.cursor()

# Create a table to store the scraped data
cursor.execute('''
               CREATE TABLE IF NOT EXISTS bostonpads_data (
                   id INTEGER PRIMARY KEY,
                   price TEXT,
                   n_beds TEXT,
                   n_baths TEXT,
                   available_date TEXT,
                   location TEXT,
                   utilities TEXT,
                   amenities TEXT,
                   agent_name TEXT,
                   agent_email TEXT,
                   agent_phone TEXT
                   )
               ''')
   
# Commit changes to the database
conn.commit()

# Write the web scraper
def web_crawler(url):
    page_num = 282
    last_page_num = None
    
    while True:
        # Append the current page number to the URL
        current_url = f"{url}&page={page_num}"
        
        # Make an HTTP request to the URL
        response = requests.get(current_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the listings on the current page
        listings = soup.find('div', class_='search_results_area').find_all('div', class_='property_item')
        if not listings:
            print("No listings found on this page.")
            break
        
        # Process the listings on the current page
        for listing in listings:
            listing_details = listing.find('div', class_='item_props').find_all('div', class_='column')
            if listing_details:
                price = listing_details[0].text.strip()
                n_beds = listing_details[1].text.strip()
                n_baths = listing_details[2].text.strip()
                avbl_date = listing_details[3].text.strip()
            location = listing.find('a', class_='item_title')
            if location:
                location = location.text.strip()
            utilities = listing.find('a')
            if utilities:
                utilities = utilities['href']
            amenities = listing.find('a')
            if amenities:
                amenities = amenities['href']
            agent_details = soup.find('div', class_='contacts').find_all('li')
            if agent_details:
                agent_name = agent_details[0].text.strip()
                agent_email = agent_details[2].find('a', href=True)['href'].split(':')[1]
                agent_phone = agent_details[1].find('a', href=True)['href'].split(':')[1]
            save_to_db(price, n_beds, n_baths, avbl_date, location, utilities, amenities, agent_name, agent_email, agent_phone)
        
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

# Store data in the SQLite database
def save_to_db(price, n_beds, n_baths, avbl_date, location, utilities, amenities, agent_name, agent_email, agent_phone):
    insert_query = 'INSERT INTO bostonpads_data (price, n_beds, n_baths, available_date, location, utilities, amenities, agent_name, agent_email, agent_phone) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    values = (price, n_beds, n_baths, avbl_date, location, utilities, amenities, agent_name, agent_email, agent_phone)
    conn.execute(insert_query, values)
    conn.commit()

# Main function to initiate scraping
def main():
    url = "https://ygl.is/bryn-allen-1?areas%5B%5D=Boston&areas%5B%5D=Boston%3AAllston&areas%5B%5D=Boston%3ABack+Bay&areas%5B%5D=Boston%3ABay+Village&areas%5B%5D=Boston%3ABeacon+Hill&areas%5B%5D=Boston%3ABrighton&areas%5B%5D=Boston%3ACharlestown&areas%5B%5D=Boston%3AChinatown&areas%5B%5D=Boston%3ADorchester&areas%5B%5D=Boston%3AEast+Boston&areas%5B%5D=Boston%3AFenway&areas%5B%5D=Boston%3AFinancial+District&areas%5B%5D=Boston%3AFort+Hill&areas%5B%5D=Boston%3AHyde+Park&areas%5B%5D=Boston%3AJamaica+Plain&areas%5B%5D=Boston%3AKenmore&areas%5B%5D=Boston%3ALeather+District&areas%5B%5D=Boston%3AMattapan&areas%5B%5D=Boston%3AMidtown&areas%5B%5D=Boston%3AMission+Hill&areas%5B%5D=Boston%3ANorth+End&areas%5B%5D=Boston%3ARoslindale&areas%5B%5D=Boston%3ARoxbury&areas%5B%5D=Boston%3ASeaport+District&areas%5B%5D=Boston%3ASouth+Boston&areas%5B%5D=Boston%3ASouth+End&areas%5B%5D=Boston%3ATheatre+District&areas%5B%5D=Boston%3AWaterfront&areas%5B%5D=Boston%3AWest+End&areas%5B%5D=Boston%3AWest+Roxbury&page=282"
    web_crawler(url)
    
    print("Data saved to the database.")
    
if __name__ == "__main__":
    main()
    
    
    