# Databricks notebook source
from bs4 import BeautifulSoup
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

# Write the web scraper
def web_crawler(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        listing_dump = soup.find_all('div', class_="col-sm-6 col-xs-12")
        if listing_dump:
            data = []
            for listing_box in listing_dump:
                last_updated = listing_box.find('div', class_='lastUpdated')
                if last_updated:
                    last_updated = last_updated.string
                price = listing_box.find('div', class_='rentPrice')
                if price:
                    price = price.string
                n_beds = listing_box.find('div', class_='fa-fw fas fa-bed')
                if n_beds:
                    n_beds = n_beds.string
                n_baths = listing_box.find('div', class_='fa-fw fas fa-bath')
                if n_baths:
                    n_baths = n_baths.string
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
                amenities = listing_box.find('div', class_='listingAmenities')
                if amenities:
                    amenities = amenities.string
                data.append((last_updated, price, n_beds, n_baths, avbl_date, location, utilities, amenities))
            return pd.DataFrame(data, columns=["last_updated", "price", "n_beds", "n_baths", "avbl_date", "location", "utilities", "amenities"])
        else:
            print('Target html element not found')
    else:
        print(f'Website <{url}> unavailable!')
        return None

# Main function to initiate scraping
def main():
    url = 'https://bostonpads.com/boston-apartments/'
    pandas_df = web_crawler(url)

    if pandas_df is not None and not pandas_df.empty:
        # Convert the Pandas DataFrame to a Spark DataFrame
        spark = SparkSession.builder.appName("PandasToDelta").getOrCreate()
        spark_df = spark.createDataFrame(pandas_df)

        # Optionally, you can add an 'id' column with monotonically increasing unique values
        spark_df = spark_df.withColumn("id", monotonically_increasing_id())

        # Step 2: Create a Databricks Delta table from the Spark DataFrame
        schema_name = "default"
        table_name = "house_details"
        delta_path = f"/delta_tables/{schema_name}/{table_name}"

        spark_df.write \
          .format("delta") \
          .mode("overwrite") \
          .save(delta_path)

        # Step 3: Optionally, you can register the Delta table as a SQL table
        spark_df.createOrReplaceTempView(table_name)

        # Display the DataFrame (optional)
        spark_df.show()

        print("Data saved to the Databricks Delta table.")
    else:
        print("No data to save.")

if __name__ == "__main__":
    main()
