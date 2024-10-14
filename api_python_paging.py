import time
import requests
import configuration as c
from datetime import datetime, timedelta
import pandas as pd

NEWS_FROM_DATE = datetime.now() - timedelta(days=100) 
BASE_URL = f"{c.stock_data_news}&apiKey={c.api_key}&published_utc.gt={NEWS_FROM_DATE.strftime('%Y-%m-%dT%H:%M:%SZ')}&ticker="
STOCKS_FOR_INVESTIGATION = c.stocks_for_investigation.split(',')
MAX_REQUESTS_PER_MINUTE = 5

result_data = []

params = {
    "adjusted": "true",
    "apiKey": c.api_key
}

######## I DECIDED TO TAKE A REAL API IN ORDER TO GET DATA AS PAGING SO THE EXEMPLE WILL FROM REAL THINGS ########

def fetch_data(url: str) -> str:   
    while url:
        request_count = 0

        #there is a linitation of 1000 
        if request_count >= MAX_REQUESTS_PER_MINUTE:
            print("rated limit wait 60 second...")
            time.sleep(62)
            request_count = 0

        response = requests.get(url)

        if response.status_code == 429:
            # If the server responds with "Too Many Requests", wait 60 seconds and retry the request
            print("Received 429 status code. Waiting 60 seconds before retrying...")
            time.sleep(60)
            continue 

        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code}")
            break

        data = response.json()

        if 'results' not in data:
            print(f"Error: 'results' key not found in response")
            break

        result_data.extend(data.get('results', []))

        next_url = data.get('next_url')
        if next_url and 'apiKey=' not in next_url:
            next_url = f"{next_url}&apiKey={c.api_key}"
        url = next_url
        request_count += 1

    return result_data 

def calculate_category_totals(collected_data : list) -> dict:

    df = pd.DataFrame(collected_data['items'])
    
    result = df.groupby('category').agg(
        item_count=('id', 'size'),             
        total_purchase_amount=('purchase_amount', 'sum') 
    ).reset_index()

    ##possible to make the same things using pyspark, it will not work because we need to create an environment for doing it, just for the idea.. :) 
        #  result = df.groupBy("category").agg(
        # count("id").alias("item_count"),          
        # sum("purchase_amount").alias("total_purchase_amount")

    return result.to_dict(orient='records')

if __name__ == '__main__':
      
    #### FOR FIRST FUNCTION EXEMPLE ####

      ##take into account that the first function collect a list that is not related to the exercise it is just using a real api to get the data using pages 
      for stock in STOCKS_FOR_INVESTIGATION:
        result_list = fetch_data(BASE_URL + stock)
  
    
    #### FOR SECOND FUNCTION EXEMPLE ####

        collected_data = {
        "items": [
            {"id": 1, "name": "item1", "purchase_amount": 20, "category": "electronics", "purchase_date": "2024-01-20"},
            {"id": 2, "name": "item2", "purchase_amount": 30, "category": "electronics", "purchase_date": "2024-01-20"},
            {"id": 3, "name": "item3", "purchase_amount": 15, "category": "books", "purchase_date": "2024-01-22"},
            {"id": 4, "name": "item4", "purchase_amount": 45, "category": "books", "purchase_date": "2024-01-23"},
            {"id": 5, "name": "item5", "purchase_amount": 25, "category": "clothing", "purchase_date": "2024-01-24"}
        ],
        "next_token": "abc123"
    }
        result = calculate_category_totals(collected_data)
        print(result)

        