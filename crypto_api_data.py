from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
import csv

## Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'cryptoapi_etl_main',
    default_args=default_args,
    description='Extract data for 5 Crytocurrencies from the CoinGecko API',
    schedule_interval='0 20 * * *',  # Run daily at 8pm UTC
)

def get_coininfo(params):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    response = requests.get(url,params= params,headers={'x-cg-demo-api-key':'CG-9HdERnohjuWAWnV5x8hKv1iQ'})
    return response


def extract_data():
    # List of Cryptocurrencies
    listofcoins = ['bitcoin','ethereum','solana','binancecoin','ripple']
    coins = []

    for coin in listofcoins:
        params = {'ids': coin,'vs_currency': 'USD'}
        apiResponse = get_coininfo(params)
        apiValue = apiResponse.json()
        coin = [apiValue[0]['name'], apiValue[0]['symbol'],apiValue[0]['current_price'],apiValue[0]['market_cap'],apiValue[0]['market_cap_rank'],apiValue[0]['total_volume'],apiValue[0]['price_change_percentage_24h']]
        coins.append(coin)
            
    crypto_df = pd.DataFrame(coins,columns = ['Name' , 'Symbol', 'CurrentPrice' , 'MarketCap','MarketCapRank','TotalVolume','PriceChangePercent'])
    print(crypto_df)
    crypto_df.to_csv('crypto_data.csv', index=False)  
    
# Function to read data from CSV files and print them
def transform_crypto_data():

    # Read Crypto data from CSV
    crypto_df = pd.read_csv('crypto_data.csv')
    print("\n PowerHouse Crypto Data:")
    print(crypto_df.head())
 
    # Convert to 2 Decimal places
    crypto_df['CurrentPrice'] = crypto_df['CurrentPrice'].apply(lambda x: round(x, 2))
    crypto_df['PriceChangePercent'] = crypto_df['PriceChangePercent'].apply(lambda x: round(x, 2))

    print("\nModified PowerHouse Crypto Data:")
    print(crypto_df.head())

    # Save modified data back to CSV
    crypto_df.to_csv('crypto_data.csv', index=False)

# Create tasks for each Coin
extract_crypto_task = PythonOperator(
    task_id='extract_cryptocurrency_data',
    python_callable=extract_data,
    dag=dag,
    )

# Create task to read and print data
transform_crypto_task = PythonOperator(
    task_id='transform_cryptocurrency_data',
    python_callable=transform_crypto_data,
    dag=dag,
    )

# Set task dependencies
extract_crypto_task >> transform_crypto_task